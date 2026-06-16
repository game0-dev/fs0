use crate::{Fs0Error, Fs0Result};
use fs0_core::HashId;
use std::{collections::HashMap, future::Future, pin::Pin, sync::Arc};
use tokio::sync::{Mutex, Semaphore};

type Handler<Req, Res> =
    dyn Fn(Req) -> Pin<Box<dyn Future<Output = Fs0Result<Res>> + Send>> + Send + Sync;
type Callback<Res> = Box<dyn FnOnce(Fs0Result<Res>) + Send>;

pub(crate) struct HashRequestScheduler<Req, Res> {
    permits: Arc<Semaphore>,
    in_flight: Arc<Mutex<HashMap<HashId, Arc<RequestSlot<Res>>>>>,
    handler: Arc<Handler<Req, Res>>,
}

impl<Req, Res> HashRequestScheduler<Req, Res>
where
    Req: Send + 'static,
    Res: Clone + Send + Sync + 'static,
{
    pub(crate) fn new<H>(concurrency: usize, handler: H) -> Self
    where
        H: Fn(Req) -> Pin<Box<dyn Future<Output = Fs0Result<Res>> + Send>> + Send + Sync + 'static,
    {
        Self {
            permits: Arc::new(Semaphore::new(concurrency.max(1))),
            in_flight: Arc::new(Mutex::new(HashMap::new())),
            handler: Arc::new(handler),
        }
    }

    pub(crate) async fn enqueue_blocking<G>(
        &self,
        hash_id: HashId,
        request: Req,
        on_complete: G,
    ) -> Fs0Result<()>
    where
        G: FnOnce(Fs0Result<Res>) + Send + 'static,
    {
        let (slot, is_owner) = {
            let mut in_flight = self.in_flight.lock().await;
            if let Some(slot) = in_flight.get(&hash_id) {
                (Arc::clone(slot), false)
            } else {
                let slot = Arc::new(RequestSlot::new());
                in_flight.insert(hash_id, Arc::clone(&slot));
                (slot, true)
            }
        };
        slot.add_callback(Box::new(on_complete)).await;

        if !is_owner {
            return Ok(());
        }

        let permit = match self.permits.clone().acquire_owned().await {
            Ok(permit) => permit,
            Err(err) => {
                let err = Fs0Error::Internal {
                    message: err.to_string(),
                };
                slot.complete(Err(err.clone())).await;
                self.in_flight.lock().await.remove(&hash_id);
                return Err(err);
            }
        };
        let in_flight = Arc::clone(&self.in_flight);
        let handler = Arc::clone(&self.handler);
        let owner_slot = Arc::clone(&slot);
        tokio::spawn(async move {
            let _permit = permit;
            let result = handler(request).await;
            owner_slot.complete(result).await;
            in_flight.lock().await.remove(&hash_id);
        });

        Ok(())
    }
}

struct RequestSlot<Q> {
    state: Mutex<RequestSlotState<Q>>,
}

struct RequestSlotState<Q> {
    result: Option<Fs0Result<Q>>,
    callbacks: Vec<Callback<Q>>,
}

impl<Q> RequestSlot<Q> {
    fn new() -> Self {
        Self {
            state: Mutex::new(RequestSlotState {
                result: None,
                callbacks: Vec::new(),
            }),
        }
    }

    async fn complete(&self, result: Fs0Result<Q>)
    where
        Q: Clone,
    {
        let callbacks = {
            let mut state = self.state.lock().await;
            state.result = Some(result.clone());
            std::mem::take(&mut state.callbacks)
        };
        for callback in callbacks {
            callback(result.clone());
        }
    }

    async fn add_callback(&self, callback: Callback<Q>)
    where
        Q: Clone,
    {
        let mut callback = Some(callback);
        let result = {
            let mut state = self.state.lock().await;
            if let Some(result) = state.result.clone() {
                Some(result)
            } else {
                state
                    .callbacks
                    .push(callback.take().expect("callback is present"));
                None
            }
        };
        if let Some(result) = result {
            callback.expect("callback is present")(result);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };
    use tokio::{runtime::Builder, sync::Notify, task::JoinSet};

    #[test]
    fn duplicate_requests_share_one_execution() {
        run(async {
            let calls = Arc::new(AtomicUsize::new(0));
            let scheduler = Arc::new(HashRequestScheduler::new(8, {
                let calls = Arc::clone(&calls);
                move |_payload: ()| {
                    let calls = Arc::clone(&calls);
                    Box::pin(async move {
                        calls.fetch_add(1, Ordering::SeqCst);
                        Ok::<_, Fs0Error>(7u32)
                    })
                }
            }));
            let hash_id = hash_id(1);
            let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
            let mut tasks = JoinSet::new();

            for _ in 0..16 {
                let scheduler = scheduler.clone();
                let tx = tx.clone();
                tasks.spawn(async move {
                    scheduler
                        .enqueue_blocking(hash_id, (), move |result| {
                            let _ = tx.send(result.unwrap());
                        })
                        .await
                        .unwrap()
                });
            }

            while let Some(result) = tasks.join_next().await {
                result.unwrap();
            }
            for _ in 0..16 {
                assert_eq!(rx.recv().await.unwrap(), 7);
            }
            assert_eq!(calls.load(Ordering::SeqCst), 1);
        });
    }

    #[test]
    fn concurrency_limits_distinct_requests() {
        run(async {
            let active = Arc::new(AtomicUsize::new(0));
            let max_active = Arc::new(AtomicUsize::new(0));
            let notify = Arc::new(Notify::new());
            let scheduler = Arc::new(HashRequestScheduler::new(2, {
                let active = Arc::clone(&active);
                let max_active = Arc::clone(&max_active);
                let notify = Arc::clone(&notify);
                move |_payload: ()| {
                    let active = Arc::clone(&active);
                    let max_active = Arc::clone(&max_active);
                    let notify = Arc::clone(&notify);
                    Box::pin(async move {
                        let current = active.fetch_add(1, Ordering::SeqCst) + 1;
                        max_active.fetch_max(current, Ordering::SeqCst);
                        notify.notified().await;
                        active.fetch_sub(1, Ordering::SeqCst);
                        Ok::<_, Fs0Error>(())
                    })
                }
            }));
            let mut tasks = JoinSet::new();
            let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();

            for index in 0..6 {
                let scheduler = scheduler.clone();
                let tx = tx.clone();
                tasks.spawn(async move {
                    scheduler
                        .enqueue_blocking(hash_id(index), (), move |result| {
                            let _ = tx.send(result.unwrap());
                        })
                        .await
                        .unwrap()
                });
            }

            while active.load(Ordering::SeqCst) < 2 {
                tokio::task::yield_now().await;
            }
            notify.notify_waiters();
            while let Some(result) = tasks.join_next().await {
                result.unwrap();
                notify.notify_waiters();
            }
            for _ in 0..6 {
                rx.recv().await.unwrap();
            }

            assert_eq!(max_active.load(Ordering::SeqCst), 2);
        });
    }

    #[test]
    fn failed_request_can_be_retried() {
        run(async {
            let calls = Arc::new(AtomicUsize::new(0));
            let scheduler = Arc::new(HashRequestScheduler::new(1, {
                let calls = Arc::clone(&calls);
                move |_payload: ()| {
                    let calls = Arc::clone(&calls);
                    Box::pin(async move {
                        if calls.fetch_add(1, Ordering::SeqCst) == 0 {
                            Err(Fs0Error::Internal {
                                message: "first attempt failed".to_owned(),
                            })
                        } else {
                            Ok(9u32)
                        }
                    })
                }
            }));
            let hash_id = hash_id(9);
            let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();

            scheduler
                .enqueue_blocking(hash_id, (), {
                    let tx = tx.clone();
                    move |result| {
                        let _ = tx.send(result);
                    }
                })
                .await
                .unwrap();
            assert!(rx.recv().await.unwrap().is_err());
            let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
            scheduler
                .enqueue_blocking(hash_id, (), move |result| {
                    let _ = tx.send(result.unwrap());
                })
                .await
                .unwrap();
            assert_eq!(rx.recv().await.unwrap(), 9);
            assert_eq!(calls.load(Ordering::SeqCst), 2);
        });
    }

    #[test]
    fn enqueue_blocking_waits_for_permit_before_returning() {
        run(async {
            let notify = Arc::new(Notify::new());
            let scheduler = Arc::new(HashRequestScheduler::new(1, {
                let notify = Arc::clone(&notify);
                move |_payload: ()| {
                    let notify = Arc::clone(&notify);
                    Box::pin(async move {
                        notify.notified().await;
                        Ok::<_, Fs0Error>(())
                    })
                }
            }));

            let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
            scheduler
                .enqueue_blocking(hash_id(1), (), {
                    let tx = tx.clone();
                    move |result| {
                        let _ = tx.send(result);
                    }
                })
                .await
                .unwrap();
            let scheduler_for_second = scheduler.clone();
            let tx_for_second = tx.clone();
            let second = tokio::spawn(async move {
                scheduler_for_second
                    .enqueue_blocking(hash_id(2), (), move |result| {
                        let _ = tx_for_second.send(result);
                    })
                    .await
                    .unwrap()
            });

            tokio::task::yield_now().await;
            assert!(!second.is_finished());

            notify.notify_waiters();
            rx.recv().await.unwrap().unwrap();
            second.await.unwrap();
            notify.notify_waiters();
            rx.recv().await.unwrap().unwrap();
        });
    }

    #[test]
    fn enqueue_blocking_duplicate_callbacks_share_result() {
        run(async {
            let notify = Arc::new(Notify::new());
            let calls = Arc::new(AtomicUsize::new(0));
            let active = Arc::new(AtomicUsize::new(0));
            let scheduler = Arc::new(HashRequestScheduler::new(1, {
                let notify = Arc::clone(&notify);
                let calls = Arc::clone(&calls);
                let active = Arc::clone(&active);
                move |_payload: ()| {
                    let notify = Arc::clone(&notify);
                    let calls = Arc::clone(&calls);
                    let active = Arc::clone(&active);
                    Box::pin(async move {
                        calls.fetch_add(1, Ordering::SeqCst);
                        active.fetch_add(1, Ordering::SeqCst);
                        notify.notified().await;
                        Ok::<_, Fs0Error>(11u32)
                    })
                }
            }));
            let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
            let hash_id = hash_id(3);

            scheduler
                .enqueue_blocking(hash_id, (), {
                    let tx = tx.clone();
                    move |result| {
                        let _ = tx.send(result.unwrap());
                    }
                })
                .await
                .unwrap();
            while active.load(Ordering::SeqCst) == 0 {
                tokio::task::yield_now().await;
            }
            scheduler
                .enqueue_blocking(hash_id, (), {
                    let tx = tx.clone();
                    move |result| {
                        let _ = tx.send(result.unwrap());
                    }
                })
                .await
                .unwrap();

            notify.notify_waiters();
            assert_eq!(rx.recv().await.unwrap(), 11);
            assert_eq!(rx.recv().await.unwrap(), 11);
            assert_eq!(calls.load(Ordering::SeqCst), 1);
        });
    }

    #[test]
    fn enqueue_blocking_handler_failure_can_be_retried() {
        run(async {
            let calls = Arc::new(AtomicUsize::new(0));
            let scheduler = Arc::new(HashRequestScheduler::new(1, {
                let calls = Arc::clone(&calls);
                move |_payload: ()| {
                    let calls = Arc::clone(&calls);
                    Box::pin(async move {
                        if calls.fetch_add(1, Ordering::SeqCst) == 0 {
                            Err(Fs0Error::InvalidData {
                                message: "handler failed".to_owned(),
                            })
                        } else {
                            Ok::<_, Fs0Error>(13u32)
                        }
                    })
                }
            }));
            let hash_id = hash_id(4);
            let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
            scheduler
                .enqueue_blocking(hash_id, (), {
                    let tx = tx.clone();
                    move |result| {
                        let _ = tx.send(result);
                    }
                })
                .await
                .unwrap();

            assert!(rx.recv().await.unwrap().is_err());
            assert_eq!(calls.load(Ordering::SeqCst), 1);

            let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
            scheduler
                .enqueue_blocking(hash_id, (), move |result| {
                    let _ = tx.send(result.unwrap());
                })
                .await
                .unwrap();
            assert_eq!(rx.recv().await.unwrap(), 13);
            assert_eq!(calls.load(Ordering::SeqCst), 2);
        });
    }

    fn run(future: impl Future<Output = ()>) {
        Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap()
            .block_on(future);
    }

    fn hash_id(value: u8) -> HashId {
        HashId::new([value; 32])
    }
}
