use crate::{Fs0Error, Fs0Result};
use fs0_core::HashId;
use std::{collections::HashMap, future::Future, marker::PhantomData, pin::Pin, sync::Arc};
use tokio::sync::{Mutex, Notify, Semaphore};

type BoxedHandler<C, P, Q> =
    dyn Fn(C, P) -> Pin<Box<dyn Future<Output = Fs0Result<Q>> + Send>> + Send + Sync;

pub(crate) struct HashRequestScheduler<C, P, Q> {
    permits: Arc<Semaphore>,
    in_flight: Arc<Mutex<HashMap<HashId, Arc<RequestSlot<Q>>>>>,
    handler: Arc<BoxedHandler<C, P, Q>>,
    _payload: PhantomData<fn(P)>,
}

impl<C, P, Q> std::fmt::Debug for HashRequestScheduler<C, P, Q> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("HashRequestScheduler")
            .field("available_permits", &self.permits.available_permits())
            .finish_non_exhaustive()
    }
}

impl<C, P, Q> Clone for HashRequestScheduler<C, P, Q> {
    fn clone(&self) -> Self {
        Self {
            permits: Arc::clone(&self.permits),
            in_flight: Arc::clone(&self.in_flight),
            handler: Arc::clone(&self.handler),
            _payload: PhantomData,
        }
    }
}

impl<C, P, Q> HashRequestScheduler<C, P, Q>
where
    C: Send + 'static,
    P: Send + 'static,
    Q: Send + Sync + 'static,
{
    pub(crate) fn new<H>(concurrency: usize, handler: H) -> Self
    where
        H: Fn(C, P) -> Pin<Box<dyn Future<Output = Fs0Result<Q>> + Send>> + Send + Sync + 'static,
    {
        Self {
            permits: Arc::new(Semaphore::new(concurrency.max(1))),
            in_flight: Arc::new(Mutex::new(HashMap::new())),
            handler: Arc::new(handler),
            _payload: PhantomData,
        }
    }

    pub(crate) async fn request(
        &self,
        hash_id: HashId,
        context: C,
        payload: P,
    ) -> Fs0Result<Arc<Q>> {
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

        if is_owner {
            let permits = Arc::clone(&self.permits);
            let in_flight = Arc::clone(&self.in_flight);
            let handler = Arc::clone(&self.handler);
            let owner_slot = Arc::clone(&slot);
            tokio::spawn(async move {
                let result = async {
                    let _permit =
                        permits
                            .acquire_owned()
                            .await
                            .map_err(|err| Fs0Error::Internal {
                                message: err.to_string(),
                            })?;
                    handler(context, payload).await.map(Arc::new)
                }
                .await;

                owner_slot.complete(result).await;
                in_flight.lock().await.remove(&hash_id);
            });
        }

        slot.wait().await
    }
}

struct RequestSlot<Q> {
    result: Mutex<Option<Fs0Result<Arc<Q>>>>,
    notify: Notify,
}

impl<Q> RequestSlot<Q> {
    fn new() -> Self {
        Self {
            result: Mutex::new(None),
            notify: Notify::new(),
        }
    }

    async fn complete(&self, result: Fs0Result<Arc<Q>>) {
        *self.result.lock().await = Some(result);
        self.notify.notify_waiters();
    }

    async fn wait(&self) -> Fs0Result<Arc<Q>> {
        loop {
            let notified = self.notify.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            {
                let result = self.result.lock().await.clone();
                if let Some(result) = result {
                    return result;
                }
            }
            notified.as_mut().await;
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
    use tokio::{runtime::Builder, task::JoinSet};

    #[test]
    fn duplicate_requests_share_one_execution() {
        run(async {
            let calls = Arc::new(AtomicUsize::new(0));
            let scheduler = HashRequestScheduler::new(8, {
                let calls = Arc::clone(&calls);
                move |(), _payload: ()| {
                    let calls = Arc::clone(&calls);
                    Box::pin(async move {
                        calls.fetch_add(1, Ordering::SeqCst);
                        Ok::<_, Fs0Error>(7u32)
                    })
                }
            });
            let hash_id = hash_id(1);
            let mut tasks = JoinSet::new();

            for _ in 0..16 {
                let scheduler = scheduler.clone();
                tasks.spawn(async move { scheduler.request(hash_id, (), ()).await });
            }

            while let Some(result) = tasks.join_next().await {
                assert_eq!(*result.unwrap().unwrap(), 7);
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
            let scheduler = HashRequestScheduler::new(2, {
                let active = Arc::clone(&active);
                let max_active = Arc::clone(&max_active);
                let notify = Arc::clone(&notify);
                move |(), _payload: ()| {
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
            });
            let mut tasks = JoinSet::new();

            for index in 0..6 {
                let scheduler = scheduler.clone();
                tasks.spawn(async move { scheduler.request(hash_id(index), (), ()).await });
            }

            while active.load(Ordering::SeqCst) < 2 {
                tokio::task::yield_now().await;
            }
            notify.notify_waiters();
            while let Some(result) = tasks.join_next().await {
                result.unwrap().unwrap();
                notify.notify_waiters();
            }

            assert_eq!(max_active.load(Ordering::SeqCst), 2);
        });
    }

    #[test]
    fn failed_request_can_be_retried() {
        run(async {
            let calls = Arc::new(AtomicUsize::new(0));
            let scheduler = HashRequestScheduler::new(1, {
                let calls = Arc::clone(&calls);
                move |(), _payload: ()| {
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
            });
            let hash_id = hash_id(9);

            assert!(scheduler.request(hash_id, (), ()).await.is_err());
            assert_eq!(*scheduler.request(hash_id, (), ()).await.unwrap(), 9);
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
