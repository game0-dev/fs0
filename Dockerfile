FROM debian:bookworm-slim AS downloader

ARG FS0_REPOSITORY=game0-dev/fs0
ARG FS0_TAG

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates curl \
    && rm -rf /var/lib/apt/lists/*

RUN test -n "$FS0_TAG" \
    && curl -fsSL "https://github.com/${FS0_REPOSITORY}/releases/download/${FS0_TAG}/fs0-linux-x86_64.tar.gz" \
    | tar -xz -C /usr/local/bin fs0 \
    && chmod +x /usr/local/bin/fs0

FROM debian:bookworm-slim

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*

COPY --from=downloader /usr/local/bin/fs0 /usr/local/bin/fs0

ENTRYPOINT ["fs0"]
