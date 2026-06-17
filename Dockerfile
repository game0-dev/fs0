FROM debian:bookworm-slim AS downloader

ARG FS0_REPOSITORY=game0-dev/fs0
ARG FS0_TAG

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates curl \
    && rm -rf /var/lib/apt/lists/*

RUN test -n "$FS0_TAG" \
    && version="${FS0_TAG#v}" \
    && curl -fsSL "https://github.com/${FS0_REPOSITORY}/releases/download/${FS0_TAG}/fs0-${version}-x86_64-unknown-linux-gnu.tar.gz" \
    | tar -xz -C /usr/local/bin fs0 \
    && chmod +x /usr/local/bin/fs0

FROM debian:bookworm-slim

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*

COPY --from=downloader /usr/local/bin/fs0 /usr/local/bin/fs0

ENTRYPOINT ["fs0"]
