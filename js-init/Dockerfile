FROM ubuntu:24.04

RUN apt-get update && apt-get install -y --no-install-recommends curl ca-certificates && rm -rf /var/lib/apt/lists/*

COPY js-init/init.sh /init.sh
RUN chmod +x /init.sh

ENTRYPOINT ["/init.sh"]
