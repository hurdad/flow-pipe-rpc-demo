FROM natsio/nats-box:latest
WORKDIR /work
COPY init.sh /work/init.sh
RUN chmod +x /work/init.sh
ENTRYPOINT ["/work/init.sh"]
