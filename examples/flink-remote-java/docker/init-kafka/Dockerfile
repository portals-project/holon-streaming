# docker/init-kafka/Dockerfile
FROM confluentinc/cp-kafka:6.1.1

USER root

# Copy the init script into /usr/local/bin (or /opt)
COPY examples/flink-remote-java/docker/init-kafka/init-kafka.sh /init-kafka.sh
RUN chmod +x /init-kafka.sh

# Preserve whatever environment variables you need.
ENTRYPOINT ["/bin/sh", "-c", "sh /init-kafka.sh"]