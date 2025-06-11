FROM flink:1.18

# Copy connectors
COPY ../flink-connectors/*.jar /opt/flink/lib/
COPY ../flink-plugins/flink-gs-fs-hadoop-1.18.1.jar /opt/flink/plugins/gs/

# Copy queries or job scripts (optional)
COPY ../queries /opt/flink/queries/

