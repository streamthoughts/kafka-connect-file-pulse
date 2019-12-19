FROM confluentinc/cp-kafka-connect-base:5.2.1

COPY connect-file-pulse-plugin/target/components/packages/streamthoughts-kafka-connect-file-pulse-1.3.0-SNAPSHOT.zip /tmp/kafka-connect-file-pulse.zip
RUN confluent-hub install --no-prompt /tmp/kafka-connect-file-pulse.zip && \
    rm -rf /tmp/kafka-connect-file-pulse.zip

COPY include/docker/connect-log4j.properties.template /etc/confluent/docker/log4j.properties.template
