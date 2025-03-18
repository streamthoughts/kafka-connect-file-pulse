#
# SPDX-License-Identifier: Apache-2.0
# Copyright (c) StreamThoughts
#
# Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
#
# Kafka Connect File Pulse
FROM confluentinc/cp-kafka-connect-base:7.9.0

USER root

LABEL org.opencontainers.image.title=streamthoughts-kafka-connect-file-pulse \
      org.opencontainers.image.authors=oss@streamthoughts.io \
      org.opencontainers.image.url=https://streamthoughts.github.io/kafka-connect-file-pulse \
      org.opencontainers.image.documentation=https://streamthoughts.github.io/kafka-connect-file-pulse \
      org.opencontainers.image.source=https://github.com/streamthoughts/kafka-connect-file-pulse \
      org.opencontainers.image.vendor=StreamThoughts \
      org.opencontainers.image.licenses=Apache-2.0

ARG CREATED
ARG VERSION
ARG COMMIT
ARG BRANCH

ENV FILE_PULSE_VERSION="${VERSION}" \
    FILE_PULSE_COMMIT="${COMMIT}" \
    FILE_PULSE_BRANCH="${BRANCH}"

COPY ./include/docker/connect-log4j.properties.template /etc/confluent/docker/log4j.properties.template
COPY ./streamthoughts-kafka-connect-file-pulse-$FILE_PULSE_VERSION.zip /tmp/streamthoughts-kafka-connect-file-pulse.zip

RUN confluent-hub install --no-prompt /tmp/streamthoughts-kafka-connect-file-pulse.zip && \
    rm -rf /tmp/streamthoughts-kafka-connect-file-pulse.zip

LABEL org.opencontainers.image.created=$CREATED \
      org.opencontainers.image.version=$VERSION \
      org.opencontainers.image.revision=$COMMIT
