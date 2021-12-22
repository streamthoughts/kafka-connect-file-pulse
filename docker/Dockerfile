
# Copyright 2019-2020 StreamThoughts.
#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements. See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License. You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# Kafka Connect File Pulse
FROM confluentinc/cp-kafka-connect-base:6.2.1

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
