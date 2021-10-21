
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

ARG connectFilePulseVersion
ARG connectFilePulseBranch
ARG connectFilePulseCommit

ENV FILE_PULSE_VERSION="${connectFilePulseVersion}" \
    FILE_PULSE_COMMIT="${connectFilePulseCommit}" \
    FILE_PULSE_BRANCH="${connectFilePulseBranch}"

COPY connect-file-pulse-plugin/target/components/packages/streamthoughts-kafka-connect-file-pulse-${FILE_PULSE_VERSION}.zip /tmp/kafka-connect-file-pulse.zip
RUN confluent-hub install --no-prompt /tmp/kafka-connect-file-pulse.zip && \
    rm -rf /tmp/kafka-connect-file-pulse.zip

COPY include/docker/connect-log4j.properties.template /etc/confluent/docker/log4j.properties.template

LABEL io.streamthoughts.docker.name="kafka-connect-file-pulse" \
      io.streamthoughts.docker.version=$FILE_PULSE_VERSION \
      io.streamthoughts.docker.branch=$FILE_PULSE_BRANCH \
      io.streamthoughts.docker.commit=$FILE_PULSE_COMMIT
