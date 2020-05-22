#!/bin/bash
#
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

last_commit="$(git diff --name-status HEAD~1 site | cat | grep 'site/')"

HUGO_BUILD_DIR=$(pwd)/docs/
HUGO_SOURCE_DIR=$(pwd)/site/
HUGO_VERSION=0.60.1
HUGO_DIST_DIR=/tmp/hugo/v${HUGO_VERSION}
HUGO_RELEASE=https://github.com/gohugoio/hugo/releases/download/v${HUGO_VERSION}/hugo_extended_${HUGO_VERSION}_Linux-64bit.tar.gz

# backup Git user info
GIT_USER_NAME=$(git config user.name)
GIT_USER_EMAIL=$(git config user.email)


build_and_deploy(){
  echo "Buildind Hugo site"
  rm -rf docs/*
  rm $HUGO_DIST_DIR && mkdir -p $HUGO_DIST_DIR
  git config --global user.email circleci@circleci
  git config --global user.name CircleCI
  # update hugo theme submodule
  git submodule sync && git submodule update --init --recursive
  # install hugo and build
  wget $HUGO_RELEASE -P $HUGO_DIST_DIR && \
  (cd $HUGO_DIST_DIR; tar -xzvf $HUGO_DIST_DIR/hugo_extended_${HUGO_VERSION}_Linux-64bit.tar.gz) && \
  (cd $HUGO_SOURCE_DIR; sudo npm install) && \
  HUGO_ENV=production $HUGO_DIST_DIR/hugo -v -s $HUGO_SOURCE_DIR -d $HUGO_BUILD_DIR
  if [[ $? -eq 0 ]]; then
    echo "Deploying site updates"
    git add docs
    git commit -m "docs(gh-pages): build and deploy site [skip ci]"
    git push
  else
    echo "Hugo build failed"
  fi
  # reset git config
  git config --global user.email $GIT_USER_EMAIL
  git config --global user.name $GIT_USER_NAME
  exit $?
}

if [ $# -eq 1 ]; then
case $1 in
    --force)
      build_and_deploy
    ;;
    *)
       echo "Unknown arg $1"
    ;;
esac
fi

if [[ ${last_commit} ]]; then
  build_and_deploy
else
  echo "Skipping site building, lastest commit message doesn't change path directory 'site/'"
fi

exit 0
