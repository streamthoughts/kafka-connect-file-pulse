#!/bin/bash
# release.sh
# This scripts can be used to execute all tasks required for releasing a new version of the project.

set -e

BASEDIR=$(dirname "$(readlink -f $0)")
DOC_BASEDIR="$BASEDIR/site/content/en/docs"
GIT_COMMITTER_NAME="Release"
GIT_COMMITTER_EMAIL="release@release"

./mvnw  -B -q versions:set -DremoveSnapshot -DgenerateBackupPoms=false >/dev/null 2>&1
RELEASE_VERSION=$(./mvnw org.apache.maven.plugins:maven-help-plugin:3.1.0:evaluate -Dexpression=project.version -q -DforceStdout)
RELEASE_DOC_VERSION=$(echo "$RELEASE_VERSION" | sed 's/\([0-9]\)\s*$/\x/')
RELEASE_DOC_DIR="$DOC_BASEDIR/Archives/v$RELEASE_DOC_VERSION"
RELEASE_DOC_LINK=$(echo "$RELEASE_VERSION" | sed -r 's/\.+/-/g')
RELEASE_DOC_LINK=${RELEASE_DOC_LINK%??}

git pull --rebase

echo "Packaging project: $RELEASE_VERSION"
./mvnw  -B -q clean package >/dev/null 2>&1

DIRS=(
"Developer Guide"
"Examples"
"FAQ"
"Getting started"
"Overview"
"Project Info"
)

echo "Creating release site documentation: v$RELEASE_DOC_VERSION"
mkdir -p "$RELEASE_DOC_DIR"
for DIR in "${DIRS[@]}"; do
  echo "Copying $DIR to $DOC_BASEDIR/$DIR";
  cp -r "$DOC_BASEDIR/$DIR" "$RELEASE_DOC_DIR";
done

echo "Creating $RELEASE_DOC_DIR/_index.md"
cat > "$RELEASE_DOC_DIR/_index.md" <<EOF
---
title: "Docs Release v$RELEASE_DOC_VERSION"
linkTitle: "v$RELEASE_DOC_VERSION"
url: "/v$RELEASE_DOC_LINK/docs"
---
This section is where the user documentation for Connect File Pulse lives - all the information that users need to understand and successfully use Connect File Pulse.
EOF

echo "Updating $BASEDIR/site/config.toml"
cat >> "$BASEDIR/site/config.toml" <<EOF
[[params.versions]]
  version = "v$RELEASE_DOC_VERSION"
  url = "/kafka-connect-file-pulse/v$RELEASE_DOC_LINK/docs"
EOF

git add "$BASEDIR/site/*" "pom.xml" "*/pom.xml" "*/*/pom.xml"
git commit -m "release version $RELEASE_VERSION" --author="$GIT_COMMITTER_NAME <$GIT_COMMITTER_EMAIL>"
git tag -a "v$RELEASE_VERSION" -m"release v$RELEASE_VERSION"

git branch "$RELEASE_DOC_VERSION"
git push
git push origin "v$RELEASE_VERSION"
git push origin "$RELEASE_DOC_VERSION"


exit 0
