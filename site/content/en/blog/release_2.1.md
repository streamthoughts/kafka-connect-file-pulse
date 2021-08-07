---
title: "Connect FilePulse 2.1 is Available 🚀"
linkTitle: "Connect FilePulse 2.1 is Available"
date: 2021-08-04
description: ""
author: Florian Hussonnois ([@fhussonnois](https://twitter.com/fhussonnois))
---

**This new release contains a number of bug fixes and improvements that make ConnectFilePulse more stable and resilient in production.**


## Full Release Notes

Connect File Pulse 2.1 can be downloaded from the [GitHub Releases Page](https://github.com/streamthoughts/kafka-connect-file-pulse/releases/tag/v2.1.0). 

### Improvements & Bugfixes
[f1c071e](https://github.com/streamthoughts/kafka-connect-file-pulse/commit/f1c071e) refactor(plugin): change default value of `offset.attributes.string` to uri (#154)
[d8aac2b](https://github.com/streamthoughts/kafka-connect-file-pulse/commit/d8aac2b) refactor(plugin): enhance error handling when file do not exist anymore
[8a17051](https://github.com/streamthoughts/kafka-connect-file-pulse/commit/8a17051) fix(plugin): fix ClassCastException when offset.attributes.string=inode (#153)
[defee21](https://github.com/streamthoughts/kafka-connect-file-pulse/commit/defee21) fix(plugin): remove duplicate log when closing file iterator
[7897bcb](https://github.com/streamthoughts/kafka-connect-file-pulse/commit/7897bcb) fix(plugin): improve DefaultFileSystemMonitor to avoid scheduling files that may be cleanup by remaining tasks (#152)
[28456db](https://github.com/streamthoughts/kafka-connect-file-pulse/commit/28456db) fix(plugin): fix task must close resources on error while starting
[ddf8b86](https://github.com/streamthoughts/kafka-connect-file-pulse/commit/ddf8b86) fix(api): fix DeadLock on KafkaStateBackingStore
[370a1d6](https://github.com/streamthoughts/kafka-connect-file-pulse/commit/370a1d6) fix(filesystems): fix FileSystemMonitorThread should not fail if file metadata cannot be retrieved (#150)
[e9462c8](https://github.com/streamthoughts/kafka-connect-file-pulse/commit/e9462c8) fix(plugin): fix NPE using version 2.0 with KafkaFileObjectStateBackingStore (#149)

### Docs
[1b72a96](https://github.com/streamthoughts/kafka-connect-file-pulse/commit/1b72a96) docs(site): fix syntax for 'exists' ScEL Built-in Function (#148)
[7da7e41](https://github.com/streamthoughts/kafka-connect-file-pulse/commit/7da7e41) docs(site): fix documentation error for StateBackingStore (#147)

### Sub-Tasks
[ec70611](https://github.com/streamthoughts/kafka-connect-file-pulse/commit/ec70611) build(deps): bump commons-io from 2.5 to 2.7
[aa62022](https://github.com/streamthoughts/kafka-connect-file-pulse/commit/aa62022) fix(script): fix docker-compose for debugging
[2723516](https://github.com/streamthoughts/kafka-connect-file-pulse/commit/2723516) refactor(build): add mvn profiles for different storages
[c1da2f8](https://github.com/streamthoughts/kafka-connect-file-pulse/commit/c1da2f8) refactor(build): improve makefile and add utility script for debugging
[54ba1e5](https://github.com/streamthoughts/kafka-connect-file-pulse/commit/54ba1e5) build(maven): add meta info

If you enjoyed reading this post, check out Connect FilePulse at GitHub and give us a ⭐!
