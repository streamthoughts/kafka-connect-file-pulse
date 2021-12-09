---
title: "Connect FilePulse 2.3 is Released 🚀"
linkTitle: "Connect FilePulse 2.3 is Released"
date: 2021-09-05
description: ""
author: Florian Hussonnois ([@fhussonnois](https://twitter.com/fhussonnois))
---

**This new release brings new capabilities and several bug fixes and improvements that make ConnectFilePulse still the more powerful Kafka Connect-based solution for processing files.**

## Full Release Notes

Connect File Pulse 2.3 can be downloaded from the [GitHub Releases Page](https://github.com/streamthoughts/kafka-connect-file-pulse/releases/tag/v2.3.0). 

### New Features
[3da6a5b](https://github.com/streamthoughts/kafka-connect-file-pulse/commit/3da6a5b) feat(filesystems): add the capability to configure alternative AWS S3 endpoint (#172)
[608d1c2](https://github.com/streamthoughts/kafka-connect-file-pulse/commit/608d1c2) feat(plugin): add new prop to cleanup on offset commit
[f6b443a](https://github.com/streamthoughts/kafka-connect-file-pulse/commit/f6b443a) feat(api): allow to configure a record-value schema
[befbc6f](https://github.com/streamthoughts/kafka-connect-file-pulse/commit/befbc6f) feat(filters): add new NullValueFilter (#169)
[d86804b](https://github.com/streamthoughts/kafka-connect-file-pulse/commit/d86804b) feat(plugin): add new config tasks.empty.poll.wait.ms
[39c37d9](https://github.com/streamthoughts/kafka-connect-file-pulse/commit/39c37d9) feat(plugin): add new prop to configure if task should halt on error (#164)
[7c219b8](https://github.com/streamthoughts/kafka-connect-file-pulse/commit/7c219b8) feat(filesystems/api): enhance XMLFileInputReader to support data type inference (#163)

### Improvements & Bugfixes
[5314e20](https://github.com/streamthoughts/kafka-connect-file-pulse/commit/5314e20) fix(filters): DelimtedRowFileInputFilter should compute schema for each record (#171)
[816f48a](https://github.com/streamthoughts/kafka-connect-file-pulse/commit/816f48a) fix(filters): fix AppendFilter to set record-value to null (#167)
[d2e776b](https://github.com/streamthoughts/kafka-connect-file-pulse/commit/d2e776b) fix(api): fix connector should accept nullable record-cord (#170)
[2266fc1](https://github.com/streamthoughts/kafka-connect-file-pulse/commit/2266fc1) fix(expression): fix SCEL expression null

### Docs
[20983dd](https://github.com/streamthoughts/kafka-connect-file-pulse/commit/20983dd) docs(site): fix documentation typos on metadata access (#165)
[4eef25f](https://github.com/streamthoughts/kafka-connect-file-pulse/commit/4eef25f) docs(site): add missing config prop

If you enjoyed reading this post, check out Connect FilePulse at GitHub and give us a ⭐!
