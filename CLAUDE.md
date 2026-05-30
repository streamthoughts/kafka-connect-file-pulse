# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project

Connect FilePulse is a Kafka Connect **source connector** that streams files (CSV, JSON, AVRO, XML, Parquet, Grok-parsed, …) from multiple filesystems (local, Amazon S3, Azure Storage, GCS, SFTP, Aliyun OSS) into Kafka. The codebase is a Maven multi-module reactor (`kafka-connect-filepulse-reactor`); the deliverable is a Confluent-style connector ZIP plus a Docker image.

## Build & test commands

The Maven wrapper (`./mvnw`) is the source of truth. Java 11 is the compiler target; CI builds on JDK 17 (zulu).

```bash
./mvnw -ntp -B verify                       # full build incl. unit + integration tests, checkstyle, spotbugs, jacoco (= what CI runs)
./mvnw -ntp -B verify -DskipTests           # build without tests
./mvnw -pl connect-file-pulse-filters test  # unit tests for a single module
./mvnw -pl connect-file-pulse-filters test -Dtest=CSVFilterTest               # single test class
./mvnw -pl connect-file-pulse-filters test -Dtest=CSVFilterTest#someMethod    # single test method
./mvnw -pl connect-file-pulse-plugin verify                                   # run integration tests (Failsafe, JUnit5 tag `integration`)
```

Important build behavior:

- **Spotless runs in the `compile` phase with the `apply` goal** — it auto-formats Java and **injects the Apache 2.0 license header from `./header`** into every source file. Don't add the header by hand; just compile.
- Checkstyle (`checkstyle/checkstyle.xml`) runs at `validate` — build fails on violations.
- SpotBugs + findsecbugs runs at `verify` with `effort=Max`, `threshold=High`. Filters live in `.spotbugs/`.
- Surefire excludes the `integration` JUnit5 tag in the plugin module; Failsafe runs only that tag.
- JaCoCo aggregates unit + integration coverage into `target/jacoco.exec` at the reactor root.

### Packaging & Docker

Distribution profiles select which filesystem implementations are bundled into the connector ZIP:

| Profile | Includes |
|---------|----------|
| `all` (default in `dist`) | every FS |
| `aws`, `gcs`, `azure`, `local` | only that FS |
| `dist` | activates the `kafka-connect-maven-plugin` to produce the Confluent component ZIP at `connect-file-pulse-plugin/target/components/packages/` |

```bash
./mvnw clean package -DskipTests -P dist,all    # build the bundled ZIP
make docker-build                                # build ZIP + Docker image (uses MVN_PROFILE env var)
make build-images                                # build images for all PROFILES (gcs/aws/azure/local)
./debug.sh -b                                    # build + run local docker-compose stack with Kafka Connect on :8083
```

## Architecture

The connector is built around a pipeline: a **file-system monitor** discovers files → the connector assigns URIs to tasks via a **partitioner** → each task reads records with a **FileInputReader** → records flow through a **RecordFilterPipeline** → emitted as Kafka `SourceRecord`s → completed files are handled by a **cleanup policy**. File-processing state (offsets, status) is persisted in a **StateBackingStore** so work survives task restarts and rebalances.

### Module responsibilities

- **`connect-file-pulse-api`** — Core SPIs. Everything in other modules implements interfaces from here:
  - `filter/`: `RecordFilter`, `RecordFilterPipeline`, conditional execution
  - `reader/`: `FileInputReader`, `FileInputIterator`, `RecordsIterable`
  - `fs/`: `FileSystemListing`, `FileSystemMonitor`, `Storage`, `TaskFileURIProvider`
  - `source/`: `FileObject`, `FileObjectMeta`, `FileObjectOffset`, `TaskPartitioner`, `SourceOffsetPolicy`
  - `clean/`: `FileCleanupPolicy` (per-file) and `BatchFileCleanupPolicy`
  - `storage/`: generic `KafkaBasedLog`-backed `StateBackingStore` used to persist file processing state on a compacted Kafka topic
  - `data/`, `schema/`: internal `TypedStruct` / `TypedValue` model used by filters (independent of Connect's `Struct`/`Schema` until emission)
- **`connect-file-pulse-filters`** — Built-in `RecordFilter` implementations (CSV, JSON, Grok, XML→JSON/Struct, Date, Append, Rename, Move, Drop, Explode, Join, MultiRow, GroupRow, …). New filters belong here and must be registered to be available by `type`.
- **`connect-file-pulse-expression`** — **SCEL** (Simple Connect Expression Language). Used in filter configs (`if:`, field accessors) for expressions like `{{ $value.field }}`. Includes ANTLR4-generated parser under `gen/`.
- **`connect-file-pulse-dataformat`** — Shared data-format helpers used by readers/filters.
- **`connect-file-pulse-filesystems`** — One submodule per backend: `filepulse-commons-fs` (shared), `filepulse-local-fs`, `filepulse-amazons3-fs`, `filepulse-azure-storage-fs`, `filepulse-google-cloud-storage-fs`, `filepulse-sftp-fs`, `filepulse-aliyunoss-fs`. Each provides a `FileSystemListing`, a `FileInputReader`, a `Storage`, and cleanup policies for that backend.
- **`connect-file-pulse-plugin`** — The connector entry points:
  - `source/FilePulseSourceConnector`, `source/FilePulseSourceTask` — the Kafka Connect classes
  - `source/FileSystemMonitorThread`, `DefaultFileRecordsPollingConsumer` — orchestrate scan + read
  - `source/HashByURITaskPartitioner`, `DefaultTaskPartitioner` — assign files to tasks
  - `state/` — `KafkaFileObjectStateBackingStore` (production) and `InMemoryFileObjectStateBackingStore`, with `StateBackingStoreAccess` ref-counting a shared store across the connector + its tasks
  - `fs/`, `offset/`, `config/` — plumbing for the above

### Dependency rules

`api` depends on no other internal module. `filters`, `expression`, `dataformat`, and each `filesystems/*` depend only on `api`. `plugin` depends on everything and pulls FS modules in via the distribution profiles. Don't introduce cycles or push concrete FS code into `api`.

## Conventions

- **Commit messages follow Conventional Commits** with a fixed scope vocabulary defined in `CONTRIBUTING.md`: `plugin`, `filters`, `cleanup-api`, `scel-api`, `api`, `ci`, `github-pages`, `checkstyle`, `examples`, `readme`, `changelogs`, `contributing`. Example: `fix(filters): handle empty CSV header row`. Subject in imperative present, lowercase, no trailing dot.
- Java source/test files **must end up with the license header from `./header`** — Spotless inserts it on compile.
- Tests use JUnit 5 (Jupiter). Vintage engine is on the classpath only to keep legacy JUnit 4 tests running; prefer Jupiter for new tests. Integration tests must carry the JUnit 5 `@Tag("integration")` so Failsafe picks them up and Surefire skips them.
