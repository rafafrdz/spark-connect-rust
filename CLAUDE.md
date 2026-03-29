# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Apache Spark Connect Rust client (`spark-connect-rs`) - a Rust client library for Apache Spark Connect that provides a DataFrame API over gRPC. Experimental/pre-release (v0.0.2).

## Build Commands

**Prerequisites:** Rust 1.81+, `protoc` (protobuf compiler), `cmake`.

```bash
# Build
cargo build -p spark-connect-rs

# Build with optional features
cargo build -p spark-connect-rs --features polars,datafusion

# Lint
cargo clippy
cargo fmt -- --check

# Generate docs
cargo doc

# Run all integration tests (requires running Spark Connect server)
docker compose up -d
cargo test -p spark-connect-rs --features polars,datafusion

# Run a single test
cargo test -p spark-connect-rs --features polars,datafusion <test_name>

# Run examples (requires running Spark Connect server)
cargo run -p examples --bin <example_name>
```

## Architecture

### Workspace Structure

- `crates/connect/` — Main library crate (`spark-connect-rs`)
- `examples/` — Example binaries (sql, reader, writer, readstream, deltalake, databricks)
- `datasets/` — Sample data for examples/tests

### Core Data Flow

User code builds a **DataFrame** (lazy) → each transformation appends to a **LogicalPlanBuilder** (unresolved `spark::Relation` protobuf) → on an action (`show()`, `collect()`, `count()`, `write()`) the plan is submitted via **SparkConnectClient** over gRPC → results come back as Arrow RecordBatches.

### Key Modules (`crates/connect/src/`)

| Module | Purpose |
|---|---|
| `session.rs` | `SparkSession` + `SparkSessionBuilder` — entry point, holds gRPC client |
| `dataframe.rs` | `DataFrame` — lazy transformations, actions trigger execution |
| `plan.rs` | `LogicalPlanBuilder` — builds unresolved `spark::Relation` trees |
| `column.rs` | `Column` — wraps `spark::Expression`, operator overloading |
| `expressions.rs` | Traits: `ToFilterExpr`, `ToLiteral` — type conversions to protobuf |
| `functions/` | 200+ Spark SQL functions generated via `gen_func!` macro |
| `readwriter.rs` | `DataFrameReader`/`DataFrameWriter` + file format options macros |
| `streaming/` | `DataStreamReader`/`DataStreamWriter`, `StreamingQuery` |
| `catalog.rs` | Catalog API (list/create/drop tables, databases, etc.) |
| `client/` | gRPC client: `SparkConnectClient`, `ChannelBuilder`, auth middleware |
| `types.rs` | `SparkDataType` trait, `StructType`, `DataType` enum |
| `errors.rs` | `SparkError` — maps gRPC status codes to typed error variants |

### Protobuf / gRPC

Proto files live in `crates/connect/protobuf/spark-3.5/spark/connect/`. Code generation happens in `crates/connect/build.rs` via `tonic_build`. Generated code is accessed through `spark::` module via `tonic::include_proto!("spark.connect")`.

### Feature Flags

- **default** — tokio + tonic transport
- **tls** — TLS support (required for Databricks)
- **datafusion** — convert results to DataFusion DataFrames
- **polars** — convert results to Polars DataFrames

### Testing

Tests are inline (`#[cfg(test)]`) within most modules. Integration tests require a live Spark Connect server — start one with `docker compose up -d` (Spark 3.5.3 with Delta Lake support, port 15002).

## Git Workflow & Branching Strategy

### Remotes

- `upstream` → `apache/spark-connect-rust` (canonical repo)
- `origin` → `rafafrdz/spark-connect-rust` (fork)

### Branches

- **`upstream/master`** — Canonical upstream. All PRs target this branch.
- **`origin/dev`** — Integration branch on the fork. Contains all accepted work merged together. Used as a "productive" branch with all features combined. Periodically rebased/recreated from upstream/master + feature branches.
- **`origin/master`** — Fork's master, kept in sync with `origin/dev`.

### Creating feature branches for PRs

**All PR branches MUST be created from `upstream/master`**, not from `origin/master` or `origin/dev`. This ensures each PR shows only its own commit(s) and no unrelated history.

```bash
# Correct: branch from upstream
git checkout -b my-feature upstream/master

# Wrong: branch from local master (includes merged work from other PRs)
git checkout -b my-feature master
```

### After a PR is created

1. Push the feature branch to `origin`: `git push origin my-feature`
2. Create PR targeting `upstream/master`
3. Merge into `origin/dev` for the integrated productive branch: `git checkout dev && git merge my-feature --no-edit && git push origin dev`
