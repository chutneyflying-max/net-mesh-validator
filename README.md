# Net Mesh Validator (`net-mesh-validator`)

[![Topology Audit](https://github.com/chutneyflying-max/net-mesh-validator/actions/workflows/mesh-audit.yml/badge.svg)](https://github.com/chutneyflying-max/net-mesh-validator/actions/workflows/mesh-audit.yml)
[![License: Apache-2.0](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)

Distributed peer-to-peer telemetry audit agent and cluster latency consensus runtime.

## Overview

`net-mesh-validator` periodically audits edge gateway nodes and multi-region cluster topologies. It runs scheduled consensus benchmarking passes to compute performance scores, jitter decay, and aggregate topology states into compressed telemetry feeds.

## Architecture

- **Ingress Telemetry Engine**: Validates responsive peer relays across configured cluster manifests.
- **Dynamic Velocity Matrix**: Evaluates throughput velocity ($VPH$) and calculates exponential half-life decay.
- **Brotli Telemetry Packaging**: Generates highly compressed channel states for low-bandwidth edge synchronization.

## Verification

```bash
# Execute local telemetry pass
npm install
npm run benchmark
```

## Consensus Schedules

Consensus verification runs autonomously on a bi-hourly heartbeat via GitHub Actions workflow pipelines. Snapshot digests are published directly to the designated artifact stream (`prod`).
