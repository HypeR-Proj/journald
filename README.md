<!--
SPDX-FileCopyrightText: 2026 roolrz
SPDX-License-Identifier: Apache-2.0
-->

# journald

Native log ingestion and bounded query service. The journal owns retained records
while admission workers deliver log streams and query endpoints.

## Workspace tests

Use the [HypeR workspace](https://github.com/HypeR-Proj/HypeR) to resolve dependencies:

```sh
python3 project.py test journald
```

Paths in the documentation are relative to the repo-managed workspace root.
Edit this repository and prepare the workspace again before building.

## Listener failure policy

Log, query-request and outgoing-protocol admissions discard malformed requests
and matched transfer rejections. Endpoint closure ends the affected worker;
unexpected local receive failures are reported and stop that listener instead of
spinning. Existing log inputs and delegated query endpoints retain their owners.

## Query resources

Eight fixed workers serve the shared query endpoint. Each request supplies its
own reply channel; retention is unlocked before the bounded reply operation.
An idle client retains only its delegated capability, not a journal thread or
session slot. Long-lived shell delegation remains valid after idle periods.

A routed `query` connection transfers the request endpoint using
`hyper_journal::open_query`. At most eight incomplete endpoint handoffs are
retained, each with an absolute five-second deadline. Excess connections close;
expired or closed peers release their temporary handles and workers. These
bounds contain aggregate resource usage, not per-client fairness.

## Registration validation

Source registration uses `hyper_journal::registration` for the receive-buffer
limit and decoding. Names contain 1–255 UTF-8 bytes with no Unicode control
characters; the complete frame is limited to 512 bytes, including the prefix
and JSON escaping. Invalid frames are rejected before publishing the source,
with received capability owners released. Structured `LOG2` and raw `LOGS`
registrations share the name contract.
