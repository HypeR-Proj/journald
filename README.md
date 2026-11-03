<!--
SPDX-FileCopyrightText: 2026 roolrz
SPDX-License-Identifier: Apache-2.0
-->

# journald

Native log ingestion and bounded query service. The journal owns retained records
while admission workers deliver log streams and independent query sessions.

## Workspace tests

Use the [HypeR workspace](https://github.com/HypeR-Proj/HypeR) to resolve dependencies:

```sh
python3 project.py test journald
```

Paths in the documentation are relative to the repo-managed workspace root.
Edit this repository and prepare the workspace again before building.

## Listener failure policy

Log, query-session and outgoing-protocol admissions discard malformed requests
and matched transfer rejections. Endpoint closure ends the affected worker;
unexpected local receive failures are reported and stop that listener instead of
spinning. Existing log inputs and unrelated query sessions retain their owners.
