# State engine v2 migration workspace

Status: temporary. This directory exists only while the state engine migration is in progress.

## Contents

- `EVIDENCE.md`: sanitized measurements and external contracts that constrain the design.
- `DESIGN.md`: the target architecture and state rules, independent of the current implementation.
- `PLAN.md`: the ordered migration and verification plan.
- `private/`: ignored local copies of raw logs, probe output, scripts, and original reports.

The private directory is deliberately excluded from Git. It may contain local network addresses,
device identifiers, topology names, timestamps, and protocol captures. Vendor documentation and
screenshots are not copied into this workspace.

## Source-of-truth rule

During the migration, this workspace is a planning and evidence aid. It is not a runtime contract.
The final source of truth is the code, tests, and permanent public documentation on `master`.

After the new runtime has completed its live acceptance period:

1. Move only durable, sanitized design facts into permanent documentation and tests.
2. Delete `.migration/state-engine-v2/`, including the ignored `private/` directory.
3. Delete the original temporary probe directory and any copied raw logs.
4. Remove migration-only `.gitignore` entries that are no longer needed.
5. Verify that no migration artifact is present in the release package or Git history.

## Publication policy

Any material that survives the migration must be neutral and reproducible. It must not include:

- private IP addresses, hostnames, usernames, room names, or device identifiers;
- raw topology snapshots or Home Assistant configuration;
- proprietary vendor documents, screenshots, or long protocol excerpts;
- claims about undocumented firmware internals that are not supported by measurements.

Public text distinguishes measured behavior, standards-based interpretation, and unknowns.
