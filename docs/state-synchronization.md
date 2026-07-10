# State synchronization

Yeelight Pro gateway writes and device state reports are asynchronous. A successful write response
means that the gateway accepted the request; it does not prove that every mesh device executed it.
State reports and readbacks also do not contain a command identifier that can establish causality.

This integration therefore uses **non-optimistic observed-state latching**. Home Assistant sees only
values that have appeared in a gateway observation. Command targets are never written directly into
the Home Assistant state machine.

## Runtime boundaries

The session package has one state owner and one publication boundary:

| Component | Responsibility |
| --- | --- |
| `GatewayRPC` | TCP framing, request IDs, response futures, timeouts, and decoded pushes |
| `ConnectionActor` | Connection lifecycle and reconnect supervision |
| `GatewaySession` | Serialized writes, conservative batching, readback scheduling, and publication |
| `StateStore` | Raw observations, visible nodes, pending property ownership, and pure reduction rules |
| Coordinator and entities | Consume only the visible snapshot and publish it to Home Assistant |

Entities do not poll the gateway directly and cannot read the raw snapshot. All topology responses,
property pushes, native-group responses, and command readbacks enter the same `StateStore` path.

## Write flow

1. Closely timed, non-conflicting property writes are collected into one gateway request.
2. Immediately before sending the request, `StateStore` assigns the written properties to a new
   pending batch. The batch's integer ID is also the stale-callback fence.
3. While a property is pending, raw gateway observations continue to update, but the visible value
   remains at its last observed and published value.
4. A gateway acknowledgement marks the batch accepted. It does not publish the target.
5. A push, topology snapshot, or readback that reports a target marks that property observed.
6. Once every still-current target in an accepted gateway batch has been observed, the complete
   batch is released in one visible publication. This prevents member-by-member Home Assistant group
   changes when gateway reports arrive at different times.
7. If an RPC fails, its hold is removed and every original caller receives the failure.

A newer write replaces ownership of overlapping properties before it is sent. Consequently, an old
readback or timer carrying an older batch ID cannot release a newer write. This is sufficient for
rapid sequences such as `A -> B -> A`; there is no separate command-generation state machine.

Property writes derive their observed targets from the protocol payload. Brightness, color
temperature, and color imply `power=true`; `power=false` cannot be combined with those properties.
Motor target properties are handled by the cover-specific movement tracker instead of the generic
pending-property reducer.

## Bounded reconciliation

An accepted write gets one delayed cache readback. Ordinary nodes use the node read method and native
mesh groups use the group read method. A readback is an observation, not execution proof, and is
reduced exactly like a push.

The current defaults are a single readback after 6 seconds and a hard deadline after 10 seconds. At
the deadline, pending ownership is removed and the latest raw observation becomes visible once. A
mismatch or missing device response does not make an entity unavailable.

Routine full synchronization is also just another source of observations. It must not clear pending
writes or cancel their timers. A real connection loss does clear pending work, fail queued callers,
and make the coordinator unavailable. Reconnect starts from a clean synchronization without replaying
old command state.

## Transition semantics

The integration has a configurable default light transition, currently 0.5 seconds. An explicit Home
Assistant `transition` value overrides that default. Transition duration changes only the command sent
to the gateway; it is not used to decide whether an observation is valid or whether a pending batch
may be released.

The gateway interface does not expose physical transition progress separately from its cached state.
The integration therefore does not synthesize intermediate light values or claim to know when a
physical fade has completed.

## Availability and unavoidable ambiguity

Availability reflects gateway connectivity and an observed node-online flag. Pending commands,
conflicting observations, readback failures, and reconciliation deadlines do not manufacture an
unavailable state.

Without command identifiers, a delayed old report can be observationally identical to a nearly
simultaneous external control. The integration cannot resolve that ambiguity immediately. It favors
temporary Home Assistant state stability while a write is pending, then guarantees convergence to the
latest observed gateway state at the bounded deadline.

Exported diagnostics contain only aggregate counts and timings: active batches, pending node/property
counts, queued requests, readbacks, release outcomes, and suppressed unchanged publications. They do
not expose command targets, node identifiers, names, or raw protocol payloads.
