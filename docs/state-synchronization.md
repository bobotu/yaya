# State synchronization

Yeelight Pro gateway writes and device state reports are asynchronous. A successful write response
means that the gateway accepted the request; it does not prove that every mesh device executed it.
Reports and readbacks do not contain command identifiers, timestamps, or revisions that establish
causality.

The integration therefore provides **bounded acknowledged-write projection**. Once the gateway
acknowledges a write, Home Assistant sees the accepted target immediately. Conflicting cached reports
cannot make that value bounce while the write is being reconciled. A matching observation confirms
the target; otherwise a bounded deadline releases the latest observed gateway state.

## Runtime boundaries

The session package has one state owner and one publication boundary:

| Component | Responsibility |
| --- | --- |
| `GatewayRPC` | TCP framing, request IDs, response futures, timeouts, and decoded pushes |
| `ConnectionActor` | Connection lifecycle and reconnect supervision |
| `GatewaySession` | Serialized writes, conservative batching, readback scheduling, and publication |
| `StateStore` | Raw observations, visible nodes, pending property ownership, and pure reduction rules |
| Coordinator and entities | Consume only the visible snapshot and publish it to Home Assistant |

Entities use the push-style `CoordinatorEntity` contract and do not poll the gateway directly. All
topology responses, property pushes, native-group responses, routine synchronization, and command
readbacks enter the same `StateStore` path. Raw gateway state is never published around that path.

## Write flow

1. Closely timed, non-conflicting property writes are collected into one gateway request.
2. Immediately before the request is sent, `StateStore` assigns its target properties to a pending
   batch. Until an acknowledgement arrives, the visible state remains unchanged.
3. A successful gateway acknowledgement marks the batch accepted and projects all still-current
   targets in one Home Assistant publication.
4. Gateway observations always update the raw snapshot. A target-matching observation confirms its
   property. A conflicting observation remains hidden while that property is pending.
5. Once every still-current target in an accepted batch has been observed, pending ownership ends.
   The visible state usually does not change because it already contains the acknowledged target.
6. If the RPC fails, its hold is removed, the latest raw state remains visible, and every original
   caller receives the failure.

A newer write replaces ownership of overlapping properties before it is sent. An old readback or
timer carries only its batch ID and therefore cannot release a newer write. Repeating the same
already-accepted target does not indefinitely extend reconciliation.

Brightness, color temperature, and color implicitly target `power=true`; `power=false` cannot be
combined with those properties. Brightness-only and color-temperature-only commands omit an explicit
power property so the gateway can apply them as one operation. Motor targets use the cover-specific
movement tracker instead of the generic light/property projection.

## Multi-light scheduling

The gateway protocol accepts multiple node writes in one request, but observed mesh dispatch may
still be sequential. For timed light payloads, the integration assigns descending per-node `delay`
values so later payloads can catch up with earlier ones. The compensation step is configurable and
defaults to 75 ms; setting it to 0 leaves the payload order and timing uncompensated. An explicit
protocol `delay` or `delayOff` is never overwritten.

## Bounded reconciliation

An unresolved accepted write gets one delayed cache readback. Ordinary nodes use the node read method
and native mesh groups use the group read method. A readback is an observation, not proof that a
physical transition completed, and is reduced exactly like a push.

The base defaults are a readback margin of 6 seconds and a hard reconciliation margin of 10 seconds.
Both are added after the largest `delay + duration` in the actual gateway batch. The hard deadline is
rebased when the ACK arrives, so RPC queueing and a requested transition cannot consume the
reconciliation window. At the deadline, pending ownership ends and the latest raw observation becomes
visible once. A mismatch or missing response does not make an entity unavailable.

Routine full synchronization is another source of observations and does not clear pending writes. A
real connection loss clears pending work, fails queued callers, and makes the coordinator unavailable.
Reconnect starts from a clean synchronization without replaying old command state.

## Home Assistant contract

The projected target means "the gateway accepted this requested state," not "the physical transition
has completed." Home Assistant lights have no standard opening/closing-style transition state, and
waiting several mesh-report cycles before changing `on`/`off` makes toggles and automations act on
stale state. Bounded projection gives those consumers read-your-writes behavior while preventing a
late cached report from producing `on -> off -> on -> off` bounce.

The default light transition is configurable and is currently 0.5 seconds. An explicit Home Assistant
`transition` overrides it. Transition duration and generated delay are sent to the gateway and extend
the reconciliation schedule; the integration does not synthesize intermediate brightness or color
values.

Availability reflects gateway connectivity and an observed node-online flag. Pending writes,
conflicting observations, readback failures, and reconciliation deadlines do not manufacture an
unavailable state.

Without causal metadata, a delayed old report can be observationally identical to a nearly
simultaneous external control. No client algorithm can resolve that ambiguity immediately. This
implementation deliberately favors temporary read-your-writes stability, then guarantees bounded
convergence to the latest gateway observation.

Exported diagnostics contain aggregate counts and timings: active batches, pending node/property
counts, queued requests, readbacks, release outcomes, and suppressed unchanged publications. They do
not expose command targets, node identifiers, names, or raw protocol payloads.
