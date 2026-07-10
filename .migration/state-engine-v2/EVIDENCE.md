# Sanitized evidence baseline

This document records only the facts needed to design and verify the replacement state engine.
Detailed captures and original reports remain in the ignored `private/` directory and will be
deleted after migration.

## Evidence levels

- **Measured**: repeated in controlled gateway probes or visible in integration logs.
- **Contract**: documented or directly verified Home Assistant behavior.
- **Inferred**: a plausible explanation consistent with measurements, but not a firmware claim.

## Measured gateway behavior

### RPC and cache

- A write RPC is normally acknowledged in tens of milliseconds. The acknowledgement means that
  the gateway accepted the request; it does not mean a mesh device executed it.
- Node and native-group read RPCs normally return in a few milliseconds. They read the gateway's
  current cache and are not synchronous device reads.
- Push identifiers are global and monotonic across tested clients, but are not command identifiers
  and do not provide causality.
- Multiple concurrent long-lived TCP clients were stable in the tested environment. A probe
  multiplexer is not required by the current evidence.

### Node classes

- Ordinary nodes and native mesh groups use different read methods.
- Reading a native group through the ordinary-node method can return no data.
- Native groups converge and publish materially later than ordinary nodes.

### Transition and light properties

- During a long explicit transition, the gateway cache and push stream can expose the final target
  well before the physical transition duration ends.
- The API does not expose present value, target value, and remaining transition time as separate
  fields. Physical transition completion is therefore not observable through this RPC interface.
- Writing brightness or color temperature while a light is off implicitly turns it on.
- Combining `power=false` with brightness or color temperature in one node write finishes on in
  the tested devices. Off must be sent without those properties.
- For separate conflicting writes, later accepted work eventually wins, but an older result can
  still become observable after the newer write has already been acknowledged.

### Batch and failure behavior

- A multi-node write receives one aggregate acknowledgement.
- A mixed online/offline batch can be acknowledged as successful while only online nodes execute.
- There is no per-node execution result. Each node must be reconciled through later gateway state.
- A gateway batch improves physical simultaneity but does not make state publication atomic.

### Full synchronization

- Requesting topology caused a full property snapshot shortly afterward in the tested session.
- A passive client did not observe a timely periodic full snapshot during the bounded observation
  period. Periodic full sync cannot be used for command reconciliation.

## Sanitized latency summary

The following aggregates came from repeated four-node batches. Values identify node classes only;
local identifiers and wall-clock timestamps are intentionally omitted.

| Metric | Ordinary node | Native group |
|---|---:|---:|
| Brightness target push P95 | about 3.4 s | about 8.1 s |
| Brightness target readback P95 | about 3.1 s | about 5.6 s |
| Power target push P95 | about 3.1 s | about 7.6 s |
| Power target readback P95 | about 2.9 s | about 5.2 s |

Additional observations:

- The largest measured four-member push skew was about seven seconds.
- Polling observed cache convergence but did not accelerate later pushes.
- Native-group samples produced conflicting old-value pushes; ordinary-node samples did not in the
  same clean test series.
- A native-group target push was occasionally absent even though a later readback held the target.
- In clean samples, readback did not revert after first reaching the requested target.

## Reproduced ambiguity

A two-command native-group test produced this sanitized sequence:

1. An on command was acknowledged.
2. Gateway readback later became on.
3. An off command was then acknowledged.
4. Readback remained on for several seconds.
5. An on push arrived after the newer off acknowledgement.
6. Readback and push eventually became off.

At the moment of the conflicting push, the gateway cache also reported on. This was not TCP packet
reordering. It was older mesh work becoming visible while newer work was still pending.

The sequence is observationally indistinguishable from a genuine external on command. Without a
command token in push messages, software cannot resolve that ambiguity immediately.

## Home Assistant contract

- Entity state is the canonical value consumed by the frontend, groups, automations, templates,
  voice assistants, and history. Home Assistant does not provide a separate general target-state
  channel for integrations.
- `CoordinatorEntity` is push-managed and does not require Core polling, but every coordinator
  update can still cause a complete entity state write. Coordinator data must therefore contain
  only HA-visible state, never raw gateway state.
- An entity state write serializes the complete state and attributes. Holding one property only at
  the notification layer is insufficient; entity properties themselves must read visible state.
- Frontend controls have different local optimistic behavior. The integration cannot depend on a
  particular card's rollback timer or local slider value.
- A service method can return after gateway acknowledgement. It must report RPC failure, but it
  need not wait for device-state convergence.
- `available` describes whether the integration can communicate with the entity. It must not be
  used to represent a pending command or a state mismatch.

Useful public references:

- [Home Assistant entity contract](https://developers.home-assistant.io/docs/core/entity/)
- [Home Assistant data fetching](https://developers.home-assistant.io/docs/integration_fetching_data/)
- [Home Assistant light entities](https://developers.home-assistant.io/docs/core/entity/light/)
- [Home Assistant group behavior](https://www.home-assistant.io/integrations/group/)
- [Bluetooth Mesh model specification](https://www.bluetooth.com/wp-content/uploads/Files/Specification/HTML/MMDL_v1.1/out/en/index-en.html)
- [Bluetooth Mesh protocol specification](https://www.bluetooth.com/wp-content/uploads/Files/Specification/HTML/MshPRT_v1.1/out/en/index-en.html)

## Standards-based interpretation

Bluetooth Mesh commonly uses asynchronous advertising, retransmission, relaying, multicast, and
application-level publication. Those mechanisms explain jitter and probabilistic delivery, but do
not by themselves prove the measured multi-second delay. The evidence is more consistent with a
gateway-side command queue, a local cache, later device status publication, and TCP-side
coalescing. The exact vendor firmware and mesh stack remain unknown.

## Design constraints derived from the evidence

1. ACK is acceptance, not state confirmation.
2. Push and readback are gateway observations; neither carries command causality.
3. Raw observations must never write HA state directly.
4. A newer write must prevent older callbacks and observations from releasing its held fields.
5. A batch needs one visible release boundary to avoid member-by-member group skew.
6. Reconciliation must be bounded. At the deadline, current observed raw state wins.
7. Timeout or mismatch must not manufacture `unavailable`.
8. Transition duration controls the command, not whether an observation is valid.
9. Native groups require their group read method.
10. The unavoidable external-control ambiguity should be resolved in favor of temporary UI
    stability, then bounded convergence to observed state.
