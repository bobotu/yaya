# yaya

Yet another Yeelight Pro Home Assistant integration.

`yaya` is an unofficial, local-first Home Assistant custom integration for
Yeelight Pro gateways. It was built for my own home setup and tested only
against the devices I have locally. If it works for your setup too, feel free to
use it, fork it, or copy ideas from it.

This is not an official Yeelight, Xiaomi, HACS, or Home Assistant project.

## Project status

This repository is mostly a personal-use project. I am not planning to actively
maintain it, triage compatibility issues, or support devices that I do not own.
I will likely improve it only when I run into problems in my own home.

Forks and independent versions are welcome. If you want stronger guarantees,
broader device coverage, a cleaner public API, or a different maintenance model,
you should probably fork this project or use it as a reference for a new
implementation.

## How this was built

This implementation was vibe-coded with AI assistance. The protocol work is
based on local gateway probing, a downloaded copy of an older shared Yeelight
Pro LAN protocol PDF (`Yeelight Pro局域网协议`, version history through 2.1),
and comparison against existing community implementations.

The PDF was found through [hasscc/yeelight-pro issue #6](https://github.com/hasscc/yeelight-pro/issues/6).
Community projects used as protocol references include
[leeteke/YeelightPro](https://github.com/leeteke/YeelightPro) and
[hasscc/yeelight-pro](https://github.com/hasscc/yeelight-pro). Their source code
is not vendored into this repository.

## What it does

The repository contains a standalone async Python protocol/session layer and a
Home Assistant custom integration under `custom_components/yeelight_pro`.

- UDP gateway discovery for `YEELIGHT_GATEWAY_CONTROL_DISCOVER` on port `1982`.
- TCP RPC client for `65443/tcp`.
- UTF-8 JSON messages separated by `\r\n`.
- Request id matching for `gateway_get.*` and explicit `gateway_set.prop`.
- Push dispatch for `gateway_post.topology`, `gateway_post.prop`, and
  `gateway_post.event`.
- Device models for lights, relay switches, curtains, panels/knobs, sensors,
  air conditioners, and bath heaters.
- Home Assistant platforms for light, cover, switch, climate, fan, number,
  select, sensor, binary sensor, event entities, and device triggers.
- Offline tests using local fixtures and fake TCP gateways.

The client is read-only until caller code explicitly invokes write methods such
as `set_prop()` or a typed device control method.

## Home Assistant installation

The integration domain is `yeelight_pro`, and the installable custom component
directory is:

```text
custom_components/yeelight_pro/
```

For a manual local install, copy that directory into the Home Assistant config
directory:

```text
config/custom_components/yeelight_pro/
```

Restart Home Assistant, then add **Yeelight Pro** from **Settings > Devices &
services**.

I have only tested this integration on Home Assistant 2026.7. Based on the Home
Assistant APIs it uses, it should theoretically run on Home Assistant 2024.6.0
or newer, but I do not guarantee compatibility with older releases.

[![Open your Home Assistant instance and add this repository to HACS.](https://my.home-assistant.io/badges/hacs_repository.svg)](https://my.home-assistant.io/redirect/hacs_repository/?owner=bobotu&repository=yaya&category=integration)

If the button does not work, add `https://github.com/bobotu/yaya` in HACS as a
custom **Integration** repository, install it, restart Home Assistant, and add
**Yeelight Pro** from the integration UI.

## Supported mapping

- `light`: imports known light nodes, including light groups (`nt=4`).
- `cover`: maps curtains to open, close, stop, and target position. Dream
  curtain tilt is enabled when the gateway reports `type=22` or explicit
  curtain subtype `pt=22`.
- `switch`: exposes relay channels for multi-switch and double-switch devices
  configured in relay mode.
- `event`: exposes wireless-mode multi-key switches, scene panels, and knobs as
  stateless event entities.
- `device_trigger`: exposes `panel_click`, `panel_hold`, `panel_release`, and
  `knob_spin` triggers by button key or knob index.
- `sensor` / `binary_sensor`: exposes battery, charging, motion, occupancy,
  door, alarm, temperature, humidity, luminance, and related states when the
  gateway reports the underlying properties.
- `climate`, `fan`, `number`, and `select`: expose the subset of air conditioner
  controller and bath heater features that were observed in my local setup.

Entities are created from topology-backed nodes only. Property pushes for
unknown node ids are retained in diagnostics for debugging but are not imported
into Home Assistant until a later topology sync identifies them. If a node
disappears from topology, its existing entities remain registered but become
unavailable.

## Modeling notes

- `pt` is not used as a general fallback for `type`. It is used only for
  explicit subtypes observed on otherwise known devices, such as `type=128,
  pt=137` for a knob-capable control panel and `type=6, pt=22` for dream-curtain
  tilt.
- Multi-key and double relay switches default to relay mode. The config/options
  flow lets users mark individual devices as wireless switch mode.
- Relay mode exposes controllable relay switch entities and filters button
  events. Wireless mode exposes button events and device triggers, hides
  controllable relay switch entities, and adds diagnostic binary sensors for
  read-only relay state.
- Knob-capable panels may report `knob.spin` with an index such as `idx=3` even
  when topology does not expose `ch_num` or `cids`. In that case the HA device
  trigger list intentionally exposes key/idx `1..6` so real events are not
  hidden from the automation UI.

## CLI

Use `uv run` during development. It creates or uses the project environment and
exposes the `yeelight-pro` console script from `pyproject.toml`:

The CLI lives under `dev_tools/`. It is a local development/debugging tool and
is not part of the HACS-installed custom component.

```powershell
uv sync
uv run yeelight-pro discover
uv run yeelight-pro list --host <gateway-host>
uv run yeelight-pro list --host <gateway-host> --include-groups
uv run yeelight-pro describe --host <gateway-host> --id <node-id>
uv run yeelight-pro listen --host <gateway-host> --id <node-id>
uv run yeelight-pro watch --host <gateway-host>
```

`list` shows raw devices only by default, so group nodes such as light groups
are hidden. Use `--include-groups` when you need to inspect group nodes too.

Only the `command` subcommand sends write requests:

```powershell
uv run yeelight-pro command --host <gateway-host> --id <node-id> set-position --position 50
uv run yeelight-pro command --host <gateway-host> --id <node-id> stop
uv run yeelight-pro command --host <gateway-host> --id <node-id> set-prop --prop tp=50
```

Panel and knob events are delivered by the gateway as `gateway_post.event` push
messages on the same TCP connection. The `listen` command keeps that connection
open and prints events such as `panel.click`, `panel.hold`, `panel.release`, and
`knob.spin`.

Use `watch` when debugging the whole gateway stream. It runs the initial
read-only sync, then prints both `gateway_post.prop` before/after state changes
and `gateway_post.event` events for every device.

## Development checks

Offline unit tests do not touch the LAN:

```powershell
uv run python -m unittest discover -s tests/unit -v
```

Format and lint with Ruff:

```powershell
uv run ruff format dev_tools custom_components tests
uv run ruff check dev_tools custom_components tests
```

The optional Home Assistant runtime harness lives under `tests/ha/`. It should
be run from a Linux Home Assistant-compatible environment, such as CI, WSL, or a
devcontainer:

```powershell
python -m pip install -r tests/ha/requirements.txt
python -m pytest tests/ha -vv
```

The HA tests patch the Yeelight gateway client with a fake local gateway. They
do not touch the LAN and do not send `gateway_set.*` to a real gateway.

## License

MIT. See [LICENSE](LICENSE).
