# Home Assistant Runtime Tests

These pytest tests exercise the custom integration through Home Assistant's own
config entry, state machine, service registry, device registry, and device
automation APIs.

They are intentionally separate from `tests/unit/` because they require a Home
Assistant-compatible Python runtime and the
`pytest-homeassistant-custom-component` test package. The normal offline library
tests remain:

```powershell
uv run python -m unittest discover -s tests/unit -v
```

Run the HA harness from a Linux Home Assistant dev/custom-component test
environment, such as the HA devcontainer, WSL, or a CI Linux runner. Native
Windows Python cannot currently run this harness because the Home Assistant test
plugin imports Unix-only modules such as `fcntl`.

```powershell
python -m pip install -r tests/ha/requirements.txt
python -m pytest tests/ha -vv
```

The tests patch the Yeelight gateway client with a fake local gateway. They do
not touch the LAN and do not send `gateway_set.*` to a real gateway.
