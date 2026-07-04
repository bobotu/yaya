from __future__ import annotations

import json
import sys
import tomllib
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]


class PackagingLayoutTests(unittest.TestCase):
    def test_custom_component_shape_and_manifest_required_fields(self) -> None:
        custom_components = ROOT / "custom_components"
        integration_dirs = [
            item.name
            for item in custom_components.iterdir()
            if item.is_dir() and not item.name.startswith("__") and not item.name.endswith(".egg-info")
        ]
        self.assertEqual(integration_dirs, ["yeelight_pro"])

        manifest = json.loads((custom_components / "yeelight_pro" / "manifest.json").read_text(encoding="utf-8"))
        for key in ("domain", "documentation", "issue_tracker", "codeowners", "name", "version"):
            self.assertIn(key, manifest)

        self.assertEqual(manifest["domain"], "yeelight_pro")
        self.assertEqual(manifest["codeowners"], ["@bobotu"])
        self.assertEqual(manifest["requirements"], [])
        self.assertFalse((custom_components / "yeelight_pro" / "yeelight_pro_cli").exists())

    def test_translation_resources_cover_english_and_simplified_chinese(self) -> None:
        integration = ROOT / "custom_components" / "yeelight_pro"
        strings = json.loads((integration / "strings.json").read_text(encoding="utf-8"))
        en = json.loads((integration / "translations" / "en.json").read_text(encoding="utf-8"))
        zh_hans = json.loads((integration / "translations" / "zh-Hans.json").read_text(encoding="utf-8"))

        for translations in (strings, en, zh_hans):
            self.assertEqual(translations["entity"]["switch"]["relay"]["name"].count("{channel}"), 1)
            self.assertIn("air_conditioner", translations["entity"]["climate"])
            self.assertIn("ventilation", translations["entity"]["fan"])
            self.assertIn("delay_off", translations["entity"]["number"])
            self.assertIn("bath_mode", translations["entity"]["select"])
            self.assertIn("motion", translations["entity"]["binary_sensor"])
            self.assertIn("occupancy", translations["entity"]["binary_sensor"])
            self.assertIn("luminance", translations["entity"]["sensor"])
            self.assertEqual(translations["entity"]["event"]["key_events"]["name"].count("{index}"), 1)
            self.assertEqual(translations["entity"]["event"]["control_events"]["name"].count("{index}"), 1)
            self.assertIn("gateway", translations["device"])
            self.assertNotIn("device_model", translations)
            self.assertIn("panel_click", translations["device_automation"]["trigger_type"])

        self.assertEqual(en["entity"]["switch"]["relay"]["name"], "Relay {channel}")
        self.assertEqual(zh_hans["entity"]["switch"]["relay"]["name"], "继电器 {channel}")

    def test_pyproject_packages_layered_library_for_cli_without_ha_dependency(self) -> None:
        pyproject = tomllib.loads((ROOT / "pyproject.toml").read_text(encoding="utf-8"))

        package_find = pyproject["tool"]["setuptools"]["packages"]["find"]
        self.assertEqual(package_find["where"], [".", "custom_components"])
        self.assertEqual(package_find["include"], ["dev_tools*", "yeelight_pro*"])
        self.assertEqual(pyproject["project"]["scripts"]["yeelight-pro"], "dev_tools.yeelight_pro_cli:main")
        self.assertEqual(pyproject["project"]["dependencies"], [])

    def test_root_src_package_has_been_removed(self) -> None:
        src = ROOT / "src"
        if src.exists():
            self.assertEqual(list(src.rglob("*.py")), [])

    def test_layered_packages_import_from_packaging_root_without_ha_dependency(self) -> None:
        sys.path.insert(0, str(ROOT))
        sys.path.insert(0, str(ROOT / "custom_components"))
        try:
            import dev_tools.yeelight_pro_cli as cli  # noqa: PLC0415
            import yeelight_pro.core as core  # noqa: PLC0415
            import yeelight_pro.session as session  # noqa: PLC0415

            self.assertTrue(hasattr(core, "iter_gateway_events"))
            self.assertTrue(hasattr(session, "YeelightProGateway"))
            self.assertTrue(hasattr(cli, "async_main"))
        finally:
            sys.path.pop(0)
            sys.path.pop(0)


if __name__ == "__main__":
    unittest.main()
