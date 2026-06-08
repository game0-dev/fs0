#!/usr/bin/env python3

import os
import re
import sys
from pathlib import Path

try:
    import tomllib
except ModuleNotFoundError:
    tomllib = None

root = Path(__file__).resolve().parents[2]
cargo_toml = root / "Cargo.toml"

if tomllib is None:
    in_workspace_package = False
    version = None
    for line in cargo_toml.read_text().splitlines():
        if line.strip() == "[workspace.package]":
            in_workspace_package = True
            continue
        if in_workspace_package and line.startswith("["):
            break
        if in_workspace_package:
            match = re.match(r'version\s*=\s*"([^"]+)"', line.strip())
            if match:
                version = match.group(1)
                break
    if version is None:
        print("failed to read [workspace.package].version from Cargo.toml")
        sys.exit(1)
else:
    with cargo_toml.open("rb") as f:
        data = tomllib.load(f)
    version = data["workspace"]["package"]["version"]

expected_tag = f"v{version}"
actual_tag = os.environ.get("GITHUB_REF_NAME", "")

if actual_tag != expected_tag:
    print(
        "version/tag mismatch: "
        f"Cargo.toml version is {version}, expected tag {expected_tag}, got {actual_tag}"
    )
    sys.exit(1)

print(f"version/tag ok: {actual_tag}")
