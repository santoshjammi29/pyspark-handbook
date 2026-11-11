#!/usr/bin/env python3
"""
Databricks Asset Bundle (DAB) Linter & Validator
Checks YAML syntax, required bundle keys, and target definitions without requiring live cloud credentials.
Includes pure-Python fallback if pyyaml is not installed.
"""

import sys
from pathlib import Path

def parse_yaml_fallback(text: str):
    """Simple parser for top-level bundle and target keys when pyyaml is missing."""
    data = {}
    lines = text.splitlines()
    current_section = None
    
    for line in lines:
        stripped = line.strip()
        if not stripped or stripped.startswith("#"):
            continue
            
        if not line.startswith(" ") and ":" in line:
            key, val = line.split(":", 1)
            current_section = key.strip()
            data[current_section] = {}
        elif current_section == "bundle" and line.startswith("  "):
            k, v = line.strip().split(":", 1)
            data["bundle"][k.strip()] = v.strip().strip("\"'")
        elif current_section == "targets" and line.startswith("  ") and not line.startswith("    "):
            target_name = line.strip().rstrip(":")
            if "targets" not in data:
                data["targets"] = {}
            data["targets"][target_name] = {}
            current_target = target_name
        elif current_section == "targets" and line.startswith("    ") and ":" in line:
            k, v = line.strip().split(":", 1)
            if "targets" in data and current_target in data["targets"]:
                data["targets"][current_target][k.strip()] = v.strip().strip("\"'")
                
    return data

def lint_bundle(bundle_path: Path):
    if not bundle_path.exists():
        print(f"ERROR: {bundle_path} does not exist.", file=sys.stderr)
        return False
        
    text = bundle_path.read_text(encoding="utf-8")
    
    try:
        import yaml
        data = yaml.safe_load(text)
    except ImportError:
        data = parse_yaml_fallback(text)
    except Exception as e:
        print(f"YAML Syntax Error in {bundle_path}: {e}", file=sys.stderr)
        return False

    errors = []
    if "bundle" not in data or "name" not in data.get("bundle", {}):
        errors.append("Missing required 'bundle.name' field")

    targets = data.get("targets", {})
    if not targets:
        errors.append("No deployment 'targets' defined in bundle")
    else:
        for tname, tconfig in targets.items():
            if not isinstance(tconfig, dict):
                errors.append(f"Target '{tname}' must be a mapping dictionary")
            mode = tconfig.get("mode")
            if mode and mode not in ("development", "production"):
                errors.append(f"Target '{tname}' mode must be 'development' or 'production', got '{mode}'")

    if errors:
        print(f"Bundle Lint Failures in {bundle_path}:")
        for err in errors:
            print(f"  - {err}")
        return False

    print(f"✓ {bundle_path.name} passed structure validation ({len(targets)} targets defined).")
    return True

if __name__ == "__main__":
    target = Path(sys.argv[1]) if len(sys.argv) > 1 else Path("databricks.yml")
    success = lint_bundle(target)
    sys.exit(0 if success else 1)
