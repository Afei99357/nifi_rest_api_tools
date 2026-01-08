#!/usr/bin/env python3
"""
Debug script to show exactly what's being read from config.yaml
"""

import argparse
from pathlib import Path
from typing import Dict, Any


def load_config(config_path: Path) -> Dict[str, Any]:
    """Load configuration from YAML file (same logic as analyze.py)."""
    if not config_path.exists():
        return {}

    config = {}
    try:
        # Simple YAML parser (no external dependencies)
        with open(config_path) as f:
            for line in f:
                line = line.strip()
                # Skip comments and empty lines
                if not line or line.startswith('#'):
                    continue
                # Parse key: value pairs
                if ':' in line:
                    key, value = line.split(':', 1)
                    key = key.strip()
                    # Strip inline comments (everything after #)
                    value = value.split('#')[0].strip().strip('"').strip("'")
                    config[key] = value
    except Exception as e:
        print(f"ERROR: Failed to parse config file: {e}")

    return config


def main():
    parser = argparse.ArgumentParser(description="Debug config.yaml parsing")
    parser.add_argument(
        '--config',
        type=Path,
        default=Path('config.yaml'),
        help='Path to config file (default: config.yaml)'
    )
    args = parser.parse_args()

    print(f"\n=== CONFIG FILE DEBUG ===\n")
    print(f"Looking for config file at: {args.config}")
    print(f"Absolute path: {args.config.absolute()}")
    print(f"File exists: {args.config.exists()}\n")

    if not args.config.exists():
        print(f"ERROR: Config file not found at {args.config}")
        print(f"\nTo fix: Create config.yaml or use --config to specify the path")
        return

    # Show raw file contents
    print("=== RAW FILE CONTENTS ===\n")
    with open(args.config) as f:
        contents = f.read()
        print(contents)

    print("\n=== PARSED CONFIG VALUES ===\n")
    config = load_config(args.config)

    for key, value in config.items():
        print(f"{key}: '{value}' (type: {type(value).__name__})")

    print("\n=== SSL VERIFICATION LOGIC ===\n")

    # Show the exact logic from analyze.py line 154
    verify_ssl_str = config.get('verify_ssl', 'false')
    print(f"1. config.get('verify_ssl', 'false') = '{verify_ssl_str}'")

    verify_ssl_lower = verify_ssl_str.lower()
    print(f"2. .lower() = '{verify_ssl_lower}'")

    not_false = verify_ssl_lower != 'false'
    print(f"3. != 'false' = {not_false}")

    # Assuming no --no-verify-ssl flag
    verify_ssl = not_false
    print(f"4. Final verify_ssl value = {verify_ssl}")

    print("\n=== RESULT ===\n")
    if verify_ssl:
        print("❌ SSL verification is ENABLED (verify_ssl=True)")
        print("   This will fail with self-signed certificates!")
        print("\n   To fix, change your config.yaml to have:")
        print("   verify_ssl: false")
    else:
        print("✅ SSL verification is DISABLED (verify_ssl=False)")
        print("   This should work with self-signed certificates.")

    print("\n=== TROUBLESHOOTING ===\n")
    print("If the value above doesn't match what you expect:")
    print("1. Check you're editing the correct config.yaml file")
    print("2. Check for typos (e.g., 'flase' instead of 'false')")
    print("3. Make sure there's no inline comment like: verify_ssl: true  # false")
    print("4. Check for extra spaces or special characters")
    print("\nYour config should have this exact line:")
    print("verify_ssl: false")
    print()


if __name__ == "__main__":
    main()
