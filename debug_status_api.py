#!/usr/bin/env python3
"""
Debug script to show the exact structure of the Status API response
"""

import json
import argparse
from pathlib import Path
from analyzer import NiFiClient

def load_config(config_path: Path):
    """Load configuration from YAML file."""
    if not config_path.exists():
        return {}

    config = {}
    with open(config_path) as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith('#'):
                continue
            if ':' in line:
                key, value = line.split(':', 1)
                key = key.strip()
                value = value.split('#')[0].strip().strip('"').strip("'")
                config[key] = value
    return config

def main():
    parser = argparse.ArgumentParser(description="Debug NiFi Status API response")
    parser.add_argument('--config', type=Path, default=Path('config.yaml'))
    parser.add_argument('--group-id', help='Process group ID to analyze')
    args = parser.parse_args()

    # Load config
    config = load_config(args.config)
    nifi_url = config.get('nifi_url')
    username = config.get('username')
    password = config.get('password')
    group_id = args.group_id or config.get('process_group_id')
    verify_ssl = config.get('verify_ssl', 'false').lower() != 'false'

    print(f"\n=== Connecting to NiFi ===")
    print(f"URL: {nifi_url}")
    print(f"Group ID: {group_id}\n")

    # Connect
    client = NiFiClient(
        base_url=nifi_url,
        username=username,
        password=password,
        verify_ssl=verify_ssl
    )

    print(f"=== Fetching Status API ===\n")
    status_data = client.get_process_group_status(group_id)

    print(f"=== Top-level keys ===")
    print(json.dumps(list(status_data.keys()), indent=2))

    pg_status = status_data.get("processGroupStatus", {})
    if pg_status:
        print(f"\n=== processGroupStatus keys ===")
        print(json.dumps(list(pg_status.keys()), indent=2))

        proc_status_list = pg_status.get("processorStatus", [])
        print(f"\n=== Found {len(proc_status_list)} processors ===\n")

        if proc_status_list:
            print(f"=== First processor complete structure ===")
            print(json.dumps(proc_status_list[0], indent=2))

            print(f"\n=== Checking aggregateSnapshot ===")
            snapshot = proc_status_list[0].get("aggregateSnapshot", {})
            print(f"aggregateSnapshot keys: {list(snapshot.keys())}")

            print(f"\n=== All fields in aggregateSnapshot ===")
            print(json.dumps(snapshot, indent=2))

            print(f"\n=== Invocations value ===")
            invocations = snapshot.get("invocations")
            print(f"Type: {type(invocations)}")
            print(f"Value: {invocations}")
    else:
        print("\nERROR: No processGroupStatus in response!")
        print(f"\nFull response:")
        print(json.dumps(status_data, indent=2))

    client.close()
    print("\n=== Done ===\n")

if __name__ == "__main__":
    main()
