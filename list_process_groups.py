#!/usr/bin/env python3
"""
List all child process groups in the root canvas and output to CSV.

This script connects to NiFi and lists all child process groups (flows)
in the specified process group (usually root), outputting their ID and name.
"""

import csv
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
                # Strip inline comments (everything after #)
                value = value.split('#')[0].strip().strip('"').strip("'")
                config[key] = value
    return config


def main():
    parser = argparse.ArgumentParser(
        description="List all child process groups and output to CSV"
    )
    parser.add_argument(
        '--config',
        type=Path,
        default=Path('config.yaml'),
        help='Path to config file (default: config.yaml)'
    )
    parser.add_argument(
        '--group-id',
        help='Process group ID to list children from (default: from config or root)'
    )
    parser.add_argument(
        '--output',
        type=Path,
        default=Path('process_groups.csv'),
        help='Output CSV file (default: process_groups.csv)'
    )
    args = parser.parse_args()

    # Load config
    config = load_config(args.config)
    nifi_url = config.get('nifi_url')
    username = config.get('username')
    password = config.get('password')
    group_id = args.group_id or config.get('process_group_id', 'root')
    verify_ssl = config.get('verify_ssl', 'false').lower() != 'false'

    if not nifi_url or not username or not password:
        print("ERROR: Missing required config: nifi_url, username, password")
        print(f"Check your config file at: {args.config}")
        return

    print(f"\nConnecting to NiFi: {nifi_url}")
    print(f"Process Group ID: {group_id}\n")

    # Connect to NiFi
    client = NiFiClient(
        base_url=nifi_url,
        username=username,
        password=password,
        verify_ssl=verify_ssl
    )

    # Get process group data
    print(f"Fetching process group data...")
    pg_data = client.get_process_group(group_id)

    # Extract child process groups
    child_groups = pg_data.get("processGroupFlow", {}).get("flow", {}).get("processGroups", [])

    if not child_groups:
        print(f"\nNo child process groups found in group {group_id}")
        client.close()
        return

    print(f"Found {len(child_groups)} child process groups\n")

    # Write to CSV
    with open(args.output, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(['id', 'flow_name'])

        for group in child_groups:
            group_id = group['id']
            flow_name = group['component']['name']
            writer.writerow([group_id, flow_name])
            print(f"  {flow_name}: {group_id}")

    print(f"\nSaved to: {args.output}")
    client.close()


if __name__ == "__main__":
    main()
