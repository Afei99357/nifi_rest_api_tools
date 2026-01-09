#!/usr/bin/env python3
"""
Test NiFi Status API with recursive=true parameter.
"""

import requests
import json
import sys

# Configuration
BASE_URL = "https://us-chd01-prod-nifi.us-chd01.nxp.com:8443/nifi-api"
USERNAME = "nxg16670"
PASSWORD = "6be!x!_Ex855cXJ"

def get_token():
    """Authenticate and get token."""
    response = requests.post(
        f"{BASE_URL}/access/token",
        data={'username': USERNAME, 'password': PASSWORD},
        verify=False
    )
    if response.status_code == 201:
        return response.text
    raise Exception(f"Auth failed: {response.status_code}")

def test_status_api(group_id, token):
    """Compare Status API with and without recursive=true."""

    headers = {'Authorization': f'Bearer {token}'}

    print(f"Testing group: {group_id[:16]}...\n")

    # Test WITHOUT recursive=true
    print("1. WITHOUT recursive=true:")
    response1 = requests.get(
        f"{BASE_URL}/flow/process-groups/{group_id}/status",
        headers=headers,
        verify=False
    )
    data1 = response1.json()

    # Count connections in aggregateSnapshot
    connections1 = data1.get('processGroupStatus', {}).get('aggregateSnapshot', {}).get('connectionStatusSnapshots', [])
    print(f"   Connections in aggregateSnapshot: {len(connections1)}")

    # Check if there are child group statuses
    child_statuses1 = data1.get('processGroupStatus', {}).get('processGroupStatus', [])
    print(f"   Child group statuses: {len(child_statuses1)}")

    # Test WITH recursive=true
    print("\n2. WITH recursive=true:")
    response2 = requests.get(
        f"{BASE_URL}/flow/process-groups/{group_id}/status?recursive=true",
        headers=headers,
        verify=False
    )
    data2 = response2.json()

    connections2 = data2.get('processGroupStatus', {}).get('aggregateSnapshot', {}).get('connectionStatusSnapshots', [])
    print(f"   Connections in aggregateSnapshot: {len(connections2)}")

    child_statuses2 = data2.get('processGroupStatus', {}).get('processGroupStatus', [])
    print(f"   Child group statuses: {len(child_statuses2)}")

    # Compare
    print("\n3. Comparison:")
    if len(connections1) == len(connections2):
        print(f"   ✓ Same connection count ({len(connections1)})")
        print(f"   → aggregateSnapshot ALREADY includes child data")
        print(f"   → recursive=true may not be necessary for Status API")
    else:
        print(f"   ⚠ Different connection counts!")
        print(f"   → WITHOUT recursive: {len(connections1)}")
        print(f"   → WITH recursive: {len(connections2)}")
        print(f"   → recursive=true changes the response")

    print("\n4. Response structure:")
    print(f"   Keys in processGroupStatus: {list(data1.get('processGroupStatus', {}).keys())}")

    return connections1, connections2

if __name__ == '__main__':
    if len(sys.argv) < 2:
        print("Usage: python3 test_recursive_api.py <group_id>")
        print("\nExample group IDs from your CSV:")
        print("  - Use one of your flow IDs from nifi_group_ids_prod.csv")
        sys.exit(1)

    group_id = sys.argv[1]

    print("Getting authentication token...")
    token = get_token()
    print("✓ Token obtained\n")

    test_status_api(group_id, token)
