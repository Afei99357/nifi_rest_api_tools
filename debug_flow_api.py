#!/usr/bin/env python3
"""
Debug Flow API response structure to fix recursion.
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

def inspect_flow_api(group_id, token):
    """Inspect Flow API response structure."""

    headers = {'Authorization': f'Bearer {token}'}

    print(f"Inspecting Flow API for group: {group_id[:16]}...\n")

    response = requests.get(
        f"{BASE_URL}/flow/process-groups/{group_id}",
        headers=headers,
        verify=False
    )
    data = response.json()

    # Navigate to flow structure
    pg_flow = data.get('processGroupFlow', {})
    flow = pg_flow.get('flow', {})

    processors = flow.get('processors', [])
    child_groups = flow.get('processGroups', [])

    print(f"Top-level processors: {len(processors)}")
    print(f"Child process groups: {len(child_groups)}\n")

    if processors:
        print("Sample processor structure:")
        sample = processors[0]
        print(f"  Keys: {list(sample.keys())}")
        print(f"  ID: {sample.get('id', 'N/A')[:16]}")
        print(f"  Component keys: {list(sample.get('component', {}).keys())}")
        print(f"  Name: {sample.get('component', {}).get('name', 'N/A')}")
        print()

    if child_groups:
        print("Child process group structure:")
        for i, child in enumerate(child_groups[:3]):  # Show first 3
            print(f"\n  Child {i+1}:")
            print(f"    Keys: {list(child.keys())}")
            print(f"    Has 'id' at top level: {'id' in child}")
            print(f"    Has 'component': {'component' in child}")

            if 'id' in child:
                print(f"    ID (top level): {child['id'][:16]}")

            if 'component' in child:
                comp = child['component']
                print(f"    Component keys: {list(comp.keys())}")
                if 'id' in comp:
                    print(f"    ID (in component): {comp['id'][:16]}")
                if 'name' in comp:
                    print(f"    Name: {comp['name']}")

        print(f"\n  ... and {len(child_groups) - 3} more child groups" if len(child_groups) > 3 else "")

        # Try to recurse into first child
        if child_groups:
            print("\n--- Recursing into first child ---")
            first_child = child_groups[0]

            # Try different ways to get the ID
            child_id = None
            if 'id' in first_child:
                child_id = first_child['id']
                print(f"✓ Found ID at top level: {child_id[:16]}")
            elif 'component' in first_child and 'id' in first_child['component']:
                child_id = first_child['component']['id']
                print(f"✓ Found ID in component: {child_id[:16]}")
            else:
                print("✗ Could not find ID!")
                print(f"  Available keys: {list(first_child.keys())}")

            if child_id:
                # Recurse
                child_response = requests.get(
                    f"{BASE_URL}/flow/process-groups/{child_id}",
                    headers=headers,
                    verify=False
                )
                child_data = child_response.json()
                child_flow = child_data.get('processGroupFlow', {}).get('flow', {})
                child_processors = child_flow.get('processors', [])
                child_child_groups = child_flow.get('processGroups', [])

                print(f"  Processors in child: {len(child_processors)}")
                print(f"  Child groups in child: {len(child_child_groups)}")

    # Calculate total if we recursed
    def count_recursive(gid):
        """Recursively count all processors."""
        resp = requests.get(
            f"{BASE_URL}/flow/process-groups/{gid}",
            headers=headers,
            verify=False
        )
        d = resp.json()
        f = d.get('processGroupFlow', {}).get('flow', {})
        procs = f.get('processors', [])
        children = f.get('processGroups', [])

        total = len(procs)
        for child in children:
            child_id = child.get('id')  # Try top level
            if not child_id and 'component' in child:
                child_id = child.get('component', {}).get('id')  # Try component

            if child_id:
                try:
                    total += count_recursive(child_id)
                except Exception as e:
                    print(f"Error recursing into {child_id[:16]}: {e}")

        return total

    print("\n--- Recursive Count ---")
    total = count_recursive(group_id)
    print(f"Total processors (recursive): {total}")

if __name__ == '__main__':
    if len(sys.argv) < 2:
        print("Usage: python3 debug_flow_api.py <group_id>")
        print("\nThis will show the actual Flow API response structure")
        print("to help fix the recursion bug.")
        sys.exit(1)

    group_id = sys.argv[1]

    import urllib3
    urllib3.disable_warnings()

    print("Getting authentication token...")
    token = get_token()
    print("✓ Token obtained\n")

    inspect_flow_api(group_id, token)
