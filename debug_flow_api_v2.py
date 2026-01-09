#!/usr/bin/env python3
"""
Debug Flow API response structure to fix recursion.
Better error handling to show actual API responses.
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
    try:
        response = requests.post(
            f"{BASE_URL}/access/token",
            data={'username': USERNAME, 'password': PASSWORD},
            verify=False
        )
        print(f"Auth response status: {response.status_code}")

        if response.status_code == 201:
            return response.text
        else:
            print(f"Auth failed!")
            print(f"Response: {response.text[:500]}")
            raise Exception(f"Auth failed with status {response.status_code}")
    except Exception as e:
        print(f"Auth error: {e}")
        raise

def inspect_flow_api(group_id, token):
    """Inspect Flow API response structure."""

    headers = {'Authorization': f'Bearer {token}'}

    print(f"Inspecting Flow API for group: {group_id[:16]}...\n")

    try:
        response = requests.get(
            f"{BASE_URL}/flow/process-groups/{group_id}",
            headers=headers,
            verify=False
        )

        print(f"Flow API response status: {response.status_code}")

        if response.status_code != 200:
            print(f"\n[ERROR] API returned status {response.status_code}")
            print(f"Response text (first 1000 chars):")
            print(response.text[:1000])
            return

        # Try to parse JSON
        try:
            data = response.json()
        except Exception as e:
            print(f"\n[ERROR] Failed to parse JSON: {e}")
            print(f"Response text (first 1000 chars):")
            print(response.text[:1000])
            return

        # Navigate to flow structure
        if 'processGroupFlow' not in data:
            print(f"\n[ERROR] No 'processGroupFlow' in response")
            print(f"Available keys: {list(data.keys())}")
            print(f"Response (first 1000 chars): {json.dumps(data, indent=2)[:1000]}")
            return

        pg_flow = data.get('processGroupFlow', {})
        flow = pg_flow.get('flow', {})

        processors = flow.get('processors', [])
        child_groups = flow.get('processGroups', [])

        print(f"✓ Successfully parsed response")
        print(f"Top-level processors: {len(processors)}")
        print(f"Child process groups: {len(child_groups)}\n")

        if processors:
            print("Sample processor structure:")
            sample = processors[0]
            print(f"  Keys: {list(sample.keys())}")
            print(f"  ID: {sample.get('id', 'N/A')[:16]}")
            if 'component' in sample:
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

            if len(child_groups) > 3:
                print(f"\n  ... and {len(child_groups) - 3} more child groups")

            # Try to recurse into first child
            print("\n--- Testing Recursion into First Child ---")
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
                try:
                    child_response = requests.get(
                        f"{BASE_URL}/flow/process-groups/{child_id}",
                        headers=headers,
                        verify=False
                    )

                    if child_response.status_code == 200:
                        child_data = child_response.json()
                        child_flow = child_data.get('processGroupFlow', {}).get('flow', {})
                        child_processors = child_flow.get('processors', [])
                        child_child_groups = child_flow.get('processGroups', [])

                        print(f"  ✓ Successfully recursed")
                        print(f"  Processors in child: {len(child_processors)}")
                        print(f"  Child groups in child: {len(child_child_groups)}")
                    else:
                        print(f"  ✗ Recursion failed with status {child_response.status_code}")
                except Exception as e:
                    print(f"  ✗ Recursion error: {e}")

        # Calculate total with recursion
        print("\n--- Full Recursive Count ---")

        def count_recursive(gid, depth=0):
            """Recursively count all processors."""
            indent = "  " * depth
            try:
                resp = requests.get(
                    f"{BASE_URL}/flow/process-groups/{gid}",
                    headers=headers,
                    verify=False
                )

                if resp.status_code != 200:
                    print(f"{indent}✗ Error: status {resp.status_code}")
                    return 0

                d = resp.json()
                f = d.get('processGroupFlow', {}).get('flow', {})
                procs = f.get('processors', [])
                children = f.get('processGroups', [])

                total = len(procs)
                print(f"{indent}Level {depth}: {len(procs)} processors, {len(children)} child groups")

                for child in children:
                    child_id = child.get('id')  # Try top level
                    if not child_id and 'component' in child:
                        child_id = child.get('component', {}).get('id')  # Try component

                    if child_id:
                        child_name = child.get('component', {}).get('name', child_id[:8])
                        print(f"{indent}  → Recursing into: {child_name}")
                        total += count_recursive(child_id, depth + 1)
                    else:
                        print(f"{indent}  ✗ Could not find ID for child")

                return total
            except Exception as e:
                print(f"{indent}✗ Error: {e}")
                return 0

        total = count_recursive(group_id)
        print(f"\n✓ Total processors (recursive): {total}")

    except Exception as e:
        print(f"\n[ERROR] Unexpected error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == '__main__':
    if len(sys.argv) < 2:
        print("Usage: python3 debug_flow_api_v2.py <group_id>")
        print("\nThis will show the actual Flow API response structure")
        print("to help fix the recursion bug.")
        sys.exit(1)

    group_id = sys.argv[1]

    import urllib3
    urllib3.disable_warnings()

    print("Getting authentication token...")
    try:
        token = get_token()
        print(f"✓ Token obtained (length: {len(token)})\n")
    except Exception as e:
        print(f"✗ Failed to get token: {e}")
        sys.exit(1)

    inspect_flow_api(group_id, token)
