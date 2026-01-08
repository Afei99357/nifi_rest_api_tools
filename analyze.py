#!/usr/bin/env python3
"""
NiFi Processor Usage Analyzer - CLI Entry Point

Analyzes processor execution frequency over a time period to identify
unused or underutilized processors for pruning decisions.

Usage:
    # Using config file
    python analyze.py

    # Using command-line arguments
    python analyze.py --url https://nifi.company.com:8443/nifi \\
                      --username admin \\
                      --password password \\
                      --group-id abc-123 \\
                      --days 30

Configuration:
    Create a config.yaml file (copy from config.example.yaml) with:
    - nifi_url: NiFi URL
    - username: NiFi username
    - password: NiFi password
    - process_group_id: Target process group ID
    - days_back: Days to look back (default: 30)
"""

import argparse
import csv
import logging
import sys
from datetime import datetime
from pathlib import Path
from typing import Dict, Any

from analyzer import NiFiClient, ProcessorUsageAnalyzer, NiFiAuthError, NiFiClientError
from rich.console import Console

console = Console()


def load_config(config_path: Path) -> Dict[str, Any]:
    """
    Load configuration from YAML file.

    Args:
        config_path: Path to config.yaml

    Returns:
        Dictionary with configuration values
    """
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
                    value = value.strip().strip('"').strip("'")
                    config[key] = value
    except Exception as e:
        console.print(f"[yellow]Warning:[/yellow] Failed to parse config file: {e}")

    return config


def main():
    """Main entry point."""
    # Set up argument parser
    parser = argparse.ArgumentParser(
        description="Analyze NiFi processor execution frequency for pruning decisions",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Using config.yaml
  python analyze.py

  # Using command-line arguments
  python analyze.py --url https://nifi:8443/nifi --username admin --password pass --group-id abc-123

  # Override config file values
  python analyze.py --days 60
        """
    )

    parser.add_argument(
        '--url',
        help='NiFi URL (e.g., https://nifi.company.com:8443/nifi)'
    )
    parser.add_argument(
        '--username',
        help='NiFi username'
    )
    parser.add_argument(
        '--password',
        help='NiFi password'
    )
    parser.add_argument(
        '--group-id',
        help='Process group ID to analyze (single-flow mode)'
    )
    parser.add_argument(
        '--flows-csv',
        type=str,
        help='Path to CSV file with columns: id,flow_name (enables batch mode)'
    )
    parser.add_argument(
        '--no-verify-ssl',
        action='store_true',
        help='Disable SSL certificate verification'
    )
    parser.add_argument(
        '--config',
        type=Path,
        default=Path('config.yaml'),
        help='Path to config file (default: config.yaml)'
    )
    parser.add_argument(
        '--output-prefix',
        help='Output file prefix (default: processor_usage_[GROUP_ID])'
    )
    parser.add_argument(
        '--verbose',
        action='store_true',
        help='Enable verbose logging'
    )

    args = parser.parse_args()

    # Configure logging
    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )

    # Load config file
    config = load_config(args.config)

    # Command-line args override config file
    nifi_url = args.url or config.get('nifi_url')
    username = args.username or config.get('username')
    password = args.password or config.get('password')
    group_id = args.group_id or config.get('process_group_id')
    flows_csv = args.flows_csv or config.get('flows_csv_path')
    verify_ssl = not args.no_verify_ssl and config.get('verify_ssl', 'false').lower() != 'false'
    output_prefix = args.output_prefix or config.get('output_prefix', 'processor_usage')

    # Validate required parameters
    missing = []
    if not nifi_url:
        missing.append('--url or nifi_url in config')
    if not username:
        missing.append('--username or username in config')
    if not password:
        missing.append('--password or password in config')
    if not group_id and not flows_csv:
        missing.append('--group-id or --flows-csv (or config equivalent)')

    if missing:
        console.print(f"[red]Error:[/red] Missing required parameters:")
        for param in missing:
            console.print(f"  • {param}")
        console.print(f"\nCreate a {args.config} file or provide parameters via command-line.")
        console.print(f"See {args.config}.example for a template.")
        sys.exit(1)

    # Display configuration
    console.print("\n[cyan]Configuration:[/cyan]")
    console.print(f"  NiFi URL: {nifi_url}")
    console.print(f"  Username: {username}")
    if flows_csv:
        console.print(f"  Mode: Batch (flows CSV: {flows_csv})")
    else:
        console.print(f"  Mode: Single-flow (Process Group ID: {group_id[:16]}...)")
    console.print(f"  Verify SSL: {verify_ssl}")

    try:
        # Connect to NiFi
        console.print("\n[yellow]Connecting to NiFi...[/yellow]")
        client = NiFiClient(
            base_url=nifi_url,
            username=username,
            password=password,
            verify_ssl=verify_ssl
        )
        console.print("[green]OK[/green] Connected successfully")

        # Determine mode: batch (multi-flow) or single-flow
        if flows_csv:
            # BATCH MODE: Process multiple flows
            console.print(f"\n[cyan]Batch Mode: Processing flows from {flows_csv}[/cyan]\n")

            # Read flows CSV
            flows = []
            try:
                with open(flows_csv, 'r') as f:
                    reader = csv.DictReader(f)
                    flows = list(reader)
                    if not flows:
                        console.print("[red]ERROR: CSV file is empty[/red]")
                        sys.exit(1)
                    # Validate columns
                    if 'id' not in flows[0] or 'flow_name' not in flows[0]:
                        console.print("[red]ERROR: CSV must have columns: id,flow_name[/red]")
                        sys.exit(1)
            except FileNotFoundError:
                console.print(f"[red]ERROR: Flows CSV not found: {flows_csv}[/red]")
                sys.exit(1)

            console.print(f"[green]Found {len(flows)} flows to analyze[/green]\n")

            # Analyze each flow
            all_results = []
            analyzer = ProcessorUsageAnalyzer(client=client)

            for i, flow in enumerate(flows, 1):
                flow_id = flow['id']
                flow_name = flow['flow_name']

                console.print(f"[yellow]({i}/{len(flows)})[/yellow] Analyzing: [cyan]{flow_name}[/cyan]")

                try:
                    analyzer.analyze(flow_id, flow_name=flow_name)

                    # Generate individual chart
                    chart_prefix = f"{output_prefix}_{flow_name}"
                    analyzer.generate_report(chart_prefix)

                    # Collect detailed results
                    all_results.extend(analyzer.get_detailed_results())

                    console.print(f"  [green]OK[/green] {flow_name}: {len(analyzer.target_processors)} processors\n")

                except Exception as e:
                    console.print(f"  [red]ERROR[/red] {flow_name}: {e}\n")
                    logging.error(f"Failed to analyze {flow_name}: {e}")
                    if args.verbose:
                        import traceback
                        traceback.print_exc()
                    continue

            # Write combined CSV
            if all_results:
                timestamp_str = datetime.now().strftime('%Y%m%d_%H%M%S')
                combined_csv_path = f"{output_prefix}_all_flows_{timestamp_str}.csv"

                with open(combined_csv_path, 'w', newline='') as csvfile:
                    fieldnames = ['snapshot_timestamp', 'flow_name', 'process_group_id',
                                 'processor_id', 'processor_name', 'processor_type', 'invocations']
                    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                    writer.writeheader()
                    writer.writerows(all_results)

                console.print(f"\n[green]OK Combined results saved:[/green]")
                console.print(f"  CSV: {combined_csv_path}")
                console.print(f"  Total processors: {len(all_results)}")
                console.print(f"  Total flows: {len(flows)}")
                console.print(f"\n[cyan]Upload this CSV to Databricks for analysis[/cyan]")
            else:
                console.print("[yellow]WARNING: No results to save[/yellow]")

        else:
            # SINGLE-FLOW MODE: Existing behavior (backward compatible)
            console.print(f"\n[cyan]Single-Flow Mode[/cyan]\n")

            analyzer = ProcessorUsageAnalyzer(client=client)
            analyzer.analyze(group_id)  # No flow_name = uses default
            analyzer.generate_report(output_prefix=output_prefix)

        # Cleanup
        client.close()

        console.print("\n[green]OK[/green] Done!")
        sys.exit(0)

    except NiFiAuthError as e:
        console.print(f"\n[red]Authentication Error:[/red] {e}")
        console.print("Check your username and password.")
        sys.exit(1)

    except NiFiClientError as e:
        console.print(f"\n[red]NiFi Client Error:[/red] {e}")
        sys.exit(1)

    except KeyboardInterrupt:
        console.print("\n[yellow]Interrupted by user[/yellow]")
        sys.exit(130)

    except Exception as e:
        console.print(f"\n[red]Unexpected Error:[/red] {e}")
        if args.verbose:
            import traceback
            traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
