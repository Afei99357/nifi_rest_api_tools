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
import logging
import sys
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
        help='Process group ID to analyze'
    )
    parser.add_argument(
        '--days',
        type=int,
        default=1,
        help='Number of days to look back for provenance (default: 1, ignored if --execution-only)'
    )
    parser.add_argument(
        '--max-events',
        type=int,
        default=10000,
        help='Maximum events per processor (default: 10000)'
    )
    parser.add_argument(
        '--no-verify-ssl',
        action='store_true',
        help='Disable SSL certificate verification'
    )
    parser.add_argument(
        '--execution-only',
        action='store_true',
        help='Show execution count only (skip provenance queries for faster results)'
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
    days_back = args.days if args.days != 30 else int(config.get('days_back', 30))
    verify_ssl = not args.no_verify_ssl and config.get('verify_ssl', 'false').lower() != 'false'

    # Validate required parameters
    missing = []
    if not nifi_url:
        missing.append('--url or nifi_url in config')
    if not username:
        missing.append('--username or username in config')
    if not password:
        missing.append('--password or password in config')
    if not group_id:
        missing.append('--group-id or process_group_id in config')

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
    console.print(f"  Process Group ID: {group_id[:16]}...")
    console.print(f"  Execution-only mode: {args.execution_only}")
    if not args.execution_only:
        console.print(f"  Days back (provenance): {days_back}")
        console.print(f"  Max events per processor: {args.max_events}")
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
        console.print("[green]✓[/green] Connected successfully")

        # Create analyzer
        analyzer = ProcessorUsageAnalyzer(
            client=client,
            days_back=days_back,
            max_events_per_processor=args.max_events,
            execution_only=args.execution_only
        )

        # Run analysis
        analyzer.analyze(group_id)

        # Generate reports
        analyzer.generate_report(output_prefix=args.output_prefix)

        # Cleanup
        client.close()

        console.print("\n[green]✓[/green] Done!")
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
