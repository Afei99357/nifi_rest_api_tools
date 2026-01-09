"""
Processor Usage Analyzer

Analyzes NiFi processor execution frequency to identify unused or underutilized
processors for pruning decisions.
"""

import csv
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional

import matplotlib.pyplot as plt
from rich.console import Console

from .nifi_client import NiFiClient


class ProcessorUsageAnalyzer:
    """
    Analyzes processor execution frequency using NiFi Status API.

    This class queries processor execution counts (invocations) to identify:
    - Unused processors (0 executions since creation)
    - Low-usage processors (<10 total executions)

    Results are used to identify candidates for pruning.

    Args:
        client: Initialized NiFiClient instance
    """

    def __init__(self, client: NiFiClient):
        self.client = client
        self.console = Console()

        # Analysis results (populated by analyze())
        self.process_group_id: Optional[str] = None
        self.flow_name: Optional[str] = None  # Flow name for batch mode
        self.server: Optional[str] = None  # Server identifier (e.g., hostname)
        self.snapshot_timestamp: Optional[datetime] = None  # When analysis ran
        self.connection_statistics: List[Dict] = []  # Connection-level data with ALL fields
        self.target_processors: List[Dict] = []  # Still needed for processor type info

    def analyze(self, process_group_id: str, flow_name: Optional[str] = None, server: Optional[str] = None) -> None:
        """
        Analyze processor execution counts for a process group.

        This method:
        1. Fetches all processors in the target process group
        2. Queries execution counts from NiFi Status API
        3. Stores results for report generation

        Args:
            process_group_id: The NiFi process group ID to analyze
            flow_name: Optional flow name for tracking (used in batch mode)
            server: Optional server identifier (e.g., hostname or environment name)

        Raises:
            Exception: If unable to fetch processors or execution counts
        """
        self.process_group_id = process_group_id
        self.flow_name = flow_name if flow_name else process_group_id[:8]  # Default to short ID
        self.server = server if server else "unknown"  # Default to "unknown" if not provided
        self.snapshot_timestamp = datetime.now()

        # Phase 1: Display analysis parameters
        self.console.print(f"\n[yellow]Analyzing processor execution counts:[/yellow]")
        self.console.print(f"  Process Group: {process_group_id[:16]}...")

        # Phase 2: Get processors in target group
        self.console.print(
            f"\n[yellow]Phase 1:[/yellow] Getting processors from target process group..."
        )

        try:
            self.target_processors = self.client.list_processors(process_group_id)
            self.console.print(f"[green]OK[/green] Found {len(self.target_processors)} processors")

            # Display processor list (first 10)
            if self.target_processors:
                self.console.print("\n[cyan]Processors in target group:[/cyan]")
                for proc in self.target_processors[:10]:
                    proc_name = proc['component']['name']
                    proc_type = proc['component']['type'].split('.')[-1]
                    self.console.print(f"  • {proc_name} ({proc_type})")
                if len(self.target_processors) > 10:
                    self.console.print(
                        f"  ... and {len(self.target_processors) - 10} more"
                    )

        except Exception as e:
            self.console.print(f"[red]ERROR[/red] Failed to get processors: {e}")
            raise

        # Phase 2: Get connection statistics (ALL fields, connection-level)
        self.console.print(
            f"\n[yellow]Phase 2:[/yellow] Fetching connection statistics (all fields)..."
        )

        try:
            connection_stats = self.client.get_connection_statistics(process_group_id)

            if len(connection_stats) == 0:
                self.console.print(
                    f"[yellow]WARNING[/yellow] No connections found in process group"
                )
                self.console.print(
                    f"[yellow]Hint:[/yellow] This may indicate an empty flow or run with --verbose for details"
                )
            else:
                self.console.print(
                    f"[green]OK[/green] Retrieved {len(connection_stats)} connections"
                )

            # Store raw connection data (no aggregation)
            self.connection_statistics = connection_stats

        except Exception as e:
            self.console.print(f"[red]ERROR[/red] Failed to fetch connection statistics: {e}")
            raise  # Cannot continue without this data

    def get_detailed_results(self) -> List[Dict]:
        """
        Get detailed results with all metadata for batch export.

        Returns connection-level data with ALL 24 fields from NiFi Status API.
        Used for creating combined CSV output and Delta Lake storage in batch mode.

        Returns:
            List of dictionaries with connection data (all available fields)

        Example:
            [
                {
                    'snapshot_timestamp': datetime(2026, 1, 9, 14, 30, 22),
                    'server': 'prod-nifi-01',
                    'flow_name': 'Production_Flow',
                    'process_group_id': '8c8677c4-29d6-...',
                    'connection_id': 'conn-abc-123',
                    'connection_name': 'success',
                    'connection_group_id': '8c8677c4-...',
                    'source_id': 'proc-123',
                    'source_name': 'FetchSFTP',
                    'destination_id': 'proc-456',
                    'destination_name': 'PutHDFS',
                    'flow_files_in': 1250,
                    'flow_files_out': 1250,
                    'bytes_in': 52000,
                    'bytes_out': 52000,
                    'input': '1,250 (50.8 KB)',
                    'output': '1,250 (50.8 KB)',
                    'queued_count': 0,
                    'queued_bytes': 0,
                    'queued': '0 (0 bytes)',
                    'queued_size': '0 bytes',
                    'percent_use_count': 0,
                    'percent_use_bytes': 0,
                    'stats_last_refreshed': '2026-01-09T14:30:22Z'
                },
                ...
            ]
        """
        results = []

        # Transform connection statistics to include metadata
        for conn in self.connection_statistics:
            results.append({
                # Metadata
                'snapshot_timestamp': self.snapshot_timestamp,
                'server': self.server,
                'flow_name': self.flow_name,
                'process_group_id': self.process_group_id,

                # Connection identity
                'connection_id': conn.get('id'),
                'connection_name': conn.get('name', ''),
                'connection_group_id': conn.get('groupId'),

                # Source processor
                'source_id': conn.get('sourceId'),
                'source_name': conn.get('sourceName'),

                # Destination processor
                'destination_id': conn.get('destinationId'),
                'destination_name': conn.get('destinationName'),

                # Flow metrics (5-minute window)
                'flow_files_in': conn.get('flowFilesIn', 0),
                'flow_files_out': conn.get('flowFilesOut', 0),
                'bytes_in': conn.get('bytesIn', 0),
                'bytes_out': conn.get('bytesOut', 0),
                'input': conn.get('input', ''),
                'output': conn.get('output', ''),

                # Queue metrics (current state)
                'queued_count': conn.get('queuedCount', 0),
                'queued_bytes': conn.get('queuedBytes', 0),
                'queued': conn.get('queued', ''),
                'queued_size': conn.get('queuedSize', ''),

                # Status indicators
                'percent_use_count': conn.get('percentUseCount', 0),
                'percent_use_bytes': conn.get('percentUseBytes', 0),

                # Timestamps
                'stats_last_refreshed': conn.get('statsLastRefreshed', '')
            })

        return results

    def generate_report(self, output_prefix: Optional[str] = None) -> None:
        """
        Generate CSV and visualization reports.

        Creates:
        - CSV file with execution count metrics
        - PNG bar chart showing execution frequency (color-coded)
        - Console summary with pruning candidates

        Args:
            output_prefix: Prefix for output files (default: processor_usage_[GROUP_ID])
        """
        if not self.connection_statistics:
            self.console.print(
                "[red]Error:[/red] No analysis results. Run analyze() first."
            )
            return

        # Determine output file prefix
        if output_prefix is None:
            group_id_short = self.process_group_id[:8] if self.process_group_id else "unknown"
            output_prefix = f"processor_usage_{group_id_short}"

        self.console.print(f"\n[yellow]Phase 3:[/yellow] Generating reports...")

        # Create output directory if it doesn't exist
        output_path = Path(output_prefix)
        output_dir = output_path.parent
        if output_dir and str(output_dir) != '.':
            output_dir.mkdir(parents=True, exist_ok=True)

        # Sort connections by flowfile output count (highest to lowest)
        sorted_connections = sorted(
            self.connection_statistics,
            key=lambda x: x.get('flowFilesOut', 0),
            reverse=True
        )

        # 1. Save to CSV with ALL 24 fields
        csv_file = Path(f"{output_prefix}.csv")
        with open(csv_file, 'w', newline='') as f:
            writer = csv.writer(f)
            # Write header with all 24 fields
            writer.writerow([
                'snapshot_timestamp', 'server', 'flow_name', 'process_group_id',
                'connection_id', 'connection_name', 'connection_group_id',
                'source_id', 'source_name', 'destination_id', 'destination_name',
                'flow_files_in', 'flow_files_out', 'bytes_in', 'bytes_out',
                'input', 'output', 'queued_count', 'queued_bytes', 'queued', 'queued_size',
                'percent_use_count', 'percent_use_bytes', 'stats_last_refreshed'
            ])

            # Write connection data
            for conn in sorted_connections:
                writer.writerow([
                    self.snapshot_timestamp, self.server, self.flow_name, self.process_group_id,
                    conn.get('id'), conn.get('name', ''), conn.get('groupId'),
                    conn.get('sourceId'), conn.get('sourceName'),
                    conn.get('destinationId'), conn.get('destinationName'),
                    conn.get('flowFilesIn', 0), conn.get('flowFilesOut', 0),
                    conn.get('bytesIn', 0), conn.get('bytesOut', 0),
                    conn.get('input', ''), conn.get('output', ''),
                    conn.get('queuedCount', 0), conn.get('queuedBytes', 0),
                    conn.get('queued', ''), conn.get('queuedSize', ''),
                    conn.get('percentUseCount', 0), conn.get('percentUseBytes', 0),
                    conn.get('statsLastRefreshed', '')
                ])

        self.console.print(f"[green]OK[/green] Saved CSV: {csv_file}")

        # 2. Generate bar chart (aggregate connections to processor-level for visualization)
        # Build processor activity by aggregating connections from each source
        processor_activity = {}
        for conn in self.connection_statistics:
            source_name = conn.get('sourceName', 'Unknown')
            if source_name not in processor_activity:
                processor_activity[source_name] = {
                    'flowFilesOut': 0,
                    'bytesOut': 0
                }
            processor_activity[source_name]['flowFilesOut'] += conn.get('flowFilesOut', 0)
            processor_activity[source_name]['bytesOut'] += conn.get('bytesOut', 0)

        # Sort by flowfile output (highest to lowest)
        sorted_processors = sorted(
            processor_activity.items(),
            key=lambda x: x[1]['flowFilesOut'],
            reverse=True
        )

        fig, ax = plt.subplots(figsize=(12, max(8, len(sorted_processors) * 0.4)))

        names = [name for name, _ in sorted_processors]
        flowfiles_out = [data['flowFilesOut'] for _, data in sorted_processors]

        # Color code: red = 0, orange = 1-9, blue = 10+
        colors = ['red' if i == 0 else 'orange' if i < 10 else 'steelblue' for i in flowfiles_out]

        ax.barh(names, flowfiles_out, color=colors)
        ax.set_xlabel('FlowFiles Out (Snapshot)', fontsize=12)
        ax.set_ylabel('Processor Name', fontsize=12)
        ax.set_title(
            f'Processor Activity (FlowFiles Output)\n'
            f'Process Group: {self.process_group_id[:16] if self.process_group_id else "unknown"}...',
            fontsize=14,
            fontweight='bold'
        )
        ax.grid(axis='x', alpha=0.3, linestyle='--')

        plt.tight_layout()

        plot_file = Path(f"{output_prefix}.png")
        plt.savefig(plot_file, dpi=150, bbox_inches='tight')
        self.console.print(f"[green]OK[/green] Saved plot: {plot_file}")

        # Close the figure to free memory
        plt.close(fig)

        # 3. Print summary
        total_flowfiles = sum(data['flowFilesOut'] for _, data in sorted_processors)
        total_bytes = sum(data['bytesOut'] for _, data in sorted_processors)
        unused_count = sum(1 for _, data in sorted_processors if data['flowFilesOut'] == 0)
        low_usage_count = sum(1 for _, data in sorted_processors if 0 < data['flowFilesOut'] < 10)

        self.console.print(f"\n[cyan]Summary:[/cyan]")
        self.console.print(f"  Total processors: {len(sorted_processors)}")
        self.console.print(f"  Total connections: {len(self.connection_statistics)}")
        self.console.print(f"  Total flowfiles output (snapshot): {total_flowfiles:,}")
        self.console.print(f"  Total bytes output (snapshot): {total_bytes:,}")
        self.console.print(f"  No output: {unused_count} processors")
        self.console.print(f"  Low output (<10 flowfiles): {low_usage_count} processors")

        # Build processor name -> type lookup from target_processors
        processor_types = {}
        for proc in self.target_processors:
            proc_name = proc['component']['name']
            proc_type = proc['component']['type'].split('.')[-1]
            processor_types[proc_name] = proc_type

        # Show pruning candidates
        if unused_count > 0:
            self.console.print(
                f"\n[yellow]WARNING: Processors with 0 flowfile output (candidates for pruning):[/yellow]"
            )
            for name, data in sorted_processors:
                if data['flowFilesOut'] == 0:
                    proc_type = processor_types.get(name, 'Unknown')
                    self.console.print(f"  • {name} ({proc_type})")

        # Show low usage processors
        if low_usage_count > 0:
            self.console.print(
                f"\n[yellow]WARNING: Processors with low flowfile output (<10 flowfiles):[/yellow]"
            )
            for name, data in sorted_processors:
                if 0 < data['flowFilesOut'] < 10:
                    proc_type = processor_types.get(name, 'Unknown')
                    self.console.print(f"  • {name} ({proc_type}): {data['flowFilesOut']} flowfiles")

        self.console.print(f"\n[green]OK[/green] Analysis complete!")
        self.console.print(f"\n[cyan]Next steps:[/cyan]")
        self.console.print(f"  1. Review the bar chart: {plot_file}")
        self.console.print(f"  2. Review the CSV: {csv_file}")
        self.console.print(f"  3. Take snapshots over time to identify inactive processors (deltas)")
