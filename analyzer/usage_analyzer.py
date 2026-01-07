"""
Processor Usage Analyzer

Analyzes NiFi processor execution frequency to identify unused or underutilized
processors for pruning decisions.
"""

import csv
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
        self.processor_event_counts: Dict[str, Dict] = {}
        self.processor_invocation_counts: Dict[str, int] = {}
        self.target_processors: List[Dict] = []

    def analyze(self, process_group_id: str) -> None:
        """
        Analyze processor execution counts for a process group.

        This method:
        1. Fetches all processors in the target process group
        2. Queries execution counts from NiFi Status API
        3. Stores results for report generation

        Args:
            process_group_id: The NiFi process group ID to analyze

        Raises:
            Exception: If unable to fetch processors or execution counts
        """
        self.process_group_id = process_group_id

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

        # Phase 2: Get processor execution counts (fast, single API call)
        self.console.print(
            f"\n[yellow]Phase 2:[/yellow] Fetching execution statistics..."
        )

        try:
            exec_stats = self.client.get_processor_invocation_counts(process_group_id)
            # Store invocation counts by processor ID
            for proc_id, stats in exec_stats.items():
                self.processor_invocation_counts[proc_id] = stats['invocations']

            if len(exec_stats) == 0 and len(self.target_processors) > 0:
                self.console.print(
                    f"[yellow]WARNING[/yellow] Retrieved execution counts for {len(exec_stats)} processors "
                    f"(expected {len(self.target_processors)})"
                )
                self.console.print(
                    f"[yellow]Hint:[/yellow] Run with --verbose to see detailed API response structure"
                )
            else:
                self.console.print(
                    f"[green]OK[/green] Retrieved execution counts for {len(exec_stats)} processors"
                )
        except Exception as e:
            self.console.print(f"[red]ERROR[/red] Failed to fetch execution counts: {e}")
            raise  # Cannot continue without this data

        # Build processor_event_counts from execution counts
        self.processor_event_counts = {}
        for proc in self.target_processors:
            proc_id = proc['id']
            proc_name = proc['component']['name']
            proc_type = proc['component']['type'].split('.')[-1]

            self.processor_event_counts[proc_name] = {
                'id': proc_id,
                'invocations': self.processor_invocation_counts.get(proc_id, 0),
                'type': proc_type
            }

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
        if not self.processor_event_counts:
            self.console.print(
                "[red]Error:[/red] No analysis results. Run analyze() first."
            )
            return

        # Determine output file prefix
        if output_prefix is None:
            group_id_short = self.process_group_id[:8] if self.process_group_id else "unknown"
            output_prefix = f"processor_usage_{group_id_short}"

        self.console.print(f"\n[yellow]Phase 3:[/yellow] Generating reports...")

        # Sort by execution count (highest to lowest)
        sorted_processors = sorted(
            self.processor_event_counts.items(),
            key=lambda x: x[1]['invocations'],
            reverse=True
        )

        # 1. Save to CSV
        csv_file = Path(f"{output_prefix}.csv")
        with open(csv_file, 'w', newline='') as f:
            writer = csv.writer(f)
            writer.writerow(['Processor Name', 'Processor Type', 'Execution Count (Total)'])

            for name, data in sorted_processors:
                writer.writerow([name, data['type'], data['invocations']])

        self.console.print(f"[green]OK[/green] Saved CSV: {csv_file}")

        # 2. Generate bar chart
        fig, ax = plt.subplots(figsize=(12, max(8, len(self.target_processors) * 0.4)))

        names = [name for name, _ in sorted_processors]
        invocations = [data['invocations'] for _, data in sorted_processors]

        # Color code: red = 0, orange = 1-9, blue = 10+
        colors = ['red' if i == 0 else 'orange' if i < 10 else 'steelblue' for i in invocations]

        ax.barh(names, invocations, color=colors)
        ax.set_xlabel('Execution Count (Total)', fontsize=12)
        ax.set_ylabel('Processor Name', fontsize=12)
        ax.set_title(
            f'Processor Execution Count\n'
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
        total_invocations = sum(data['invocations'] for _, data in sorted_processors)
        unused_count = sum(1 for _, data in sorted_processors if data['invocations'] == 0)
        low_usage_count = sum(1 for _, data in sorted_processors if 0 < data['invocations'] < 10)

        self.console.print(f"\n[cyan]Summary:[/cyan]")
        self.console.print(f"  Total processors: {len(self.target_processors)}")
        self.console.print(f"  Total executions (all time): {total_invocations:,}")
        self.console.print(f"  Never executed: {unused_count} processors")
        self.console.print(f"  Low usage (<10 executions): {low_usage_count} processors")

        # Show pruning candidates
        if unused_count > 0:
            self.console.print(
                f"\n[yellow]WARNING: Processors with 0 executions (candidates for pruning):[/yellow]"
            )
            for name, data in sorted_processors:
                if data['invocations'] == 0:
                    self.console.print(f"  • {name} ({data['type']})")

        # Show low usage processors
        if low_usage_count > 0:
            self.console.print(
                f"\n[yellow]WARNING: Processors with low execution count (<10 invocations):[/yellow]"
            )
            for name, data in sorted_processors:
                if 0 < data['invocations'] < 10:
                    self.console.print(f"  • {name} ({data['type']}): {data['invocations']} executions")

        self.console.print(f"\n[green]OK[/green] Analysis complete!")
        self.console.print(f"\n[cyan]Next steps:[/cyan]")
        self.console.print(f"  1. Review the bar chart: {plot_file}")
        self.console.print(f"  2. Review the CSV: {csv_file}")
        self.console.print(f"  3. Consider pruning unused processors")
