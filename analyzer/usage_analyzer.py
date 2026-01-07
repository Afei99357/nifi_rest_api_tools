"""
Processor Usage Analyzer

Analyzes NiFi processor execution frequency to identify unused or underutilized
processors for pruning decisions.
"""

import csv
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional

import matplotlib.pyplot as plt
import numpy as np
from rich.console import Console
from rich.progress import Progress, SpinnerColumn, TextColumn

from .nifi_client import NiFiClient


class ProcessorUsageAnalyzer:
    """
    Analyzes processor execution frequency based on provenance events and execution counts.

    This class queries NiFi provenance data and processor status to determine:
    1. Execution count (total invocations since processor creation)
    2. FlowFiles processed (optional, over specified time period)

    Results are used to identify candidates for pruning (unused or rarely-used processors).

    Args:
        client: Initialized NiFiClient instance
        days_back: Number of days to look back for provenance events (default: 1)
        max_events_per_processor: Maximum events to fetch per processor (default: 10000)
        execution_only: If True, skip provenance queries for faster results (default: False)
    """

    def __init__(
        self,
        client: NiFiClient,
        days_back: int = 1,
        max_events_per_processor: int = 10000,
        execution_only: bool = False
    ):
        self.client = client
        self.days_back = days_back
        self.max_events_per_processor = max_events_per_processor
        self.execution_only = execution_only
        self.console = Console()

        # Analysis results (populated by analyze())
        self.process_group_id: Optional[str] = None
        self.start_date: Optional[datetime] = None
        self.end_date: Optional[datetime] = None
        self.processor_event_counts: Dict[str, Dict] = {}
        self.processor_invocation_counts: Dict[str, int] = {}
        self.target_processors: List[Dict] = []

    def analyze(self, process_group_id: str) -> None:
        """
        Analyze processor usage for a process group.

        This method:
        1. Calculates the date range (past N days)
        2. Fetches all processors in the target process group
        3. Queries provenance events for each processor
        4. Stores results for report generation

        Args:
            process_group_id: The NiFi process group ID to analyze

        Raises:
            Exception: If unable to fetch processors or provenance data
        """
        self.process_group_id = process_group_id
        self.end_date = datetime.now()
        self.start_date = self.end_date - timedelta(days=self.days_back)

        # Phase 1: Display analysis parameters
        self.console.print(f"\n[yellow]Analyzing processor usage:[/yellow]")
        self.console.print(f"  Process Group: {process_group_id[:16]}...")
        self.console.print(
            f"  Date Range: {self.start_date.strftime('%Y-%m-%d')} to "
            f"{self.end_date.strftime('%Y-%m-%d')}"
        )

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

        # Phase 1.5: Get processor execution counts (fast, single API call)
        self.console.print(
            f"\n[yellow]Phase 1.5:[/yellow] Fetching execution statistics..."
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

        # Phase 2: Query provenance per processor (optional, based on execution_only flag)
        self.processor_event_counts = {}

        if not self.execution_only:
            self.console.print(
                f"\n[yellow]Phase 2:[/yellow] Querying provenance (past {self.days_back} day(s))..."
            )

            with Progress(
                SpinnerColumn(),
                TextColumn("[progress.description]{task.description}"),
                console=self.console
            ) as progress:
                task = progress.add_task(
                    f"Fetching events for {len(self.target_processors)} processors...",
                    total=len(self.target_processors)
                )

                for index, proc in enumerate(self.target_processors, 1):
                    processor_id = proc['id']
                    proc_name = proc['component']['name']
                    proc_type = proc['component']['type'].split('.')[-1]

                    # Update progress to show current processor
                    progress.update(
                        task,
                        description=f"Fetching: {proc_name} ({index}/{len(self.target_processors)})"
                    )

                    try:
                        # Query with date range
                        events = self.client.query_provenance(
                            processor_id=processor_id,
                            start_date=self.start_date,
                            end_date=self.end_date,
                            max_events=self.max_events_per_processor
                        )

                        self.processor_event_counts[proc_name] = {
                            'id': processor_id,
                            'flowfiles_count': len(events),
                            'invocations': self.processor_invocation_counts.get(processor_id, 0),
                            'type': proc_type
                        }

                        progress.advance(task)

                    except Exception as e:
                        self.console.print(f"[yellow]WARNING[/yellow]  Failed for {proc_name}: {e}")
                        self.processor_event_counts[proc_name] = {
                            'id': processor_id,
                            'flowfiles_count': 0,
                            'invocations': self.processor_invocation_counts.get(processor_id, 0),
                            'type': proc_type
                        }
                        progress.advance(task)

            self.console.print(
                f"[green]OK[/green] Found provenance for {len(self.processor_event_counts)} processors"
            )
        else:
            # Execution-only mode: skip provenance, just use execution counts
            self.console.print(
                f"\n[yellow]Phase 2:[/yellow] Skipping provenance (execution-only mode)"
            )

            for proc in self.target_processors:
                proc_id = proc['id']
                proc_name = proc['component']['name']
                proc_type = proc['component']['type'].split('.')[-1]

                self.processor_event_counts[proc_name] = {
                    'id': proc_id,
                    'invocations': self.processor_invocation_counts.get(proc_id, 0),
                    'flowfiles_count': None,
                    'type': proc_type
                }

            self.console.print(
                f"[green]OK[/green] Skipped provenance, using execution counts only"
            )

    def generate_report(self, output_prefix: Optional[str] = None) -> None:
        """
        Generate CSV and visualization reports.

        Creates:
        - CSV file with detailed processor usage metrics
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
            if self.execution_only:
                writer.writerow(['Processor Name', 'Processor Type', 'Execution Count (Total)'])
            else:
                writer.writerow(['Processor Name', 'Processor Type', 'Execution Count (Total)', f'FlowFiles Processed ({self.days_back}d)'])

            for name, data in sorted_processors:
                if self.execution_only:
                    writer.writerow([name, data['type'], data['invocations']])
                else:
                    writer.writerow([name, data['type'], data['invocations'], data['flowfiles_count']])

        self.console.print(f"[green]OK[/green] Saved CSV: {csv_file}")

        # 2. Generate bar chart
        fig, ax = plt.subplots(figsize=(14 if not self.execution_only else 12, max(8, len(self.target_processors) * 0.4)))

        names = [name for name, _ in sorted_processors]
        invocations = [data['invocations'] for _, data in sorted_processors]

        if self.execution_only:
            # Single bar chart (execution count only)
            colors = ['red' if i == 0 else 'orange' if i < 10 else 'steelblue' for i in invocations]
            ax.barh(names, invocations, color=colors)
            ax.set_xlabel('Execution Count (Total)', fontsize=12)
            ax.set_title(
                f'Processor Execution Count\n'
                f'Process Group: {self.process_group_id[:16] if self.process_group_id else "unknown"}...',
                fontsize=14,
                fontweight='bold'
            )
        else:
            # Grouped bar chart (execution count + FlowFiles)
            flowfiles = [data['flowfiles_count'] for _, data in sorted_processors]
            x = np.arange(len(names))
            width = 0.35

            bars1 = ax.barh(x - width/2, invocations, width, label='Execution Count (Total)', color='steelblue')
            bars2 = ax.barh(x + width/2, flowfiles, width, label=f'FlowFiles ({self.days_back}d)', color='lightcoral')

            ax.set_yticks(x)
            ax.set_yticklabels(names)
            ax.set_xlabel('Count', fontsize=12)
            ax.set_title(
                f'Processor Usage: Executions vs FlowFiles\n'
                f'Process Group: {self.process_group_id[:16] if self.process_group_id else "unknown"}...',
                fontsize=14,
                fontweight='bold'
            )
            ax.legend()

        ax.set_ylabel('Processor Name', fontsize=12)
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

        self.console.print(f"\n[cyan]Summary:[/cyan]")
        self.console.print(f"  Total processors: {len(self.target_processors)}")
        self.console.print(f"  Total executions (all time): {total_invocations:,}")

        if not self.execution_only:
            total_flowfiles = sum(data['flowfiles_count'] for _, data in sorted_processors)
            self.console.print(f"  Total FlowFiles (past {self.days_back} day(s)): {total_flowfiles:,}")

        self.console.print(f"  Never executed: {unused_count} processors")

        # Show pruning candidates
        if unused_count > 0:
            self.console.print(
                f"\n[yellow]WARNING: Processors with 0 executions (candidates for pruning):[/yellow]"
            )
            for name, data in sorted_processors:
                if data['invocations'] == 0:
                    self.console.print(f"  • {name} ({data['type']})")

        # Show low usage processors (only in full mode)
        if not self.execution_only:
            low_usage_count = sum(1 for _, data in sorted_processors if 0 < data['invocations'] < 10)
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
