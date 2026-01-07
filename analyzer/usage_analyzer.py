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
from rich.console import Console
from rich.progress import Progress, SpinnerColumn, TextColumn

from .nifi_client import NiFiClient


class ProcessorUsageAnalyzer:
    """
    Analyzes processor execution frequency based on provenance events.

    This class queries NiFi provenance data to determine how often each processor
    in a process group has executed over a specified time period. Results are
    used to identify candidates for pruning (unused or rarely-used processors).

    Args:
        client: Initialized NiFiClient instance
        days_back: Number of days to look back for provenance events (default: 30)
        max_events_per_processor: Maximum events to fetch per processor (default: 10000)
    """

    def __init__(
        self,
        client: NiFiClient,
        days_back: int = 30,
        max_events_per_processor: int = 10000
    ):
        self.client = client
        self.days_back = days_back
        self.max_events_per_processor = max_events_per_processor
        self.console = Console()

        # Analysis results (populated by analyze())
        self.process_group_id: Optional[str] = None
        self.start_date: Optional[datetime] = None
        self.end_date: Optional[datetime] = None
        self.processor_event_counts: Dict[str, Dict] = {}
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
            self.console.print(f"[green]✓[/green] Found {len(self.target_processors)} processors")

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
            self.console.print(f"[red]✗[/red] Failed to get processors: {e}")
            raise

        # Phase 3: Query provenance per processor
        self.console.print(
            f"\n[yellow]Phase 2:[/yellow] Querying provenance (past {self.days_back} days)..."
        )

        self.processor_event_counts = {}

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
                        'count': len(events),
                        'type': proc_type
                    }

                    progress.advance(task)

                except Exception as e:
                    self.console.print(f"[yellow]⚠[/yellow]  Failed for {proc_name}: {e}")
                    self.processor_event_counts[proc_name] = {
                        'id': processor_id,
                        'count': 0,
                        'type': proc_type
                    }
                    progress.advance(task)

        self.console.print(
            f"[green]✓[/green] Found provenance for {len(self.processor_event_counts)} processors"
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
            key=lambda x: x[1]['count'],
            reverse=True
        )

        # 1. Save to CSV
        csv_file = Path(f"{output_prefix}.csv")
        with open(csv_file, 'w', newline='') as f:
            writer = csv.writer(f)
            writer.writerow(['Processor Name', 'Processor Type', 'Event Count', 'Events per Day'])
            for name, data in sorted_processors:
                events_per_day = data['count'] / self.days_back
                writer.writerow([name, data['type'], data['count'], f"{events_per_day:.1f}"])

        self.console.print(f"[green]✓[/green] Saved CSV: {csv_file}")

        # 2. Generate bar chart
        fig, ax = plt.subplots(figsize=(12, max(6, len(self.target_processors) * 0.3)))

        names = [name for name, _ in sorted_processors]
        counts = [data['count'] for _, data in sorted_processors]

        # Color bars: red for 0 events (unused), orange for low usage (<10), green for active
        colors = ['red' if c == 0 else 'orange' if c < 10 else 'green' for c in counts]

        ax.barh(names, counts, color=colors)
        ax.set_xlabel('Number of Provenance Events', fontsize=12)
        ax.set_ylabel('Processor Name', fontsize=12)
        ax.set_title(
            f'Processor Execution Frequency - Past {self.days_back} Days\n'
            f'Process Group: {self.process_group_id[:16] if self.process_group_id else "unknown"}...',
            fontsize=14,
            fontweight='bold'
        )
        ax.grid(axis='x', alpha=0.3, linestyle='--')

        plt.tight_layout()

        plot_file = Path(f"{output_prefix}.png")
        plt.savefig(plot_file, dpi=150, bbox_inches='tight')
        self.console.print(f"[green]✓[/green] Saved plot: {plot_file}")

        # Close the figure to free memory
        plt.close(fig)

        # 3. Print summary
        total_events = sum(data['count'] for _, data in sorted_processors)
        unused_count = sum(1 for _, data in sorted_processors if data['count'] == 0)
        low_usage_count = sum(1 for _, data in sorted_processors if 0 < data['count'] < 10)

        self.console.print(f"\n[cyan]Summary:[/cyan]")
        self.console.print(f"  Total processors: {len(self.target_processors)}")
        self.console.print(f"  Total events: {total_events:,}")
        self.console.print(f"  Date range: {self.days_back} days")
        self.console.print(f"  Unused processors (0 events): {unused_count}")
        self.console.print(f"  Low usage processors (<10 events): {low_usage_count}")

        # Show pruning candidates
        if unused_count > 0:
            self.console.print(
                f"\n[yellow]⚠ Processors with 0 events (candidates for pruning):[/yellow]"
            )
            for name, data in sorted_processors:
                if data['count'] == 0:
                    self.console.print(f"  • {name} ({data['type']})")

        if low_usage_count > 0:
            self.console.print(
                f"\n[orange]⚠ Processors with low usage (<10 events):[/orange]"
            )
            for name, data in sorted_processors:
                if 0 < data['count'] < 10:
                    self.console.print(f"  • {name} ({data['type']}): {data['count']} events")

        self.console.print(f"\n[green]✓[/green] Analysis complete!")
        self.console.print(f"\n[cyan]Next steps:[/cyan]")
        self.console.print(f"  1. Review the bar chart: {plot_file}")
        self.console.print(f"  2. Review the CSV: {csv_file}")
        self.console.print(f"  3. Consider pruning unused processors")
