# NiFi Processor Usage Analyzer

Analyze NiFi processor execution frequency to identify unused or underutilized processors for pruning decisions.

## Overview

This lightweight tool connects to your Apache NiFi instance and analyzes processor execution patterns over a configurable time period (default: 30 days). It generates:

- **Bar Chart**: Visual distribution of processor usage (color-coded)
- **CSV Report**: Detailed metrics for each processor
- **Pruning Candidates**: List of unused or rarely-used processors

## Quick Start

### 1. Install

```bash
# Clone or download this repository
cd nifi-processor-analyzer

# Install dependencies
pip install -r requirements.txt

# Or install in development mode
pip install -e .
```

### 2. Configure

Copy the example config file and edit with your NiFi details:

```bash
cp config.example.yaml config.yaml
# Edit config.yaml with your NiFi URL, credentials, and process group ID
```

Example `config.yaml`:

```yaml
nifi_url: https://nifi.company.com:8443/nifi
username: admin
password: your-password
process_group_id: abc-123-your-group-id
days_back: 30
verify_ssl: false
```

### 3. Run

```bash
python analyze.py
```

## Usage

### Using Config File (Recommended)

Create `config.yaml` with your settings and run:

```bash
python analyze.py
```

### Using Command-Line Arguments

```bash
python analyze.py \
  --url https://nifi.company.com:8443/nifi \
  --username admin \
  --password password123 \
  --group-id abc-123-process-group-id \
  --days 30
```

### Overriding Config File Values

```bash
# Use config.yaml but analyze 60 days instead of 30
python analyze.py --days 60

# Use config.yaml but target different process group
python analyze.py --group-id xyz-789-different-group
```

### All Options

```
--url              NiFi URL (e.g., https://nifi:8443/nifi)
--username         NiFi username
--password         NiFi password
--group-id         Process group ID to analyze
--days             Number of days to look back (default: 30)
--max-events       Maximum events per processor (default: 10000)
--no-verify-ssl    Disable SSL certificate verification
--config           Path to config file (default: config.yaml)
--output-prefix    Output file prefix (default: processor_usage_[GROUP_ID])
--verbose          Enable verbose logging
```

## Output

The analyzer generates two files:

### 1. Bar Chart (`processor_usage_[GROUP_ID].png`)

Horizontal bar chart showing execution frequency for each processor:
- **Green bars**: Active processors (≥10 events)
- **Orange bars**: Low usage processors (1-9 events)
- **Red bars**: Unused processors (0 events)

### 2. CSV Report (`processor_usage_[GROUP_ID].csv`)

Detailed metrics in CSV format:

```csv
Processor Name,Processor Type,Event Count,Events per Day
LogMessage,LogMessage,1250,41.7
UpdateAttribute,UpdateAttribute,890,29.7
RouteOnAttribute,RouteOnAttribute,0,0.0
```

## Finding Your Process Group ID

1. Open NiFi UI
2. Navigate to the process group you want to analyze
3. Right-click on the process group
4. Select "Copy ID"
5. Paste the ID into your config file or command-line argument

## Understanding the Results

### Event Count = Execution Frequency

Each provenance event represents one execution of a processor. Higher event counts indicate:
- More data flowing through the processor
- More frequent execution
- Higher importance in the workflow

### Pruning Candidates

Processors with **0 events** over the analysis period are strong candidates for removal:
- They haven't processed any data in the past N days
- They may be:
  - Disabled/stopped processors
  - Processors in test/development flows
  - Legacy processors from old implementations
  - Misconfigured processors that never receive data

Processors with **low event counts** (<10) may also be candidates:
- Rarely-used edge case handlers
- Debugging processors left in production
- Processors with incorrect routing logic

### Best Practices

1. **Start with 30 days**: This gives a good baseline for normal operations
2. **Run during peak season**: Avoid analyzing during holidays or downtime
3. **Cross-check with team**: Some processors may be intentionally idle (disaster recovery, etc.)
4. **Use multiple time periods**: Compare 7-day, 30-day, and 90-day analyses
5. **Review before pruning**: Always verify with your team before removing processors

## Dependencies

Only 3 minimal dependencies:
- `requests` - NiFi REST API client
- `matplotlib` - Visualization
- `rich` - Console formatting

No heavy libraries like nipyapi, lxml, networkx, etc.

## Architecture

```
nifi-processor-analyzer/
├── analyzer/
│   ├── __init__.py         # Package initialization
│   ├── nifi_client.py      # Minimal NiFi REST API client
│   └── usage_analyzer.py   # Analysis logic
├── analyze.py              # CLI entry point
├── config.example.yaml     # Configuration template
├── config.yaml             # Your configuration (git-ignored)
├── pyproject.toml          # Package metadata
├── requirements.txt        # Dependencies
└── README.md               # This file
```

## Comparison with Full nifi2py

This is a **focused, standalone tool** extracted from the larger `nifi2py` project:

| Feature | nifi-processor-analyzer | nifi2py |
|---------|------------------------|---------|
| Lines of code | ~400 | ~5000+ |
| Dependencies | 3 packages | 10+ packages |
| Purpose | Processor usage analysis | Full NiFi → Python conversion |
| Install time | <5 seconds | ~30 seconds |
| Complexity | Simple | Complex |

Use this tool when you only need processor usage analysis. Use the full `nifi2py` project when you need complete dataflow conversion capabilities.

## Troubleshooting

### Authentication Errors

```
Error: Authentication failed
```

**Solution**: Check your username and password in config.yaml

### Process Group Not Found

```
Error: Resource not found
```

**Solution**: Verify your process group ID. Use NiFi UI → Right-click → Copy ID

### SSL Certificate Errors

```
Error: SSL verification failed
```

**Solution**: Add `verify_ssl: false` to config.yaml or use `--no-verify-ssl`

### No Events Found

```
Warning: All processors have 0 events
```

**Possible causes**:
- Process group has been idle during the analysis period
- Provenance repository retention is shorter than your analysis period
- Wrong process group ID

**Solution**:
- Check that processors in the group are running
- Reduce `days_back` to a shorter period
- Verify the process group ID

## Contributing

This is a standalone tool extracted from the nifi2py project. For contributions:

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Submit a pull request

## License

MIT License - See LICENSE file for details

## Support

For issues or questions:
- Open an issue on GitHub
- Check the troubleshooting section above
- Review the `config.example.yaml` for configuration help
