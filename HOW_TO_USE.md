# How to Use NiFi Processor Usage Analyzer

Complete step-by-step guide for analyzing processor usage in your NiFi flows.

## Table of Contents

- [What This Tool Does](#what-this-tool-does)
- [Prerequisites](#prerequisites)
- [Installation](#installation)
- [Configuration](#configuration)
- [Running the Analysis](#running-the-analysis)
- [Understanding the Results](#understanding-the-results)
- [Common Usage Scenarios](#common-usage-scenarios)
- [Troubleshooting](#troubleshooting)
- [FAQ](#faq)

---

## What This Tool Does

This tool connects to your Apache NiFi instance and analyzes processor execution counts (total invocations since creation). It helps you:

- **Identify unused processors** (0 executions since creation) - candidates for removal
- **Find low-usage processors** (<10 total executions) - may need review
- **Visualize processor activity** - color-coded bar chart
- **Generate reports** - CSV file for detailed analysis
- **Fast analysis** - ~5-10 seconds using NiFi Status API

---

## Prerequisites

Before you start, you need:

1. **Python 3.8 or newer** installed on your computer
   ```bash
   # Check your Python version
   python3 --version
   ```

2. **Access to NiFi** with:
   - NiFi URL (e.g., `https://nifi.company.com:8443/nifi`)
   - Username and password
   - Read access to the process groups you want to analyze

3. **Process Group ID** from NiFi UI:
   - In NiFi, navigate to the process group you want to analyze
   - Right-click on the process group
   - Click "Copy ID"
   - Save this ID - you'll need it for configuration

---

## Installation

### Step 1: Get the Tool

If you received a zip/tar file:
```bash
# Extract the archive
tar -xzf nifi-processor-analyzer.tar.gz
cd nifi-processor-analyzer
```

If you cloned from git:
```bash
git clone <repository-url>
cd nifi-processor-analyzer
```

If you're already in the directory:
```bash
cd /home/eric/Projects/nifi-processor-analyzer
```

### Step 2: Install Dependencies

Install the required Python packages (only 3 packages):

```bash
pip3 install -r requirements.txt
```

This installs:
- `requests` - To connect to NiFi REST API
- `matplotlib` - To create visualizations
- `rich` - To make console output pretty

**That's it!** Installation is complete.

---

## Configuration

### Step 1: Create Your Config File

```bash
# Copy the example config
cp config.example.yaml config.yaml
```

### Step 2: Edit the Config File

Open `config.yaml` in your favorite text editor:

```bash
nano config.yaml
# or
vim config.yaml
# or
code config.yaml
```

### Step 3: Fill in Your Details

Replace the placeholder values with your actual NiFi connection details:

```yaml
# NiFi connection details
nifi_url: https://nifi.company.com:8443/nifi    # ← Your NiFi URL
username: admin                                  # ← Your NiFi username
password: your-password-here                     # ← Your NiFi password

# Process group to analyze
process_group_id: abc-123-your-process-group-id  # ← Paste the ID you copied from NiFi

# SSL verification
verify_ssl: false          # Set to true if you have valid SSL certificates
```

### Step 4: Save and Close

Save the file. **Important:** Don't commit `config.yaml` to git - it contains your password!

---

## Running the Analysis

### Basic Usage

Run the analysis with your config file:

```bash
python3 analyze.py
```

That's it! The tool will:
1. Connect to NiFi
2. Get all processors in your target group
3. Query execution counts from Status API
4. Generate a chart and CSV report

### What You'll See

```
Configuration:
  NiFi URL: https://nifi.company.com:8443/nifi
  Username: admin
  Process Group ID: abc-123...
  Verify SSL: False

Connecting to NiFi...
OK Connected successfully

Analyzing processor execution counts:
  Process Group: abc-123...

Phase 1: Getting processors from target process group...
OK Found 25 processors

Phase 2: Fetching execution statistics...
OK Retrieved execution counts for 25 processors

Phase 3: Generating reports...
OK Saved CSV: processor_usage_abc-123.csv
OK Saved plot: processor_usage_abc-123.png

Summary:
  Total processors: 25
  Total executions (all time): 15,234
  Never executed: 3 processors
  Low usage (<10 executions): 5 processors

WARNING: Processors with 0 executions (candidates for pruning):
  • OldProcessor (UpdateAttribute)
  • TestProcessor (LogMessage)

OK Analysis complete!
```

---

## Understanding the Results

The tool generates two files:

### 1. Bar Chart (processor_usage_[ID].png)

A horizontal bar chart showing execution counts:

**Color coding:**
- 🔵 **Blue bars** = Active processors (≥10 executions) - Keep these
- 🟠 **Orange bars** = Low usage (1-9 executions) - Review these
- 🔴 **Red bars** = Unused (0 executions) - **Pruning candidates!**

**How to read it:**
- Longer bars = more executions (all-time) = more important
- Shorter bars = fewer executions = less important
- Red bars = never executed = probably can be removed

### 2. CSV Report (processor_usage_[ID].csv)

Spreadsheet with execution count metrics:

```csv
Processor Name,Processor Type,Execution Count (Total)
LogMessage,LogMessage,1250
UpdateAttribute,UpdateAttribute,890
RouteOnAttribute,RouteOnAttribute,150
OldProcessor,UpdateAttribute,0
```

**Columns:**
- **Processor Name**: The processor's name in NiFi
- **Processor Type**: The type (LogMessage, UpdateAttribute, etc.)
- **Execution Count (Total)**: Total executions since processor was created

**How to use it:**
- Sort by "Execution Count (Total)" to find unused processors
- Open in Excel/Google Sheets for analysis
- Share with your team for discussion

### What to Do with the Results

**Processors with 0 events (red bars):**
- Strong candidates for removal
- Verify with team before deleting
- May be:
  - Disabled/stopped processors
  - Test/development code
  - Legacy from old implementations
  - Misconfigured (never receive data)

**Processors with low events (orange bars):**
- Review with team
- May be:
  - Edge case handlers (intentionally rare)
  - Debugging processors left in production
  - Incorrectly routed (not getting data)

**Processors with high events (green bars):**
- Core processors - keep these!
- Critical to your data flow

---

## Common Usage Scenarios

### Scenario 1: Batch Mode - Analyze Multiple Flows at Once

**Why:** Analyze many NiFi flows in one command, perfect for weekly monitoring or large-scale analysis

**Step 1: Create flows CSV**

Create a CSV file with your flow definitions (e.g., `flows.csv`):

```csv
id,flow_name
8c8677c4-29d6-3607-a32e-1234567890ab,Production_Data_Pipeline
abc-123-def-456-7890-abcdef123456,Development_Testing_Flow
xyz-789-ghi-012-3456-7890abcdef12,QA_Validation_Flow
```

**Step 2: Run batch analysis**

```bash
python3 analyze.py --flows-csv flows.csv
```

**What you'll get:**

**Per-flow charts:**
- `processor_usage_Production_Data_Pipeline.png`
- `processor_usage_Development_Testing_Flow.png`
- `processor_usage_QA_Validation_Flow.png`

**Combined CSV (Databricks-ready):**
- `processor_usage_all_flows_20260108_143022.csv`

**Combined CSV Schema:**
```
snapshot_timestamp,flow_name,process_group_id,processor_id,processor_name,processor_type,invocations
2026-01-08 14:30:22,Production_Data_Pipeline,8c8677c4...,proc-id-1,LogMessage,LogMessage,1250
2026-01-08 14:30:22,Production_Data_Pipeline,8c8677c4...,proc-id-2,UpdateAttribute,UpdateAttribute,890
...
```

**Why this is useful:**
- ✅ Analyze 10+ flows in one run
- ✅ Combined CSV includes flow_name and timestamp
- ✅ Upload to Databricks for time-series analysis
- ✅ Track processor activity trends over weeks/months
- ✅ Compare activity across different flows

**Example workflow (VDI + Databricks):**

1. **Monday morning on VDI** (where NiFi is accessible):
   ```bash
   python3 analyze.py --flows-csv flows.csv
   ```

2. **Upload to Databricks:**
   ```bash
   databricks fs cp processor_usage_all_flows_20260108_143022.csv dbfs:/nifi_analysis/
   ```

3. **Load in Databricks notebook:**
   ```python
   df = spark.read.csv("/dbfs/nifi_analysis/processor_usage_all_flows_20260108_143022.csv",
                       header=True, inferSchema=True)
   df.write.format("delta").mode("append").saveAsTable("main.default.nifi_processor_snapshots")
   ```

4. **Analyze trends over time (SQL):**
   ```sql
   -- Find processors inactive in last 7 days
   WITH recent_activity AS (
     SELECT flow_name, processor_name,
            MAX(invocations) - MIN(invocations) as delta
     FROM main.default.nifi_processor_snapshots
     WHERE snapshot_timestamp >= current_date() - 7
     GROUP BY flow_name, processor_name
   )
   SELECT * FROM recent_activity WHERE delta = 0;
   ```

### Scenario 2: Analyze Multiple Process Groups (One at a Time)

**Why:** Check execution counts across different parts of your NiFi flow

```bash
# Production flow
python3 analyze.py --group-id abc-123-production

# Development flow
python3 analyze.py --group-id xyz-789-development

# Legacy flow
python3 analyze.py --group-id old-456-legacy
```

### Scenario 3: Override Config Settings

**Why:** Quickly analyze different process groups without editing config.yaml

```bash
# Use config.yaml but different process group
python3 analyze.py --group-id xyz-different-group
```

### Scenario 4: One-Time Analysis Without Config File

**Why:** Quick analysis without creating config.yaml

```bash
python3 analyze.py \
  --url https://nifi.company.com:8443/nifi \
  --username admin \
  --password mypassword \
  --group-id abc-123-group-id
```

### Scenario 5: Custom Output Names

**Why:** Save multiple analyses with descriptive names

```bash
# Production analysis
python3 analyze.py --output-prefix production_analysis

# Creates: production_analysis.png and production_analysis.csv
```

### Scenario 6: Debug Mode

**Why:** See detailed logs if something goes wrong

```bash
python3 analyze.py --verbose
```

---

## Troubleshooting

### Problem: "python: command not found"

**Solution:** Use `python3` instead of `python`:
```bash
python3 analyze.py
```

### Problem: "Authentication failed"

**Cause:** Wrong username or password

**Solution:**
1. Check your `config.yaml` - is username/password correct?
2. Try logging into NiFi UI with same credentials
3. Check if password has special characters (may need quotes in YAML)

### Problem: "Resource not found" or "Process group not found"

**Cause:** Wrong process group ID

**Solution:**
1. In NiFi UI, right-click the process group
2. Click "Copy ID"
3. Paste into `config.yaml` as `process_group_id`
4. Make sure you copied the entire ID (it's long!)

### Problem: "SSL certificate verification failed"

**Cause:** NiFi using self-signed SSL certificate

**Solution:** Set `verify_ssl: false` in config.yaml or use:
```bash
python3 analyze.py --no-verify-ssl
```

### Problem: "All processors have 0 events"

**Possible causes:**
1. Processors haven't run during the time period
2. Provenance retention is shorter than your analysis period
3. Wrong process group selected

**Solutions:**
```bash
# Try shorter time period
python3 analyze.py --days 7

# Verify correct group ID
python3 analyze.py --verbose
```

### Problem: "Missing required parameters"

**Cause:** Config file not found or missing values

**Solution:**
1. Make sure `config.yaml` exists in the same directory
2. Check all required fields are filled in:
   - `nifi_url`
   - `username`
   - `password`
   - `process_group_id`

### Problem: "Module not found" errors

**Cause:** Dependencies not installed

**Solution:**
```bash
# Install dependencies
pip3 install -r requirements.txt

# Or install individually
pip3 install requests matplotlib rich
```

---

## FAQ

### Q: How long does the analysis take?

**A:** Depends on number of processors:
- 10 processors: ~30 seconds
- 50 processors: ~2-3 minutes
- 100 processors: ~5 minutes

### Q: Will this affect my running NiFi flow?

**A:** No! The tool only **reads** data via REST API. It doesn't:
- Stop processors
- Delete anything
- Modify configurations
- Affect data flow

### Q: How often should I run this analysis?

**A:** Recommended schedule:
- **Monthly**: Regular maintenance check
- **Quarterly**: Deep review for pruning
- **Before major changes**: Understand current state
- **After migration**: Verify everything migrated correctly

### Q: What if a processor should have 0 events?

**A:** Some processors are intentionally idle:
- Disaster recovery processors
- Error handling (only runs on failures)
- Scheduled processors (run monthly/quarterly)
- Development/testing processors

**Action:** Review with your team - not all 0-event processors should be removed!

### Q: Can I analyze the root process group?

**A:** Yes! Just use "root" as the process group ID:
```yaml
process_group_id: root
```

### Q: Can I schedule this to run automatically?

**A:** Yes! Use cron (Linux/Mac):

```bash
# Run every Monday at 9 AM
0 9 * * 1 cd /path/to/nifi-processor-analyzer && python3 analyze.py
```

Or Windows Task Scheduler.

### Q: How do I share results with my team?

**A:**
1. Email the PNG chart (easy to visualize)
2. Share the CSV in Excel/Google Sheets (detailed analysis)
3. Add to documentation/wiki
4. Present in team meetings

### Q: What's a good threshold for pruning?

**A:** Recommended approach:
- **0 events in 30 days** = Strong candidate for removal
- **<10 events in 30 days** = Review with team
- **<1 event per day** = Possibly underutilized

Always verify with team before deleting!

### Q: Can I analyze processors added/removed during the time period?

**A:** Yes! The tool analyzes:
- Currently active processors
- If they existed during the time period, events will show
- If added recently, they may show low/zero events (this is expected)

---

## Quick Reference

### Most Common Commands

```bash
# Basic usage (with config.yaml)
python3 analyze.py

# Different process group
python3 analyze.py --group-id xyz-789

# Debug mode
python3 analyze.py --verbose

# See all options
python3 analyze.py --help
```

### File Outputs

| File | Description |
|------|-------------|
| `processor_usage_[ID].png` | Color-coded bar chart |
| `processor_usage_[ID].csv` | Detailed metrics spreadsheet |

### Important Files

| File | Purpose | Git Tracked? |
|------|---------|--------------|
| `config.yaml` | Your configuration | ❌ No (has password!) |
| `config.example.yaml` | Template | ✅ Yes |
| `analyze.py` | Main script | ✅ Yes |
| `requirements.txt` | Dependencies | ✅ Yes |

---

## Getting Help

1. **Check this guide** - Most common issues are covered above
2. **Run with `--verbose`** - See detailed error messages
3. **Check the README.md** - Additional documentation
4. **Ask your team** - Someone may have seen the issue before

---

## Tips for Success

1. ✅ **Fast results** - Analysis completes in ~5-10 seconds
2. ✅ **Review with team** - Don't prune alone!
3. ✅ **Understand limitations** - Execution count is cumulative (all-time), not time-specific
4. ✅ **Document decisions** - Keep notes on why processors were removed
5. ✅ **Backup before pruning** - Export flow template before deleting
6. ✅ **Test after pruning** - Verify flow still works correctly
7. ✅ **Consider context** - 0 executions might be intentional (DR, error handling)

---

**Ready to start?** Go back to [Installation](#installation) and follow the steps!
