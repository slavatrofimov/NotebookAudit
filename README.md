# Fabric Notebook Auditor

A Fabric Spark notebook that scans Microsoft Fabric Notebook definitions for potential data exfiltration patterns. It reads notebook files from a Lakehouse Files area, distributes analysis across Spark executors, and writes findings to a Delta table for downstream reporting and review.

**No external dependencies are required.** All detection logic uses only the Python standard library (`re`, `ast`, `json`, `math`, `base64`).

---

## Files

| File | Description |
|------|-------------|
| `fabric_exfil_scanner_notebook.ipynb` | Fabric Spark notebook. Reads from a Lakehouse Files area and writes findings to a Delta table. |
---

## Detection Capabilities

### 1. AST Analysis
Parses Python source code into an abstract syntax tree to detect:
- Imports of suspicious modules (networking, cloud SDKs, file transfer libraries)
- `eval()` / `exec()` / `compile()` calls that execute dynamically constructed code

### 2. Regex Patterns (100+)
Pattern-based detection covering:

| Category | Examples |
|----------|---------|
| `http_exfiltration` | `requests.post()`, raw IP HTTP calls, ngrok/burp/webhook.site URLs |
| `cloud_storage_write` | Dropbox, Box, Google Drive, MEGA, WeTransfer, pCloud, Firebase, Supabase, Backblaze B2, Cloudinary |
| `webhook_exfiltration` | Slack webhooks, Discord webhooks, Telegram Bot API, AWS API Gateway |
| `api_exfiltration` | Microsoft Graph API, Fabric REST API, Power BI Push API, Airtable, Notion |
| `credential_access` | Hardcoded storage account keys, SAS tokens, SQL passwords, client secrets, Bearer JWTs, OpenAI API keys, connection strings |
| `email_exfiltration` | `smtplib`, SendGrid, Mailgun, Mandrill, SparkPost |
| `dns_exfiltration` | `dns.resolver`, `dnspython`, `socket.getaddrinfo` |
| `filesystem_exfiltration` | `mssparkutils.fs.put/mv`, `dbutils.fs.cp` to cloud paths, Pandas/Polars file exports |
| `spark_external_write` | Spark with MongoDB/Cassandra/Kafka connectors, `saveAsTable` with 3-part catalog names |
| `spark_streaming_exfiltration` | Spark Structured Streaming to Kafka, Event Hubs, MQTT, custom `foreachBatch` sinks |
| `external_database_write` | JDBC connection strings, Azure SQL server names, SQL DML via `.execute()` |
| `shell_execution` | `os.system()`, `subprocess`, `!curl`, `!wget`, `!nc` |
| `dynamic_code_execution` | `exec(base64...)`, `eval(codecs...)`, `getattr(__builtins__, ...)` |
| `in_notebook_package_install` | `!pip install`, `%pip install`, installs from non-PyPI indexes |
| `paste_service` | Pastebin, GitHub Gists, GitLab Snippets, ix.io, Hastebin |

### 3. URL / URI Detection
- Matches 30+ protocol schemes: `http`, `https`, `ftp`, `abfs`, `wasbs`, `s3`, `gs`, `hdfs`, `dbfs`, `jdbc`, `mongodb`, `kafka`, `mqtt`, `ssh`, `smb`, and more
- Detects bare `www.` domain references
- **Direction classification**: Inspects surrounding code context to classify each URL as `read`, `write`, `read_write`, or `unknown`
- Dynamically constructed URLs: f-strings, string concatenation, `.format()`, `.join()`, `+=`, `urljoin()`
- Filters benign URLs: localhost, PyPI, docs sites, Apache docs, schema registries

### 4. Entropy Analysis
- Flags string literals with Shannon entropy > 4.5 and length > 40 characters
- Helps detect obfuscated payloads, base64-encoded data, or embedded keys

---

## Fabric Spark Notebook

### Overview

`fabric_exfil_scanner_notebook.ipynb` runs entirely within a Microsoft Fabric Spark session. It scans notebook files stored in a Lakehouse Files area, distributes work across Spark executors using `mapPartitions`, and writes findings to a Delta table.

**No external dependencies are required.** All detection logic is inlined using Python standard library (`re`, `ast`, `json`, `math`, `base64`).

### Prerequisites

- A Microsoft Fabric workspace with a Spark capacity
- A Lakehouse attached to the notebook as the default Lakehouse
- Notebook files to scan placed under the Lakehouse's **Files** area (see [Preparing the Source Files](#preparing-the-source-files))

### Architecture

```
Lakehouse Files (notebooks/)
        │
        ▼
spark.read.format("binaryFile")          ← Cell 3: Load all files as binary
        │
        ▼
file_df.repartition(n)                   ← Cell 3: Distribute across executors
        │
        ▼
rdd.mapPartitions(scan_partition)        ← Cell 10: Detect patterns in parallel
        │
        ▼
findings_df.saveAsTable(OUTPUT_TABLE)    ← Cell 11: Persist to Delta table
        │
        ▼
Summary view + SQL query examples        ← Cells 13–15
```

### Preparing the Source Files

Export notebooks from Fabric and place them in the Lakehouse Files area. Common methods:

- **Fabric Git Integration**: Connect your workspace to a Git repository; notebook definitions are exported as `.ipynb` files.
- **fabric-cli / Power BI REST API**: Download item definitions via the Fabric Items API.
- **Manual export**: Download individual notebooks from the Fabric UI (`File → Export`).

Then copy the files into your Lakehouse:
```python
# Example using mssparkutils
mssparkutils.fs.cp("abfss://<source>@<account>.dfs.core.windows.net/notebooks/", 
                   "Files/notebooks/", recurse=True)
```

### Configuration

Edit the **Configuration** cell (Cell 2) before running:

```python
# Path under the attached Lakehouse's Files area to scan
SOURCE_PATH = "Files/notebooks"

# File glob patterns. Accepts a single string or a list of patterns.
SOURCE_FILE_GLOB = ["*.ipynb", "*.py"]   # scan both notebook types
# SOURCE_FILE_GLOB = "*.ipynb"           # single pattern also works
# SOURCE_FILE_GLOB = ["*.ipynb", "*.py", "*.json"]  # add Fabric Item JSON

# Delta table name to write findings into
OUTPUT_TABLE = "notebook_exfil_findings"

# Minimum severity to report: "low", "medium", "high", "critical"
MIN_SEVERITY = "medium"

# Limit for testing (set to an integer, e.g. 10, or None for all files)
MAX_FILES = None

# Files per Spark partition (tune based on cluster size)
TARGET_PARTITION_SIZE = 100
```

### Running the Notebook

1. Open `fabric_exfil_scanner_notebook.ipynb` in your Fabric workspace.
2. Attach a Lakehouse as the default Lakehouse.
3. Update the **Configuration** cell with your paths and settings.
4. Click **Run All**.

The notebook will print progress, write results to the Delta table, and display a summary:

```
Found 342 files. Scanning in 4 partition(s).
Scan complete. 47 findings detected across 342 files.
Results written to table: notebook_exfil_findings
```

### Output Table Schema

Findings are written to the configured `OUTPUT_TABLE` with the following schema:

| Column | Type | Description |
|--------|------|-------------|
| `file_path` | string | Full Lakehouse path of the scanned file |
| `file_name` | string | File name only |
| `cell_index` | integer | Notebook cell index (0-based) where the finding occurred |
| `line_number` | integer | Line number within the cell |
| `category` | string | Detection category (e.g., `credential_access`, `http_exfiltration`) |
| `severity` | string | `low`, `medium`, `high`, or `critical` |
| `message` | string | Human-readable description of the finding |
| `code_snippet` | string | Up to 200 characters of the matching code |
| `scanned_at` | timestamp | UTC timestamp when the file was scanned |

### Querying Findings

After the scan, query the findings table directly in the notebook or from any Fabric SQL endpoint:

```sql
-- All critical findings
SELECT * FROM notebook_exfil_findings WHERE severity = 'critical';

-- Findings per file, sorted by risk
SELECT file_name, COUNT(*) AS finding_count
FROM notebook_exfil_findings
GROUP BY file_name
ORDER BY finding_count DESC;

-- Credential access findings
SELECT file_name, cell_index, line_number, message, code_snippet
FROM notebook_exfil_findings
WHERE category = 'credential_access';

-- External URL references (write direction only)
SELECT file_name, message, code_snippet
FROM notebook_exfil_findings
WHERE category = 'external_url_reference'
  AND message LIKE '%write%';

-- High-risk cloud storage patterns
SELECT file_name, category, message, code_snippet
FROM notebook_exfil_findings
WHERE category IN ('cloud_storage_write', 'webhook_exfiltration', 'spark_streaming_exfiltration')
ORDER BY severity;
```

### Supported File Formats

The scanner handles three notebook file formats automatically:

| Format | Detection |
|--------|-----------|
| Standard `.ipynb` (Jupyter) | Reads all `code` cells from the `cells` array |
| Fabric Item JSON (`properties.cells`) | Reads code cells from the Fabric item definition |
| Fabric Item JSON (`definition.parts`) | Decodes base64-encoded `.py` or nested `.ipynb` payloads |
| Plain `.py` files | Scans the entire file as a single code block |

### Severity Levels

| Severity | Meaning |
|----------|---------|
| `critical` | Direct exfiltration evidence (known exfil service URL, decoded exec, webhook key) |
| `high` | Strong exfiltration indicator (cloud SDK import, credential access, shell command) |
| `medium` | Potential exfiltration indicator (external URL reference, DataFrame file export) |
| `low` | Low-confidence signal worth manual review |

---

## Notes and Limitations

- **Static analysis only**: The scanner cannot evaluate runtime behavior. Exfiltration via variables resolved at runtime (e.g., `displayHTML(variable)`) may not be detected.
- **False positives**: URL and import detections may flag legitimate patterns. Set `MIN_SEVERITY = "high"` to reduce noise, or review `medium` findings manually.
- **`saveAsTable` scoping**: Unqualified (`tablename`) and two-part (`schema.table`) names are not flagged as external writes — they target the default Lakehouse. Only three-part names (`catalog.db.table`) are flagged.
- **Fabric environment required**: The notebook must run within a Fabric Spark session. It cannot be executed locally.
