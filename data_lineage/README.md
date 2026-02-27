# Data Lineage

This folder contains scripts for working with Amazon DataZone data lineage.

## Available Scripts

### retrieve_lineage_events.py

Retrieves lineage events from Amazon DataZone within a specified time range and saves them to a JSON file.

The script fetches lineage events from a DataZone domain using timestamp filters, retrieves detailed event data for each event, and exports the results to a timestamped JSON file for analysis or archival purposes.

---

## retrieve_lineage_events.py

### Prerequisites

#### Required Permissions

Your IAM identity must have the following permissions:

- `datazone:ListLineageEvents` - List lineage events in the domain
- `datazone:GetLineageEvent` - Retrieve detailed event data

#### Python Dependencies

Install the required Python packages:

```bash
pip3 install boto3
```

#### AWS Credentials

Configure your AWS credentials using one of these methods:

- AWS CLI: `aws configure`
- Environment variables: `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_SESSION_TOKEN`
- IAM role (if running on EC2, Lambda, etc.)

For more details, see the [Boto3 credentials documentation](https://boto3.amazonaws.com/v1/documentation/api/latest/guide/credentials.html).

### Usage

#### Basic Command

```bash
python3 retrieve_lineage_events.py \
  --region us-east-1 \
  --domain-identifier dzd-abc123xyz \
  --timestamp-after 1742404458 \
  --timestamp-before 1742440527
```

#### Command-Line Arguments

| Argument | Required | Description | Default |
|----------|----------|-------------|---------|
| `--region` | Yes | AWS region where the DataZone domain is located | - |
| `--domain-identifier` | Yes | DataZone domain identifier | - |
| `--timestamp-after` | Yes | Start of time range (Unix epoch timestamp) | - |
| `--timestamp-before` | Yes | End of time range (Unix epoch timestamp) | - |
| `--max-results` | No | Maximum number of results per API page | 50 |

#### Examples

##### Retrieve events from the last 24 hours

```bash
# Calculate timestamps (Unix epoch)
TIMESTAMP_BEFORE=$(date +%s)
TIMESTAMP_AFTER=$((TIMESTAMP_BEFORE - 86400))

python3 retrieve_lineage_events.py \
  --region us-east-1 \
  --domain-identifier dzd-abc123xyz \
  --timestamp-after $TIMESTAMP_AFTER \
  --timestamp-before $TIMESTAMP_BEFORE
```

##### Retrieve events for a specific date range

```bash
# January 15, 2025 00:00:00 to January 16, 2025 00:00:00
python3 retrieve_lineage_events.py \
  --region us-west-2 \
  --domain-identifier dzd-xyz789abc \
  --timestamp-after 1736899200 \
  --timestamp-before 1736985600 \
  --max-results 100
```

### Output

#### Console Output

```
Fetching lineage events from domain dzd-abc123xyz
Time range: 1742404458 to 1742440527
Retrieved 42 lineage events
Lineage event data saved to lineage_events_dzd-abc123xyz_us-east-1_1742445678.json
```

#### Output File

The script creates a JSON file with the naming pattern:
```
lineage_events_{domain-identifier}_{region}_{timestamp}.json
```

Example: `lineage_events_dzd-abc123xyz_us-east-1_1742445678.json`

The file contains an array of lineage event objects in OpenLineage format.

### Converting Timestamps

#### From human-readable date to Unix epoch

```bash
# macOS
date -j -f "%Y-%m-%d %H:%M:%S" "2025-01-15 00:00:00" +%s

# Linux
date -d "2025-01-15 00:00:00" +%s
```

#### From Unix epoch to human-readable date

```bash
# macOS
date -r 1742404458

# Linux
date -d @1742404458
```

### Troubleshooting

#### Permission Denied

If you receive permission errors, verify your IAM identity has the required DataZone permissions:

```bash
aws sts get-caller-identity
```

#### Rate Limiting

If you encounter rate limiting errors, reduce the `--max-results` value or add delays between GetLineageEvent calls.

---
