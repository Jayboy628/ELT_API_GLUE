Close.io → AWS Glue Ingest (README)

This project contains a Glue ETL job (closeio_ingest_glue.py) that pulls leads and activities from the Close API and lands them in S3 as Parquet or JSON, partitioned by ingestion date. The job reads the API key from AWS Secrets Manager, handles pagination and rate limits, and writes clean, queryable data to your data lake.

What this job does (high level)

Auth: Reads your Close API key from Secrets Manager (either a plain string or {"api_key":"..."}).

Verify: Calls GET /me/ once to fail fast if the key is wrong.

Find leads updated in a date window: For each day in [START_DATE..END_DATE], calls POST /data/search/ with a fixed_local_date filter on date_updated to collect lead IDs (page size is capped at 200).

Fetch activities per lead: For each lead ID, calls GET /activity?lead_id=... using cursor-by-time pagination and enforces Close’s limit ≤ 100.

(Optional) Fetch lead details: If enabled, also pulls GET /lead/{id}.

Write to S3: Batches are converted to Spark DataFrames and written to:

s3://<bucket>/landing/closeio/activities/ingest_date=YYYY-MM-DD/

s3://<bucket>/landing/closeio/leads/ingest_date=YYYY-MM-DD/ (only if enabled)

Adds ingest_ts_utc and ingest_date columns.

Files you’ll use

closeio_ingest_glue.py — the Glue ETL script.

cloudformation/01_iam.yml — creates the S3 bucket and Glue IAM role.

cloudformation/02_secrets.yml — creates a Secrets Manager secret for the Close API key.

cloudformation/03_gluejobs.yml — creates the Glue job pointing to the script in S3.

cloudformation/deploy_pipeline.sh — helper to deploy the stacks and upload the script.

.github/workflows/deploy.yml — CI workflow to run the deploy script (optional).

Prerequisites

AWS account & IAM permissions to create S3 buckets, Secrets, IAM roles, and Glue jobs.

S3 bucket for scripts and output (the stack can create ${Prefix}-elt-${AccountId}-${Env}).

Close API key (create it in Close and store via Secrets Manager stack).

Secret format:

Either a plain string:

my_long_close_api_key_value


Or JSON:

{"api_key": "my_long_close_api_key_value"}


The script supports both.

How to deploy
Option A — Run the deploy script (local or CI)

Set the secret value in your CI secrets as CLOSE_API_KEY (or export locally before running).

Run the deploy script:

chmod +x cloudformation/deploy_pipeline.sh
./cloudformation/deploy_pipeline.sh


This will:

Create/update IAM stack (bucket + role)

Create/update Secrets stack (writes the API key into Secrets Manager)

Upload closeio_ingest_glue.py to s3://<bucket>/scripts/

Create/update the Glue job

Option B — Manual

Create the S3 bucket first (or use an existing one).

Deploy 01_iam.yml, then 02_secrets.yml, then 03_gluejobs.yml with correct parameters.

Upload closeio_ingest_glue.py to s3://<bucket>/scripts/.

Running the job

You can run from the Glue console or the CLI. Example CLI:

aws glue start-job-run \
  --job-name lead-closeio-ingest-dev \
  --arguments '{
    "--SECRET_ID": "api_key_closeio-dev",
    "--API_BASE_URL": "https://api.close.com/api/v1",
    "--S3_OUTPUT": "s3://lead-elt-123456789012-dev/landing/closeio/",
    "--OUTPUT_FORMAT": "parquet",
    "--START_DATE": "2025-08-23",
    "--END_DATE": "2025-08-24",
    "--PAGE_SIZE": "200",
    "--MAX_PAGES": "500",
    "--FETCH_LEAD_DETAILS": "false",
    "--LEAD_LIMIT": "0"
  }'


Required arguments

--SECRET_ID — Secret name or ARN for the Close API key.

--API_BASE_URL — Usually https://api.close.com/api/v1.

--S3_OUTPUT — Base S3 path for output (the script appends activities/ and leads/).

--OUTPUT_FORMAT — parquet or json.

Optional arguments

--START_DATE — YYYY-MM-DD. Default: yesterday.

--END_DATE — YYYY-MM-DD. Default: today.

--PAGE_SIZE — Suggested 200 (capped automatically: 200 for search, 100 for activities).

--MAX_PAGES — Safety cap for /data/search/ pagination (default 500).

--FETCH_LEAD_DETAILS — true|false (default false).

--LEAD_LIMIT — Limit number of leads processed (default 0 = no cap).

S3 output layout
s3://<bucket>/landing/closeio/
  activities/
    ingest_date=2025-08-24/
      part-*.snappy.parquet
  leads/                      (only if FETCH_LEAD_DETAILS=true)
    ingest_date=2025-08-24/
      part-*.snappy.parquet


Each file includes:

All fields returned by Close

ingest_ts_utc (Spark ingestion timestamp)

ingest_date (partition column)

You can query these data sets with Athena by pointing at each prefix and partitioning by ingest_date.

How pagination & limits work

Lead search uses POST /data/search/ with a fixed_local_date filter on date_updated for each day in your window.

Page size is capped at 200 (Close’s API limit).

We page using the cursor returned by Close until it’s gone or we hit MAX_PAGES.

Activities per lead use GET /activity?lead_id=....

limit is capped at 100 (Close’s API limit) — the script enforces this to prevent HTTP 400 errors.

We paginate by repeatedly requesting older items with activity_at__lt (fallback is date_created__lt).

Rate limits & retries

Both GET and POST requests retry for 429 and 5xx responses with exponential backoff + jitter.

A quick pre-flight GET /me/ validates the key before heavy pulls.

Configuration notes

Output format: --OUTPUT_FORMAT must be parquet or json. Parquet is recommended for query performance and cost.

Date window defaults: If you omit dates, the job pulls yesterday..today.

Lead details: Disabled by default. Enable with --FETCH_LEAD_DETAILS true to write a leads/ dataset.

LEAD_LIMIT: Useful for testing (e.g., --LEAD_LIMIT 100).

IAM & permissions

The Glue job role must allow:

S3 read/write to your bucket (prefixes for scripts, temp, landing paths).

Secrets Manager GetSecretValue.

Glue & CloudWatch Logs defaults (handled by AWS managed policies in 01_iam.yml).

Common errors & fixes

401 Unauthorized

Cause: Wrong/empty API key, wrong secret name/ARN, region mismatch.

Fix: Verify the secret value and format (string or {"api_key":"..."}), confirm region and --SECRET_ID.

The script fails fast via GET /me/ with a helpful message.

400: max_limit = 100 on /activity

Cause: Passing limit > 100.

Fix: Built-in: the script now caps activity limit to 100 automatically.

NoSuchBucket when writing to S3

Cause: Bucket not created or wrong name.

Fix: Create bucket first (the IAM stack can do this) or correct --S3_OUTPUT.

AccessDenied to Secrets or S3

Cause: Missing IAM permissions for Glue role.

Fix: Confirm role policies include secretsmanager:GetSecretValue and S3 access to the specific bucket & prefixes.

Performance tips

Start with NumberOfWorkers: 2 (G.1X) and scale up if you have many leads.

Larger PAGE_SIZE speeds up /data/search (capped at 200).

Activities volume depends on your Close usage; consider running the job per day or scheduling.

Example: Athena quick start (Parquet)
CREATE EXTERNAL TABLE IF NOT EXISTS close_activities (
  -- define columns you frequently query; unknown fields will still exist in Parquet
  lead_id string,
  activity_at string,
  _type string
)
PARTITIONED BY (ingest_date date)
STORED AS PARQUET
LOCATION 's3://<bucket>/landing/closeio/activities/';

MSCK REPAIR TABLE close_activities;  -- discover partitions

Troubleshooting checklist

CloudWatch Logs → check the Glue job run for stack traces.

Confirm the secrets value is correct and in the same region that Glue is running.

Verify --S3_OUTPUT and that the bucket exists and is writable.

If the job seems slow or stops early, look for 429/5xx retry logs. You might be hitting Close rate limits—try a narrower date window or stagger runs.

Maintaining & extending

New endpoints: Follow the same request pattern—respect Close’s per-endpoint limits and pagination style.

Schema drift: Parquet handles evolving fields well; if you need a strict schema, add a transform step after landing.

Event-mode: If you later switch to “since last cursor” ingestion, add a small JSON cursor file in S3 and pass a MODE arg—this code is “window-mode” (by date) today.

Support

If you hit issues, collect the error block from CloudWatch (status code + first 200 bytes of the response) and the job arguments you passed. That’s usually enough to pinpoint auth, limits, or permissions problems quickly.

ChatGPT can make mistakes. Check important info.