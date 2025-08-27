import os, json, time, base64, random, traceback, datetime as dt
from typing import Dict, Any, List, Optional
import boto3
import urllib3
from pyspark.sql import functions as F

# -----------------------------
# CONFIG (edit these)
# -----------------------------
REGION             = os.environ.get("AWS_REGION", "us-east-1")
SECRET_ID          = "api_key_closeio-dev"   # Secrets Manager name or ARN; secret = raw string or {"api_key":"..."}
API_BASE_URL       = "https://api.close.com/api/v1"
S3_OUTPUT          = "s3://lead-elt-657082399901-dev/landing/closeio/"  # bucket/prefix
OUTPUT_FORMAT      = "parquet"               # "parquet" or "json"

START_DATE         = ""                      # "YYYY-MM-DD" (inclusive). If "", defaults to yesterday
END_DATE           = ""                      # "YYYY-MM-DD" (inclusive). If "", defaults to today

PAGE_SIZE          = 200                     # leads search page size (cap at 200)
MAX_PAGES          = 500                     # safety cap per-day search pages
FETCH_LEAD_DETAILS = False                   # also fetch /lead/{id}/
LEAD_LIMIT         = 0                       # 0 = no cap; otherwise first N leads

ACT_FLUSH_EVERY    = 10_000                  # flush activities to S3 every N records
LEAD_FLUSH_EVERY   = 1_000                   # flush lead details to S3 every N records

# -----------------------------
# Spark / Glue session
# -----------------------------
try:
    from pyspark.context import SparkContext
    from awsglue.context import GlueContext
    sc = SparkContext.getOrCreate()
    glueContext = GlueContext(sc)
    spark = glueContext.spark_session
except Exception:
    from pyspark.sql import SparkSession
    spark = SparkSession.builder.getOrCreate()

# -----------------------------
# Resolve and normalize config
# -----------------------------
out_base = S3_OUTPUT.rstrip("/") + "/"
act_out  = out_base + "activities/"
lead_out = out_base + "leads/"

limit_lead_search = min(max(int(PAGE_SIZE), 1), 200)  # Close caps around 200
limit_activity    = min(max(int(PAGE_SIZE), 1), 100)  # Close caps at 100

today = dt.date.today()
if START_DATE:
    start_day = dt.datetime.strptime(START_DATE, "%Y-%m-%d").date()
else:
    start_day = today - dt.timedelta(days=1)  # default yesterday
if END_DATE:
    end_day = dt.datetime.strptime(END_DATE, "%Y-%m-%d").date()
else:
    end_day = today

if OUTPUT_FORMAT not in ("parquet", "json"):
    raise ValueError(f"OUTPUT_FORMAT must be parquet|json, got {OUTPUT_FORMAT!r}")

# -----------------------------
# Fetch API key from Secrets Manager (inline)
# -----------------------------
sm = boto3.client("secretsmanager", region_name=REGION)
_secret_str = sm.get_secret_value(SecretId=SECRET_ID)["SecretString"]

api_key = None
try:
    _parsed = json.loads(_secret_str)
    if isinstance(_parsed, dict):
        api_key = _parsed.get("api_key") or _parsed.get("API_KEY")
    elif isinstance(_parsed, str):
        api_key = _parsed
except json.JSONDecodeError:
    api_key = _secret_str

if not api_key:
    raise RuntimeError(f"Secret {SECRET_ID} did not contain an API key")
api_key = api_key.strip().strip('"').strip("'")
if api_key.endswith(":"):
    api_key = api_key[:-1]
if not api_key:
    raise RuntimeError("API key is empty after normalization")

# -----------------------------
# Build auth headers (inline)
# -----------------------------
token = base64.b64encode(f"{api_key}:".encode("utf-8")).decode("ascii")
headers = {
    "Authorization": f"Basic {token}",
    "Accept": "application/json",
    "Content-Type": "application/json",
    "User-Agent": "glue-closeio-ingest/1.0"
}
print(f"[AUTH] Using API key len={len(api_key)} fingerprint={api_key[:4]}…{api_key[-3:]}")

# -----------------------------
# HTTP client + verify auth
# -----------------------------
http = urllib3.PoolManager()

# GET /me/ with light retry for 429/5xx
_auth_url = API_BASE_URL.rstrip("/") + "/me/"
_attempt = 0
while True:
    _resp = http.request("GET", _auth_url, headers=headers,
                         timeout=urllib3.Timeout(connect=10.0, read=30.0),
                         retries=False)
    if _resp.status in (429,) or _resp.status >= 500:
        _attempt += 1
        if _attempt > 6:
            raise RuntimeError(f"Auth check failed after retries: {_resp.status} {_resp.data[:200]!r}")
        _delay = min(0.5 * (2 ** (_attempt - 1)), 16.0) * (0.5 + random.random())
        print(f"[WARN] /me/ {_resp.status}, retry in ~{_delay:.2f}s")
        time.sleep(_delay)
        continue
    if _resp.status != 200:
        raise RuntimeError(f"Auth check failed: {_resp.status} {_resp.data[:200]!r}")
    break
print("[AUTH] OK")

print(f"[START] window={start_day}..{end_day} page_size={PAGE_SIZE} max_pages={MAX_PAGES} "
      f"details={FETCH_LEAD_DETAILS} limit={LEAD_LIMIT} fmt={OUTPUT_FORMAT} out={out_base}")

# -----------------------------
# Collect lead IDs (cursor pagination, NO functions)
# -----------------------------
all_leads: List[str] = []
_day = start_day
while _day <= end_day:
    _cursor = None
    _page = 0
    while _page < MAX_PAGES:
        _params = {
            "_limit": limit_lead_search,
            "query": {
                "negate": False,
                "queries": [
                    {"negate": False, "object_type": "lead", "type": "object_type"},
                    {
                        "negate": False,
                        "queries": [
                            {
                                "negate": False,
                                "queries": [
                                    {
                                        "condition": {
                                            "before":      {"type": "fixed_local_date", "value": _day.strftime("%Y-%m-%d"), "which": "end"},
                                            "on_or_after": {"type": "fixed_local_date", "value": _day.strftime("%Y-%m-%d"), "which": "start"},
                                            "type": "moment_range"
                                        },
                                        "field": {"field_name": "date_updated", "object_type": "lead", "type": "regular_field"},
                                        "negate": False,
                                        "type": "field_condition"
                                    }
                                ],
                                "type": "and"
                            }
                        ],
                        "type": "and"
                    }
                ],
                "type": "and"
            },
            "sort": []
        }
        if _cursor:
            _params["cursor"] = _cursor

        # POST /data/search/ with retry for 429/5xx
        _attempt = 0
        while True:
            _resp = http.request(
                "POST",
                API_BASE_URL.rstrip("/") + "/data/search/",
                body=json.dumps(_params).encode("utf-8"),
                headers=headers,
                timeout=urllib3.Timeout(connect=10.0, read=60.0),
                retries=False
            )
            if _resp.status in (429,) or _resp.status >= 500:
                _attempt += 1
                if _attempt > 6:
                    raise RuntimeError(f"POST /data/search failed after retries: {_resp.status} {_resp.data[:200]!r}")
                _delay = min(0.5 * (2 ** (_attempt - 1)), 16.0) * (0.5 + random.random())
                print(f"[WARN] search {_resp.status}, retry in ~{_delay:.2f}s")
                time.sleep(_delay)
                continue
            if _resp.status >= 400:
                raise RuntimeError(f"POST /data/search failed: {_resp.status} {_resp.data[:200]!r}")
            _payload = json.loads(_resp.data)
            break

        _data = _payload.get("data", [])
        _ids = [it["id"] for it in _data if it.get("__object_type") == "lead"]
        all_leads.extend(_ids)
        _cursor = _payload.get("cursor")
        _page += 1
        if not _cursor:
            break

    print(f"[LEADS] day={_day} fetched={len(_ids)} total={len(all_leads)}")
    _day += dt.timedelta(days=1)

# de-dup while preserving order
_seen = set()
deduped: List[str] = []
for _lid in all_leads:
    if _lid not in _seen:
        _seen.add(_lid)
        deduped.append(_lid)
if LEAD_LIMIT > 0:
    deduped = deduped[:LEAD_LIMIT]
print(f"[LEADS] unique={len(deduped)}")

# -----------------------------
# Fetch activities (+ optional lead details) with pagination; write in batches
# -----------------------------
act_buffer: List[Dict[str, Any]] = []
lead_buffer: List[Dict[str, Any]] = []
total_act = 0
total_lead = 0

for _idx, _lead_id in enumerate(deduped, 1):
    try:
        # ---- Activities pagination (limit<=100, walk back by activity_at__lt)
        _first = True
        _cursor_time = None
        while True:
            _params: Dict[str, Any] = {"lead_id": _lead_id, "limit": limit_activity}
            if not _first and _cursor_time:
                _params["activity_at__lt"] = _cursor_time

            # GET /activity/ with retry
            _attempt = 0
            while True:
                _resp = http.request(
                    "GET",
                    API_BASE_URL.rstrip("/") + "/activity/",
                    headers=headers,
                    fields=_params,  # urllib3 will add as query params
                    timeout=urllib3.Timeout(connect=10.0, read=60.0),
                    retries=False
                )
                if _resp.status in (429,) or _resp.status >= 500:
                    _attempt += 1
                    if _attempt > 6:
                        raise RuntimeError(f"GET /activity failed after retries: {_resp.status} {_resp.data[:200]!r}")
                    _delay = min(0.5 * (2 ** (_attempt - 1)), 16.0) * (0.5 + random.random())
                    print(f"[WARN] activity {_resp.status}, retry in ~{_delay:.2f}s")
                    time.sleep(_delay)
                    continue
                if _resp.status >= 400:
                    raise RuntimeError(f"GET /activity failed: {_resp.status} {_resp.data[:200]!r}")
                _payload = json.loads(_resp.data)
                break

            _data = _payload.get("data", [])
            if not _data:
                break

            for _it in _data:
                if "lead_id" not in _it:
                    _it["lead_id"] = _lead_id
            act_buffer.extend(_data)

            if not _payload.get("has_more", False):
                break

            _cursor_time = _data[-1].get("activity_at") or _data[-1].get("date_created")
            _first = False

        # ---- Optional lead detail
        if FETCH_LEAD_DETAILS:
            _attempt = 0
            while True:
                _resp = http.request(
                    "GET",
                    API_BASE_URL.rstrip("/") + f"/lead/{_lead_id}/",
                    headers=headers,
                    timeout=urllib3.Timeout(connect=10.0, read=60.0),
                    retries=False
                )
                if _resp.status in (429,) or _resp.status >= 500:
                    _attempt += 1
                    if _attempt > 6:
                        raise RuntimeError(f"GET /lead/{{id}} failed after retries: {_resp.status} {_resp.data[:200]!r}")
                    _delay = min(0.5 * (2 ** (_attempt - 1)), 16.0) * (0.5 + random.random())
                    print(f"[WARN] lead detail {_resp.status}, retry in ~{_delay:.2f}s")
                    time.sleep(_delay)
                    continue
                if _resp.status >= 400:
                    raise RuntimeError(f"GET /lead/{{id}} failed: {_resp.status} {_resp.data[:200]!r}")
                _detail = json.loads(_resp.data)
                break

            lead_buffer.append(_detail)

        # ---- Periodic flushes (activities)
        if len(act_buffer) >= ACT_FLUSH_EVERY:
            _rdd = spark.sparkContext.parallelize([json.dumps(r) for r in act_buffer])
            _df = spark.read.json(_rdd)
            _now = F.current_timestamp()
            _df = _df.withColumn("ingest_ts_utc", _now).withColumn("ingest_date", F.to_date(_now))
            if OUTPUT_FORMAT == "parquet":
                _df.write.mode("append").partitionBy("ingest_date").parquet(act_out)
            else:
                _df.write.mode("append").partitionBy("ingest_date").json(act_out)
            _written = _df.count()
            total_act += _written
            act_buffer.clear()
            print(f"[WRITE] activities batch={_written} total={total_act}")

        # ---- Periodic flushes (lead details)
        if FETCH_LEAD_DETAILS and len(lead_buffer) >= LEAD_FLUSH_EVERY:
            _rdd = spark.sparkContext.parallelize([json.dumps(r) for r in lead_buffer])
            _df = spark.read.json(_rdd)
            _now = F.current_timestamp()
            _df = _df.withColumn("ingest_ts_utc", _now).withColumn("ingest_date", F.to_date(_now))
            if OUTPUT_FORMAT == "parquet":
                _df.write.mode("append").partitionBy("ingest_date").parquet(lead_out)
            else:
                _df.write.mode("append").partitionBy("ingest_date").json(lead_out)
            _written = _df.count()
            total_lead += _written
            lead_buffer.clear()
            print(f"[WRITE] leads batch={_written} total={total_lead}")

        if _idx % 100 == 0:
            print(f"[PROGRESS] leads processed {_idx}/{len(deduped)}")

    except Exception as e:
        print(f"[ERROR] lead_id={_lead_id}: {e}")
        print(traceback.format_exc())

# -----------------------------
# Final flush
# -----------------------------
if act_buffer:
    _rdd = spark.sparkContext.parallelize([json.dumps(r) for r in act_buffer])
    _df = spark.read.json(_rdd)
    _now = F.current_timestamp()
    _df = _df.withColumn("ingest_ts_utc", _now).withColumn("ingest_date", F.to_date(_now))
    if OUTPUT_FORMAT == "parquet":
        _df.write.mode("append").partitionBy("ingest_date").parquet(act_out)
    else:
        _df.write.mode("append").partitionBy("ingest_date").json(act_out)
    _written = _df.count()
    total_act += _written
    print(f"[WRITE] activities final={_written} total={total_act}")

if FETCH_LEAD_DETAILS and lead_buffer:
    _rdd = spark.sparkContext.parallelize([json.dumps(r) for r in lead_buffer])
    _df = spark.read.json(_rdd)
    _now = F.current_timestamp()
    _df = _df.withColumn("ingest_ts_utc", _now).withColumn("ingest_date", F.to_date(_now))
    if OUTPUT_FORMAT == "parquet":
        _df.write.mode("append").partitionBy("ingest_date").parquet(lead_out)
    else:
        _df.write.mode("append").partitionBy("ingest_date").json(lead_out)
    _written = _df.count()
    total_lead += _written
    print(f"[WRITE] leads final={_written} total={total_lead}")

print(f"[DONE] total_activities={total_act} total_leads={total_lead}")
