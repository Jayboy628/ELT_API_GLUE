# closeio_ingest_glue.py
# Glue 4.0 (Python 3.10). Ingest Close leads/activities using an API key from Secrets Manager.

import sys, json, time, datetime as dt, base64, traceback
from typing import Dict, Any, Iterable, List, Optional

import boto3
import urllib3

from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import functions as F

# -----------------------
# Job Arguments
# -----------------------
args = getResolvedOptions(sys.argv, [
    "JOB_NAME",
    "SECRET_ID",          # e.g. api_key_closeio-dev (secret contains {"api_key":"..."} or raw string)
    "API_BASE_URL",       # e.g. https://api.close.com/api/v1
    "S3_OUTPUT",          # e.g. s3://your-bucket/landing/closeio/
    "OUTPUT_FORMAT",      # parquet | json

    # Optional knobs:
    "START_DATE",         # e.g. 2025-08-22  (inclusive, local/UTC not critical for Close's fixed_local_date)
    "END_DATE",           # e.g. 2025-08-23  (inclusive end-of-day loop)
    "PAGE_SIZE",          # default 200
    "MAX_PAGES",          # safety cap per search request, default 500
    "FETCH_LEAD_DETAILS", # true|false (default false)
    "LEAD_LIMIT"          # for testing; positive int caps number of leads processed
])

SECRET_ID     = args["SECRET_ID"]
API_BASE_URL  = args["API_BASE_URL"].rstrip("/")
S3_OUTPUT     = args["S3_OUTPUT"].rstrip("/") + "/"
OUTPUT_FORMAT = args["OUTPUT_FORMAT"].lower()

START_DATE    = args.get("START_DATE") or None
END_DATE      = args.get("END_DATE") or None
PAGE_SIZE     = int(args.get("PAGE_SIZE", "200"))
MAX_PAGES     = int(args.get("MAX_PAGES", "500"))
FETCH_LEAD_DETAILS = (args.get("FETCH_LEAD_DETAILS", "false").lower() == "true")
LEAD_LIMIT    = int(args.get("LEAD_LIMIT", "0"))  # 0 = no limit

# -----------------------
# Glue setup
# -----------------------
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# -----------------------
# Helpers
# -----------------------
def get_api_key_from_secrets(secret_id: str) -> str:
    sm = boto3.client("secretsmanager")
    val = sm.get_secret_value(SecretId=secret_id)
    if "SecretString" in val:
        txt = val["SecretString"]
        try:
            payload = json.loads(txt)
            # support either {"api_key":"..."} or {"token":"..."} or a raw string
            k = payload.get("api_key") or payload.get("token")
            return k if k else txt
        except Exception:
            # raw string in SecretString
            return txt
    else:
        return val["SecretBinary"].decode("utf-8")

def build_auth_header(api_key: str) -> str:
    # If the key looks like "key:anything" use Basic auth; otherwise use Bearer.
    if ":" in api_key:
        b64 = base64.b64encode(api_key.encode("utf-8")).decode("ascii")
        return f"Basic {b64}"
    return f"Bearer {api_key}"

def backoff_sleep(attempt: int, base: float = 1.0, cap: float = 30.0):
    time.sleep(min(cap, base * (2 ** attempt)))

def date_range_days(start_date: dt.date, end_date: dt.date) -> Iterable[dt.date]:
    cur = start_date
    one = dt.timedelta(days=1)
    while cur <= end_date:
        yield cur
        cur += one

def http_post_json(http: urllib3.PoolManager, url: str, headers: Dict[str, str], body: Dict[str, Any]) -> Dict[str, Any]:
    data = json.dumps(body).encode("utf-8")
    attempt = 0
    while True:
        resp = http.request("POST", url, body=data, headers=headers, timeout=urllib3.Timeout(connect=10.0, read=60.0))
        if resp.status == 429 or resp.status >= 500:
            attempt += 1
            print(f"[WARN] POST {url} -> {resp.status}, retry attempt={attempt}")
            backoff_sleep(attempt)
            if attempt > 6:
                raise RuntimeError(f"POST {url} failed after retries: {resp.status} {resp.data[:200]!r}")
            continue
        if resp.status >= 400:
            raise RuntimeError(f"POST {url} failed: {resp.status} {resp.data[:200]!r}")
        return json.loads(resp.data)

def http_get(http: urllib3.PoolManager, url: str, headers: Dict[str, str], params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    q = ""
    if params:
        from urllib.parse import urlencode
        q = "?" + urlencode(params, doseq=True)
    attempt = 0
    while True:
        resp = http.request("GET", url + q, headers=headers, timeout=urllib3.Timeout(connect=10.0, read=60.0))
        if resp.status == 429 or resp.status >= 500:
            attempt += 1
            print(f"[WARN] GET {url} -> {resp.status}, retry attempt={attempt}")
            backoff_sleep(attempt)
            if attempt > 6:
                raise RuntimeError(f"GET {url} failed after retries: {resp.status} {resp.data[:200]!r}")
            continue
        if resp.status >= 400:
            raise RuntimeError(f"GET {url} failed: {resp.status} {resp.data[:200]!r}")
        return json.loads(resp.data)

def search_lead_ids_for_day(http, headers, day: dt.date, page_size: int, max_pages: int) -> List[str]:
    """
    Calls POST /data/search to get Lead IDs where date_updated is on 'day'.
    Mirrors your original JSON query (fixed_local_date on start/end of day).
    """
    url = f"{API_BASE_URL}/data/search/"
    lead_ids: List[str] = []
    cursor = None
    page = 0
    while page < max_pages:
        params = {
            "_limit": page_size,
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
                                            "before":     {"type": "fixed_local_date", "value": day.strftime("%Y-%m-%d"), "which": "end"},
                                            "on_or_after":{"type": "fixed_local_date", "value": day.strftime("%Y-%m-%d"), "which": "start"},
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
            "results_limit": None,
            "sort": []
        }
        if cursor:
            params["cursor"] = cursor
        resp = http_post_json(http, url, headers, params)
        data = resp.get("data", [])
        ids = [it["id"] for it in data if it.get("__object_type") == "lead"]
        lead_ids.extend(ids)
        cursor = resp.get("cursor")
        page += 1
        if not cursor:
            break
    return lead_ids

def fetch_all_activities_for_lead(http, headers, lead_id: str) -> List[Dict[str, Any]]:
    """
    GET /activity?lead_id=... with pagination using has_more + last item’s activity_at.
    """
    items: List[Dict[str, Any]] = []
    first = True
    cursor_time = None
    while True:
        params = {"lead_id": lead_id}
        if not first and cursor_time:
            # Walk back in time
            params["date_created__lt"] = cursor_time
        resp = http_get(http, f"{API_BASE_URL}/activity/", headers, params)
        data = resp.get("data", [])
        items.extend(data)
        if not resp.get("has_more", False) or not data:
            break
        cursor_time = data[-1].get("activity_at") or data[-1].get("date_created")
        first = False
    # attach lead_id if missing
    for it in items:
        it.setdefault("lead_id", lead_id)
    return items

def fetch_lead_detail(http, headers, lead_id: str) -> Dict[str, Any]:
    return http_get(http, f"{API_BASE_URL}/lead/{lead_id}/", headers)

def write_records(records: List[Dict[str, Any]], out_path: str, fmt: str):
    if not records:
        return 0
    # Convert small batch to Spark DF without driver OOM
    rdd = spark.sparkContext.parallelize([json.dumps(r) for r in records])
    df = spark.read.json(rdd)
    now = F.current_timestamp()
    df = df.withColumn("ingest_ts_utc", now).withColumn("ingest_date", F.to_date(now))
    mode = "append"
    if fmt == "parquet":
        df.write.mode(mode).partitionBy("ingest_date").parquet(out_path)
    elif fmt == "json":
        df.write.mode(mode).partitionBy("ingest_date").json(out_path)
    else:
        raise ValueError(f"Unsupported OUTPUT_FORMAT {fmt}")
    return df.count()

# -----------------------
# Main
# -----------------------
def main():
    api_key = get_api_key_from_secrets(SECRET_ID).strip()
    auth_header = build_auth_header(api_key)
    headers = {
        "Authorization": auth_header,
        "Accept": "application/json",
        "Content-Type": "application/json",
        "User-Agent": "glue-close-ingest/1.0"
    }
    http = urllib3.PoolManager()

    # Date window to search leads (day-by-day)
    today = dt.date.today()
    start_day = dt.datetime.strptime(START_DATE, "%Y-%m-%d").date() if START_DATE else today
    end_day   = dt.datetime.strptime(END_DATE,   "%Y-%m-%d").date() if END_DATE   else today

    print(f"[START] window={start_day}..{end_day} page_size={PAGE_SIZE} max_pages={MAX_PAGES} details={FETCH_LEAD_DETAILS} limit={LEAD_LIMIT}")

    # 1) Gather lead IDs for the date window
    all_leads: List[str] = []
    for day in date_range_days(start_day, end_day):
        ids = search_lead_ids_for_day(http, headers, day, PAGE_SIZE, MAX_PAGES)
        all_leads.extend(ids)
        print(f"[LEADS] day={day} fetched={len(ids)} total={len(all_leads)}")
    # de-dup while preserving order
    seen = set()
    deduped = []
    for lid in all_leads:
        if lid not in seen:
            seen.add(lid)
            deduped.append(lid)
    if LEAD_LIMIT > 0:
        deduped = deduped[:LEAD_LIMIT]
    print(f"[LEADS] unique={len(deduped)}")

    # 2) For each lead: fetch activities (and optionally details)
    act_out = f"{S3_OUTPUT}activities/"
    lead_out = f"{S3_OUTPUT}leads/"

    ACT_FLUSH_EVERY = 10000   # write every N activity records
    LEAD_FLUSH_EVERY = 1000   # write every N lead details

    act_buffer: List[Dict[str, Any]] = []
    lead_buffer: List[Dict[str, Any]] = []

    total_act = 0
    total_lead = 0

    for idx, lead_id in enumerate(deduped, 1):
        try:
            acts = fetch_all_activities_for_lead(http, headers, lead_id)
            act_buffer.extend(acts)
            if FETCH_LEAD_DETAILS:
                det = fetch_lead_detail(http, headers, lead_id)
                lead_buffer.append(det)

            if len(act_buffer) >= ACT_FLUSH_EVERY:
                written = write_records(act_buffer, act_out, OUTPUT_FORMAT)
                total_act += written
                print(f"[WRITE] activities batch: {written} (total={total_act})")
                act_buffer.clear()

            if FETCH_LEAD_DETAILS and len(lead_buffer) >= LEAD_FLUSH_EVERY:
                written = write_records(lead_buffer, lead_out, OUTPUT_FORMAT)
                total_lead += written
                print(f"[WRITE] leads batch: {written} (total={total_lead})")
                lead_buffer.clear()

            if idx % 100 == 0:
                print(f"[PROGRESS] leads processed {idx}/{len(deduped)}")

        except Exception as e:
            print(f"[ERROR] lead_id={lead_id}: {e}")
            print(traceback.format_exc())

    # final flush
    if act_buffer:
        written = write_records(act_buffer, act_out, OUTPUT_FORMAT)
        total_act += written
        print(f"[WRITE] activities final: {written} (total={total_act})")
    if FETCH_LEAD_DETAILS and lead_buffer:
        written = write_records(lead_buffer, lead_out, OUTPUT_FORMAT)
        total_lead += written
        print(f"[WRITE] leads final: {written} (total={total_lead})")

    print(f"[DONE] total_activities={total_act} total_leads={total_lead}")

try:
    main()
    job.commit()
except Exception as e:
    print("[FATAL]", e)
    print(traceback.format_exc())
    raise
