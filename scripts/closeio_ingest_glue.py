# closeio_ingest_glue.py
# Glue 4.0 (Python 3.10). Ingest Close activities using an API key from Secrets Manager.

import sys, json, time, datetime as dt, base64, traceback, os, random
from typing import Dict, Any, Iterable, List, Optional, Tuple

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
    "SECRET_ID",           # e.g. api_key_closeio-dev (raw string or {"api_key":"..."})
    "API_BASE_URL",        # e.g. https://api.close.com/api/v1
    "S3_OUTPUT",           # e.g. s3://bucket/landing/closeio/
    "OUTPUT_FORMAT",       # parquet | json
    "MODE",                # window | event
    # Optional knobs:
    "START_DATE",          # YYYY-MM-DD
    "END_DATE",            # YYYY-MM-DD
    "PAGE_SIZE",           # default 200
    "MAX_PAGES",           # default 500 (window mode)
    "FETCH_LEAD_DETAILS",  # true|false
    "LEAD_LIMIT",          # int; 0 = no limit (window mode)
    "CURSOR_S3"            # s3://.../bookmarks/event_cursor.json  (event mode)
])

SECRET_ID     = args["SECRET_ID"]
API_BASE_URL  = args["API_BASE_URL"].rstrip("/")
S3_OUTPUT     = args["S3_OUTPUT"].rstrip("/") + "/"
OUTPUT_FORMAT = args["OUTPUT_FORMAT"].lower()
MODE          = args["MODE"].lower()

START_DATE    = args.get("START_DATE") or None
END_DATE      = args.get("END_DATE") or None
PAGE_SIZE     = int(args.get("PAGE_SIZE", "200"))
MAX_PAGES     = int(args.get("MAX_PAGES", "500"))
FETCH_LEAD_DETAILS = (args.get("FETCH_LEAD_DETAILS", "false").lower() == "true")
LEAD_LIMIT    = int(args.get("LEAD_LIMIT", "0"))
CURSOR_S3     = (args.get("CURSOR_S3") or (S3_OUTPUT + "bookmarks/event_cursor.json"))

if OUTPUT_FORMAT not in ("parquet", "json"):
    raise ValueError(f"OUTPUT_FORMAT must be parquet|json, got {OUTPUT_FORMAT!r}")
if MODE not in ("window", "event"):
    raise ValueError(f"MODE must be 'window' or 'event', got {MODE!r}")

# -----------------------
# Glue setup
# -----------------------
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# -----------------------
# S3 helpers (bookmarks, etc.)
# -----------------------
def _split_s3_uri(uri: str) -> Tuple[str, str]:
    if not uri.startswith("s3://"):
        raise ValueError(f"Invalid S3 URI: {uri!r}")
    p = uri[5:]
    bucket, key = (p.split("/", 1) + [""])[:2]
    return bucket, key

def s3_read_json(uri: str) -> Optional[dict]:
    b, k = _split_s3_uri(uri)
    s3 = boto3.client("s3")
    try:
        obj = s3.get_object(Bucket=b, Key=k)
        return json.loads(obj["Body"].read().decode("utf-8"))
    except s3.exceptions.NoSuchKey:
        return None
    except Exception:
        return None

def s3_write_json(uri: str, payload: dict):
    b, k = _split_s3_uri(uri)
    boto3.client("s3").put_object(
        Bucket=b,
        Key=k,
        Body=json.dumps(payload, default=str).encode("utf-8"),
        ContentType="application/json"
    )

# -----------------------
# HTTP helpers
# -----------------------
def backoff_sleep(attempt: int):
    """Exponential backoff with jitter: 0.5,1,2,4,8,16s capped."""
    base = min(0.5 * (2 ** (attempt - 1)), 16.0)
    time.sleep(base * (0.5 + random.random()))

def get_api_key_from_secret(secret_id: str) -> str:
    """Load Close API key from Secrets Manager (supports raw string or {"api_key": "..."} JSON)."""
    sm = boto3.client("secretsmanager", region_name=os.environ.get("AWS_REGION", "us-east-1"))
    s = sm.get_secret_value(SecretId=secret_id)["SecretString"]
    key = None
    try:
        parsed = json.loads(s)
        if isinstance(parsed, dict):
            key = parsed.get("api_key") or parsed.get("API_KEY")
        elif isinstance(parsed, str):
            key = parsed
    except json.JSONDecodeError:
        key = s
    if not key:
        raise RuntimeError(f"Secret {secret_id} did not contain an API key")
    key = key.strip().strip('"').strip("'")
    return key[:-1] if key.endswith(":") else key

def build_close_headers(api_key: str) -> Dict[str, str]:
    """Close v1 uses HTTP Basic with API_KEY as username and empty password."""
    token = base64.b64encode(f"{api_key}:".encode("utf-8")).decode("ascii")
    return {
        "Authorization": f"Basic {token}",
        "Accept": "application/json",
        "Content-Type": "application/json",
        "User-Agent": "glue-closeio-ingest/1.1"
    }

http = urllib3.PoolManager()

def _sleep_from_headers(resp):
    ra = resp.headers.get("Retry-After")
    try:
        if ra:
            time.sleep(float(ra))
            return
    except:
        pass
    time.sleep(2.0)

def request_json(method: str, url: str, headers: Dict[str,str], params: Optional[Dict[str,Any]]=None, body: Optional[Dict[str,Any]]=None):
    from urllib.parse import urlencode
    q = ("?" + urlencode(params, doseq=True)) if params else ""
    data = json.dumps(body).encode("utf-8") if body is not None else None
    attempt = 0
    while True:
        resp = http.request(method.upper(), url + q, headers=headers, body=data,
                            timeout=urllib3.Timeout(connect=10, read=60), retries=False)
        if resp.status == 429:
            attempt += 1
            _sleep_from_headers(resp)
            continue
        if resp.status >= 500:
            attempt += 1
            backoff_sleep(attempt)
            if attempt > 6:
                raise RuntimeError(f"{method} {url} failed after retries: {resp.status} {resp.data[:200]!r}")
            continue
        if resp.status >= 400:
            raise RuntimeError(f"{method} {url} failed: {resp.status} {resp.data[:200]!r}")
        return json.loads(resp.data)

def verify_auth(base_url: str, headers: Dict[str, str]):
    url = base_url.rstrip("/") + "/me/"
    resp = http.request("GET", url, headers=headers,
                        timeout=urllib3.Timeout(connect=10, read=30), retries=False)
    if resp.status != 200:
        raise RuntimeError(f"Auth check failed: GET {url} -> {resp.status} {resp.data[:200]!r}")

# -----------------------
# Date helpers
# -----------------------
def date_range_days(start_date: dt.date, end_date: dt.date) -> Iterable[dt.date]:
    cur = start_date
    one = dt.timedelta(days=1)
    while cur <= end_date:
        yield cur
        cur += one

# -----------------------
# Activity pulls (fast path: by window)
# -----------------------
def fetch_activities_window(base_url: str, headers: Dict[str,str], day: dt.date,
                            page_size: int = 200, max_pages: int = 500) -> List[Dict[str,Any]]:
    """
    Pull ALL activities for a day using date_created filters + _limit/_skip.
    Order by -date_created to align with filter (faster).
    """
    url = f"{base_url}/activity/"
    start = f"{day}T00:00:00Z"
    end   = f"{day}T23:59:59Z"

    activities: List[Dict[str,Any]] = []
    skip = 0
    pages = 0
    while pages < max_pages:
        params = {
            "_limit": page_size,
            "_skip": skip,
            "_order_by": "-date_created",
            "date_created__gte": start,
            "date_created__lte": end,
        }
        resp = request_json("GET", url, headers, params=params)
        data = resp.get("data", [])
        activities.extend(data)
        if not resp.get("has_more"):
            break
        skip += page_size
        pages += 1
    return activities

# -----------------------
# Event log incremental (most efficient deltas)
# -----------------------
def get_event_cursor() -> Optional[str]:
    doc = s3_read_json(CURSOR_S3)
    return (doc or {}).get("cursor")

def put_event_cursor(cursor: str):
    s3_write_json(CURSOR_S3, {"cursor": cursor, "saved_at_utc": dt.datetime.utcnow().isoformat()+"Z"})

def fetch_activity_by_id(base_url: str, headers: Dict[str,str], act_id: str) -> Dict[str,Any]:
    return request_json("GET", f"{base_url}/activity/{act_id}/", headers)

def pull_events_and_collect_activity_ids(base_url: str, headers: Dict[str,str],
                                         start_iso: Optional[str], end_iso: Optional[str],
                                         page_limit: int = 50) -> Tuple[List[str], Optional[str]]:
    """
    Iterate /event for object_type=activity, honoring cursor.
    Returns (list_of_activity_ids, next_cursor)
    """
    url = f"{base_url}/event/"
    params: Dict[str,Any] = {"object_type": "activity", "_limit": page_limit}
    cursor = get_event_cursor()
    if cursor:
        params["_cursor"] = cursor
    else:
        if start_iso: params["date_updated__gte"] = start_iso
        if end_iso:   params["date_updated__lt"]  = end_iso

    activity_ids: List[str] = []
    next_cursor: Optional[str] = None

    while True:
        resp = request_json("GET", url, headers, params=params)
        data = resp.get("data", [])
        for ev in data:
            # each event typically has object_id for the activity doc
            oid = ev.get("object_id")
            if oid:
                activity_ids.append(oid)
        next_cursor = resp.get("next_cursor")
        if not next_cursor:
            break
        params = {"_cursor": next_cursor}

    # de-dup while preserving order
    seen = set()
    uniq = []
    for a in activity_ids:
        if a not in seen:
            seen.add(a)
            uniq.append(a)
    return uniq, next_cursor

# -----------------------
# Writing
# -----------------------
def write_records(records: List[Dict[str, Any]], out_path: str, fmt: str) -> int:
    if not records:
        return 0
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
    return len(records)  # avoid extra Spark action

# -----------------------
# Main
# -----------------------
def main():
    api_key = get_api_key_from_secret(SECRET_ID).strip()
    headers = build_close_headers(api_key)
    print(f"Using Close API key len={len(api_key)} fingerprint={api_key[:4]}***{api_key[-3:]}")

    # Fail fast if auth is wrong
    verify_auth(API_BASE_URL, headers)

    # Outputs
    act_out  = f"{S3_OUTPUT}activities/"
    lead_out = f"{S3_OUTPUT}leads/"

    # Buffers
    ACT_FLUSH_EVERY  = 10_000
    LEAD_FLUSH_EVERY = 1_000
    act_buffer: List[Dict[str, Any]] = []
    lead_buffer: List[Dict[str, Any]] = []
    total_act = 0
    total_lead = 0

    # MODE: window (daily slices via /activity)
    if MODE == "window":
        today = dt.date.today()
        start_day = dt.datetime.strptime(START_DATE, "%Y-%m-%d").date() if START_DATE else today
        end_day   = dt.datetime.strptime(END_DATE,   "%Y-%m-%d").date() if END_DATE   else today
        print(f"[START window] {start_day}..{end_day} page_size={PAGE_SIZE} max_pages={MAX_PAGES} details={FETCH_LEAD_DETAILS} limit={LEAD_LIMIT}")

        processed_leads = set()  # only used if you choose to fetch lead details

        for day in date_range_days(start_day, end_day):
            acts = fetch_activities_window(API_BASE_URL, headers, day, PAGE_SIZE, MAX_PAGES)
            if LEAD_LIMIT > 0:
                # optional throttle at record level (crude but simple)
                acts = acts[:LEAD_LIMIT]
            act_buffer.extend(acts)

            if FETCH_LEAD_DETAILS:
                # Collect unique lead_ids from these activities and fetch details (slow; optional)
                for a in acts:
                    lid = a.get("lead_id")
                    if lid and lid not in processed_leads:
                        try:
                            det = request_json("GET", f"{API_BASE_URL}/lead/{lid}/", headers)
                            lead_buffer.append(det)
                            processed_leads.add(lid)
                        except Exception as e:
                            print(f"[WARN] lead detail {lid}: {e}")

            # Flush periodically
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

        # final flush
        if act_buffer:
            written = write_records(act_buffer, act_out, OUTPUT_FORMAT)
            total_act += written
            print(f"[WRITE] activities final: {written} (total={total_act})")
        if FETCH_LEAD_DETAILS and lead_buffer:
            written = write_records(lead_buffer, lead_out, OUTPUT_FORMAT)
            total_lead += written
            print(f"[WRITE] leads final: {written} (total={total_lead})")

    # MODE: event (incremental via /event with cursor)
    else:
        # Optional bounded catch-up window
        start_iso = f"{START_DATE}T00:00:00Z" if START_DATE else None
        end_iso   = f"{END_DATE}T23:59:59Z" if END_DATE else None
        print(f"[START event] start_iso={start_iso} end_iso={end_iso} page_limit=50")

        act_ids, next_cursor = pull_events_and_collect_activity_ids(API_BASE_URL, headers, start_iso, end_iso, page_limit=50)
        print(f"[EVENT] changed activity ids: {len(act_ids)}")

        # Fetch each activity document
        for i, aid in enumerate(act_ids, 1):
            try:
                doc = fetch_activity_by_id(API_BASE_URL, headers, aid)
                act_buffer.append(doc)
            except Exception as e:
                print(f"[WARN] fetch activity {aid}: {e}")

            if len(act_buffer) >= ACT_FLUSH_EVERY:
                written = write_records(act_buffer, act_out, OUTPUT_FORMAT)
                total_act += written
                print(f"[WRITE] activities batch: {written} (total={total_act})")
                act_buffer.clear()

            if i % 1000 == 0:
                print(f"[PROGRESS] fetched {i}/{len(act_ids)} activities")

        # final flush
        if act_buffer:
            written = write_records(act_buffer, act_out, OUTPUT_FORMAT)
            total_act += written
            print(f"[WRITE] activities final: {written} (total={total_act})")

        # Save new cursor (if any)
        if next_cursor:
            put_event_cursor(next_cursor)
            print(f"[CURSOR] saved next_cursor to {CURSOR_S3}")

    print(f"[DONE] total_activities={total_act} total_leads={total_lead}")

# -----------------------
# Entrypoint
# -----------------------
try:
    main()
    job.commit()
except Exception as e:
    print("[FATAL]", e)
    print(traceback.format_exc())
    raise
