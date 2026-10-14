"""
DataForge Pro v6 — bounded, multi-source collection
- Parallel downloads using a bounded ThreadPoolExecutor
- Persistent connection sessions (3x faster than new connections)
- Per-file speed measurement — skips slow servers automatically
- Smart timeouts — HEAD request first to check size/speed
- Chunked streaming with speed throttle detection
- Archive.org direct file API (skip HTML pages)
"""
import os, re, json, time, threading, hashlib, zipfile, uuid, warnings, sqlite3, math, mimetypes, html
from contextlib import closing, contextmanager
import feedparser, requests, pandas as pd
from datetime import datetime
from collections import deque
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib.parse import urlparse, urlunparse, parse_qs, urlencode, quote_plus, urljoin, quote
from flask import Flask, render_template, request, jsonify, send_file
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

warnings.filterwarnings("ignore")

try:
    from ddgs import DDGS; DDGS_OK=True
except: DDGS_OK=False
try:
    from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
    _sia=SentimentIntensityAnalyzer(); VADER_OK=True
except: VADER_OK=False
try:
    from sklearn.feature_extraction.text import TfidfVectorizer
    from sklearn.cluster import KMeans; SK_OK=True
except: SK_OK=False
try:
    from bs4 import BeautifulSoup; BS4_OK=True
except: BS4_OK=False
try:
    import lxml
    LXML_OK=True
except: LXML_OK=False
HTML_PARSER = "lxml" if LXML_OK else "html.parser"

app = Flask(__name__)
JOBS = {}
BASE_DIR   = os.path.dirname(os.path.abspath(__file__))
OUTPUT_DIR = os.path.abspath(os.environ.get("DATAFORGE_OUTPUT_DIR", os.path.join(BASE_DIR, "outputs")))
os.makedirs(OUTPUT_DIR, exist_ok=True)
JOB_DB_PATH = os.environ.get("DATAFORGE_JOB_DB", os.path.join(OUTPUT_DIR, "jobs.sqlite3"))

def _bounded_env_int(name, default, minimum, maximum):
    try:
        return max(minimum, min(maximum, int(os.environ.get(name, default))))
    except (TypeError, ValueError):
        return default

JOB_RUNTIME_SECONDS = _bounded_env_int("DATAFORGE_JOB_SECONDS", 300, 30, 3600)
MAX_ACTIVE_JOBS = _bounded_env_int("DATAFORGE_MAX_ACTIVE_JOBS", 4, 1, 16)
MAX_JOB_BYTES = _bounded_env_int("DATAFORGE_MAX_OUTPUT_MB", 2048, 8, 8192) * 1024 * 1024
FINALIZE_RESERVE_SECONDS = 30
JOB_EXECUTOR = ThreadPoolExecutor(max_workers=MAX_ACTIVE_JOBS, thread_name_prefix="dataforge-job")
JOB_STORE_LOCK = threading.Lock()

def _job_db():
    return sqlite3.connect(JOB_DB_PATH, timeout=10)

def init_job_store():
    os.makedirs(os.path.dirname(JOB_DB_PATH) or ".", exist_ok=True)
    with closing(_job_db()) as conn, conn:
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("""CREATE TABLE IF NOT EXISTS jobs (
            job_id TEXT PRIMARY KEY,
            status TEXT NOT NULL,
            payload TEXT NOT NULL,
            updated_at REAL NOT NULL
        )""")

def persist_job(job_id, job):
    payload = json.dumps(job, ensure_ascii=False)
    with JOB_STORE_LOCK, closing(_job_db()) as conn, conn:
        conn.execute(
            "INSERT INTO jobs(job_id,status,payload,updated_at) VALUES(?,?,?,?) "
            "ON CONFLICT(job_id) DO UPDATE SET status=excluded.status, "
            "payload=excluded.payload, updated_at=excluded.updated_at",
            (job_id, job.get("status", "starting"), payload, time.time()),
        )

def load_job(job_id):
    with closing(_job_db()) as conn, conn:
        row = conn.execute("SELECT payload FROM jobs WHERE job_id=?", (job_id,)).fetchone()
    if not row:
        return None
    try:
        return json.loads(row[0])
    except (TypeError, json.JSONDecodeError):
        return None

def reserve_job(job_id, job):
    """Atomically cap active jobs across web-worker processes on this host."""
    now = time.time()
    with JOB_STORE_LOCK, closing(_job_db()) as conn, conn:
        conn.execute("BEGIN IMMEDIATE")
        stale = conn.execute(
            "SELECT job_id,payload FROM jobs WHERE status IN ('starting','queued','running') AND updated_at<?",
            (now - JOB_RUNTIME_SECONDS - 30,),
        ).fetchall()
        for old_id, payload in stale:
            try:
                old_job = json.loads(payload)
            except (TypeError, json.JSONDecodeError):
                old_job = {}
            old_job.update(status="error", error="Worker stopped before the job finished.")
            conn.execute("UPDATE jobs SET status='error',payload=?,updated_at=? WHERE job_id=?",
                         (json.dumps(old_job, ensure_ascii=False), now, old_id))
        active = conn.execute(
            "SELECT COUNT(*) FROM jobs WHERE status IN ('starting','queued','running')"
        ).fetchone()[0]
        if active >= MAX_ACTIVE_JOBS:
            conn.rollback()
            return False
        conn.execute("INSERT INTO jobs(job_id,status,payload,updated_at) VALUES(?,?,?,?)",
                     (job_id, job["status"], json.dumps(job, ensure_ascii=False), now))
        return True

init_job_store()

# ── BOUNDED THROUGHPUT SETTINGS ──
CRAWL_WORKERS = _bounded_env_int("DATAFORGE_CRAWL_WORKERS", 16, 2, 64)
PARALLEL_WORKERS = _bounded_env_int("DATAFORGE_DOWNLOAD_WORKERS", 16, 2, 64)
MAX_DOWNLOADS_PER_BATCH = _bounded_env_int("DATAFORGE_DOWNLOADS_PER_BATCH", 256, 8, 1000)
MAX_FILE_SIZE_MB = min(_bounded_env_int("DATAFORGE_MAX_FILE_MB", 1024, 16, 4096),
                       max(1,MAX_JOB_BYTES//1024//1024))
MAX_ROUNDS = _bounded_env_int("DATAFORGE_MAX_ROUNDS", 20, 1, 100)
MAX_PAGES_PER_SITE = _bounded_env_int("DATAFORGE_MAX_PAGES_PER_SITE", 5, 1, 20)
MAX_SEEDS_PER_ROUND = _bounded_env_int("DATAFORGE_MAX_SEEDS_PER_ROUND", 200, 8, 2000)
MAX_HOST_CONNECTIONS = _bounded_env_int("DATAFORGE_MAX_HOST_CONNECTIONS", 3, 1, 16)
MAX_EMPTY_ROUNDS = _bounded_env_int("DATAFORGE_EMPTY_ROUNDS", 4, 1, 12)
MAX_SEEDS_PER_DOMAIN = _bounded_env_int("DATAFORGE_MAX_SEEDS_PER_DOMAIN", 6, 1, 50)
SEARCH_QUERIES_PER_ROUND = _bounded_env_int("DATAFORGE_QUERIES_PER_ROUND", 6, 1, 12)
BRAVE_SEARCH_API_KEY = os.environ.get("BRAVE_SEARCH_API_KEY", "").strip()
TAVILY_API_KEY = os.environ.get("TAVILY_API_KEY", "").strip()
MIN_SPEED_KBPS     = 20     # skip servers slower than 20 KB/s
CONNECT_TIMEOUT    = 3      # seconds to establish connection
READ_TIMEOUT       = 8      # seconds between response chunks
CHUNK_SIZE         = 131072 # 128KB chunks (faster than 64KB)
PAGE_TIMEOUT       = 4      # seconds for HTML page fetch

HEADERS = {
    "User-Agent":"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/124 Safari/537.36",
    "Accept-Language":"en-US,en;q=0.9",
    "Accept-Encoding":"gzip, deflate",
    "Connection":"keep-alive",
}

def request_timeout(deadline=None, read_cap=READ_TIMEOUT):
    if deadline is None:
        return CONNECT_TIMEOUT, read_cap
    remaining = deadline - time.monotonic()
    if remaining <= 0:
        raise TimeoutError("Job time limit reached")
    return min(CONNECT_TIMEOUT, remaining), min(read_cap, remaining)

IMAGE_EXTS = {".jpg",".jpeg",".jfif",".png",".gif",".webp",".bmp",".tiff",".avif",".heic",".heif"}
AUDIO_EXTS = {".mp3",".wav",".ogg",".flac",".m4a",".aac",".opus",".aiff",".aif",".wma",".mid",".midi",".amr"}
VIDEO_EXTS = {".mp4",".webm",".avi",".mov",".mkv",".flv",".m4v",".mpeg",".mpg",".3gp",".ts",".m2ts"}
_host_semaphores = {}
_host_semaphores_lock = threading.Lock()

def _host_semaphore(url):
    host=urlparse(url).netloc.lower()
    with _host_semaphores_lock:
        if host not in _host_semaphores:
            _host_semaphores[host]=threading.BoundedSemaphore(MAX_HOST_CONNECTIONS)
        return _host_semaphores[host]

@contextmanager
def host_slot(url):
    """Limit concurrent requests to a single public host."""
    gate=_host_semaphore(url)
    acquired=gate.acquire(timeout=4)
    try:
        yield acquired
    finally:
        if acquired:
            gate.release()

# ── SESSION FACTORY — persistent connections ──
def make_session():
    """Create a session with connection pooling and retry logic."""
    s = requests.Session()
    retry = Retry(total=0, backoff_factor=0,
                  status_forcelist=[500,502,503,504],
                  allowed_methods=["GET","HEAD"])
    adapter = HTTPAdapter(max_retries=retry, pool_connections=20, pool_maxsize=40)
    s.mount("http://", adapter)
    s.mount("https://", adapter)
    s.headers.update(HEADERS)
    return s

# Thread-local sessions — each thread gets its own persistent session
_local = threading.local()
def get_session():
    if not hasattr(_local, "session"):
        _local.session = make_session()
    return _local.session

# ── UTILS ──
def norm_url(url):
    try:
        p=urlparse(url)
        noise={"utm_source","utm_medium","utm_campaign","fbclid","gclid","ref"}
        qs={k:v for k,v in parse_qs(p.query).items() if k not in noise}
        return urlunparse((p.scheme,p.netloc,p.path,p.params,urlencode(qs,doseq=True),"")).rstrip("/")
    except: return url

def valid_url(url):
    if not url or not url.startswith("http"): return False
    try: return urlparse(url).scheme in {"http","https"} and bool(urlparse(url).netloc)
    except: return False

def file_ext(url):
    try: return os.path.splitext(urlparse(url).path.split("?")[0])[1].lower()
    except: return ""

def norm_topic(t):
    t=t.strip()
    t=re.sub(r"^(tell me about|what is|what are|explain|give me|search for|find|show me|i want)\s+","",t,flags=re.I)
    return re.sub(r"[?.!]+$","",t).strip()

def extract_keywords(topic):
    stop={"the","a","an","of","in","on","for","to","and","or","is","are","was","what",
          "how","why","when","where","tell","me","about","give","show","find","want",
          "please","dataset","data","file","files","collect","generate","make","with"}
    words=re.findall(r'\b[a-zA-Z0-9][a-zA-Z0-9_-]{2,}\b',topic.lower())
    return [w for w in words if w not in stop]

def relevance_score(keywords, text):
    """Combine distinct query-term coverage with local TF-IDF cosine similarity."""
    terms=[re.sub(r"[^a-z0-9]+", "", str(k).lower()) for k in (keywords or [])]
    terms=[t for t in terms if len(t)>=3]
    body=re.sub(r"[^a-z0-9]+", " ", str(text or "").lower())[:12000]
    if not terms or not body.strip():
        return 0.0
    tokens=set(body.split())
    weights=[min(2.0, 1.0 + max(0, len(t)-5)*0.12) for t in terms]
    def matched(term):
        variants={term}
        if term.endswith("ies") and len(term)>5: variants.add(term[:-3]+"y")
        if term.endswith("s") and len(term)>4: variants.add(term[:-1])
        if term.endswith("es") and len(term)>5: variants.add(term[:-2])
        if term.endswith("ing") and len(term)>6: variants.add(term[:-3])
        if term.endswith("ed") and len(term)>5: variants.add(term[:-2])
        return any(v in tokens or v in body for v in variants)
    hits=[w for t,w in zip(terms,weights) if matched(t)]
    coverage=sum(hits)/max(sum(weights), 1.0)
    cosine=0.0
    if SK_OK:
        try:
            from sklearn.metrics.pairwise import cosine_similarity
            matrix=TfidfVectorizer(stop_words="english",ngram_range=(1,2),max_features=5000).fit_transform([" ".join(terms),body])
            cosine=float(cosine_similarity(matrix[0:1],matrix[1:2])[0,0])
        except Exception:
            cosine=0.0
    return max(coverage, min(1.0, cosine*1.35))

def is_relevant(keywords, text, min_score=0.2):
    if not keywords: return True
    return relevance_score(keywords,text)>=max(0.0,min(0.95,float(min_score)))

def relevance_cutoff(user_level, stage="page"):
    """Map the dashboard's strictness control to a usable match-score cutoff."""
    level=max(0.0,min(1.0,float(user_level or 0.0)))
    floor={"page":0.08,"text":0.12,"media":0.10,"download":0.10}.get(stage,0.10)
    return min(0.56,floor+level*0.42)

def kaggle_credentials_available():
    env_ready=bool(os.environ.get("KAGGLE_USERNAME") and os.environ.get("KAGGLE_KEY"))
    config_path=os.environ.get("KAGGLE_CONFIG_DIR")
    if config_path:
        config_path=os.path.join(config_path,"kaggle.json")
    else:
        config_path=os.path.join(os.path.expanduser("~"),".kaggle","kaggle.json")
    return env_ready or os.path.isfile(config_path)

def discover_dataset_suggestions(topic, log=None, deadline=None):
    """Search public GitHub/Kaggle catalogs and return a Google catalog link."""
    clean=norm_topic(topic)
    keywords=extract_keywords(clean)
    suggestions=[]
    status=[]
    google_url="https://datasetsearch.research.google.com/search?query="+quote_plus(clean)
    suggestions.append({"source":"Google Dataset Search","title":f"Search Google Dataset Search for: {clean}",
                       "description":"Open the Google dataset catalog for this topic.","url":google_url,
                       "relevance":1.0,"external_id":"google-dataset-search"})

    def github_search():
        headers={"Accept":"application/vnd.github+json","X-GitHub-Api-Version":"2022-11-28"}
        token=os.environ.get("GITHUB_TOKEN","").strip()
        if token: headers["Authorization"]="Bearer "+token
        try:
            remaining=5 if deadline is None else max(1,min(5,int(deadline-time.monotonic())))
            response=get_session().get("https://api.github.com/search/repositories",
                params={"q":f"{clean} dataset in:name,description,readme","sort":"stars","order":"desc","per_page":8},
                headers=headers,timeout=request_timeout(deadline,remaining))
            response.raise_for_status()
            for repo in response.json().get("items",[]):
                title=str(repo.get("full_name") or repo.get("name") or "GitHub dataset")
                desc=str(repo.get("description") or "Public GitHub repository")[:500]
                score=relevance_score(keywords,title+" "+desc+" "+str(repo.get("topics",[])))
                if score>=0.08:
                    suggestions.append({"source":"GitHub","title":title,"description":desc,
                        "url":repo.get("html_url",""),"relevance":round(score,3),
                        "external_id":title,"stars":repo.get("stargazers_count",0)})
        except Exception as exc:
            status.append("GitHub search was unavailable: "+str(exc)[:120])

    def kaggle_search():
        if not kaggle_credentials_available():
            status.append("Kaggle search needs your authorized API credentials (KAGGLE_USERNAME and KAGGLE_KEY, or kaggle.json).")
            return
        try:
            from kaggle.api.kaggle_api_extended import KaggleApi
            api=KaggleApi(); api.authenticate()
            for item in api.dataset_list(search=clean,page=1):
                ref=str(getattr(item,"ref","") or "")
                title=str(getattr(item,"title","") or ref or "Kaggle dataset")
                desc=str(getattr(item,"subtitle","") or "Kaggle dataset")[:500]
                score=relevance_score(keywords,title+" "+desc+" "+ref)
                if ref and score>=0.08:
                    suggestions.append({"source":"Kaggle","title":title,"description":desc,
                        "url":"https://www.kaggle.com/datasets/"+ref,"relevance":round(score,3),
                        "external_id":ref})
        except Exception as exc:
            status.append("Kaggle search failed using the configured credentials: "+str(exc)[:120])

    with ThreadPoolExecutor(max_workers=2) as pool:
        futures=[pool.submit(github_search),pool.submit(kaggle_search)]
        for future in futures:
            try: future.result(timeout=8)
            except Exception as exc: status.append("Dataset catalog search timed out: "+str(exc)[:100])
    # Prefer the best match from each provider, keep the Google catalog link visible.
    external=[x for x in suggestions if x["source"]!="Google Dataset Search"]
    external.sort(key=lambda x:(x["relevance"],x.get("stars",0)),reverse=True)
    final=external[:12]+[suggestions[0]]
    if log:
        log(f"🔎 Dataset catalogs: {len(final)} suggestions available (Google link included)")
        if status:
            log("ℹ️ "+status[0])
    return final,status

def decode_gnews(url):
    if "news.google.com/rss/articles/" not in url: return url
    try:
        import base64
        enc=url.split("/articles/")[-1].split("?")[0]+"=="
        dec=base64.urlsafe_b64decode(enc)
        s=dec.find(b"http")
        if s==-1: return url
        b=dec[s:]
        e=next((i for i,x in enumerate(b) if x<32 or x>126),len(b))
        return b[:e].decode("utf-8",errors="ignore")
    except: return url

def fmt_size(b):
    if b<1024: return f"{b}B"
    if b<1024**2: return f"{b/1024:.1f}KB"
    if b<1024**3: return f"{b/1024**2:.1f}MB"
    return f"{b/1024**3:.2f}GB"

def fmt_speed(bps):
    if bps<1024: return f"{bps:.0f}B/s"
    if bps<1024**2: return f"{bps/1024:.0f}KB/s"
    return f"{bps/1024**2:.1f}MB/s"

# ── ARCHIVE.ORG DIRECT FILE FINDER ──
def get_archive_direct_files(identifier, allowed_exts, log=None, deadline=None):
    """
    Use Archive.org metadata API to get direct download URLs.
    Much faster than scraping HTML pages.
    """
    urls = []
    try:
        s = get_session()
        with host_slot("https://archive.org/metadata/"+identifier) as acquired:
            if not acquired: return urls
            r = s.get(f"https://archive.org/metadata/{identifier}",
                      timeout=request_timeout(deadline))
        r.raise_for_status()
        data = r.json()
        base = f"https://archive.org/download/{identifier}/"
        metadata=data.get("metadata",{})
        item_context=" ".join(str(x) for x in [identifier,metadata.get("title","") ,metadata.get("description","")])[:1500]
        for f in data.get("files", []):
            name = f.get("name","")
            ext = os.path.splitext(name)[1].lower()
            if ext in allowed_exts:
                size = int(f.get("size", 0))
                # Skip files that are too large
                if size > MAX_FILE_SIZE_MB * 1024 * 1024:
                    continue
                urls.append((base + quote(name,safe="/"), item_context+" "+name))
    except: pass
    return urls

# ── SPEED-AWARE DOWNLOADER ──
class TransferBudget:
    """Thread-safe byte allowance for one concurrent media batch."""
    def __init__(self,limit):
        self.limit=max(0,int(limit)); self.used=0; self.lock=threading.Lock()
    def reserve(self,size):
        with self.lock:
            if self.used+size>self.limit: return False
            self.used+=size
            return True
    def release(self,size):
        with self.lock: self.used=max(0,self.used-size)
    @property
    def remaining(self):
        with self.lock: return max(0,self.limit-self.used)

def _download_file_fast(url, dest_folder, allowed_exts, keywords, context="", min_rel=0.2, deadline=None, budget=None):
    """
    Download with speed checking:
    1. Check size when possible without a redundant request for known file URLs
    2. Stream with size and speed measurement
    3. Stop files that exceed the configured limits
    """
    budget_reserved=0
    def rollback_budget():
        nonlocal budget_reserved
        if budget is not None and budget_reserved:
            budget.release(budget_reserved); budget_reserved=0
    try:
        if deadline is not None and time.monotonic() >= deadline:
            return "", 0, "deadline"
        ext = file_ext(url)
        # The user-selected strictness guides filtering but is calibrated so a
        # relevant page or media description can still pass when a URL is opaque.
        combined = urlparse(url).path.replace("-"," ").replace("_"," ")+" "+str(context or "")
        download_cutoff=relevance_cutoff(min_rel,"download")*0.55
        if keywords and not is_relevant(keywords, combined, download_cutoff):
            return "", 0, "not_relevant"

        s = get_session()

        # Skip a separate HEAD round trip for known file extensions. The
        # streaming GET below validates MIME type and content length itself.
        size_header=0; ct=""
        if ext not in allowed_exts:
            try:
                head=s.head(url,timeout=request_timeout(deadline,2),allow_redirects=True)
                size_header=int(head.headers.get("content-length",0))
                if size_header>MAX_FILE_SIZE_MB*1024*1024:
                    head.close()
                    return "",0,f"too_big_{size_header//1024//1024}MB"
                if budget is not None and size_header and size_header>budget.remaining:
                    head.close()
                    return "",0,"batch_cap"
                ct=head.headers.get("content-type","").split(";",1)[0].lower()
                ext=media_extension(ct)
                head.close()
            except:
                size_header=0; ct=""

        # Stream download with speed measurement
        if deadline is not None and time.monotonic() >= deadline:
            return "", 0, "deadline"
        r = s.get(url, timeout=request_timeout(deadline), stream=True)
        r.raise_for_status()
        response_ct=r.headers.get("content-type","").split(";",1)[0].lower()
        if ext not in allowed_exts:
            ext=file_ext(r.url) if file_ext(r.url) in allowed_exts else media_extension(response_ct)
        if ext not in allowed_exts:
            r.close()
            return "",0,"wrong_type"
        # Reject HTML/error pages and type mismatches even if a URL ends in a media extension.
        if response_ct and response_ct not in {"application/octet-stream","binary/octet-stream"}:
            if ext in IMAGE_EXTS and not response_ct.startswith("image/"): r.close(); return "",0,"wrong_ct"
            if ext in AUDIO_EXTS and not response_ct.startswith("audio/"): r.close(); return "",0,"wrong_ct"
            if ext in VIDEO_EXTS and not response_ct.startswith("video/"): r.close(); return "",0,"wrong_ct"

        # Size checks
        size_header = int(r.headers.get("content-length", size_header))
        if ext in IMAGE_EXTS and 0<size_header<512: r.close(); return "",0,"too_small"
        if ext in AUDIO_EXTS and 0<size_header<1024: r.close(); return "",0,"too_small"
        if ext in VIDEO_EXTS and 0<size_header<8192: r.close(); return "",0,"too_small"
        if size_header > MAX_FILE_SIZE_MB*1024*1024: r.close(); return "",0,"too_big"

        fname = f"file_{hashlib.sha256(norm_url(url).encode()).hexdigest()[:16]}{ext}"
        fpath = os.path.join(dest_folder, fname)
        if os.path.exists(fpath):
            r.close()
            return fname, os.path.getsize(fpath), "cached"

        # Download with speed measurement
        total = 0
        speed_check_bytes = 0
        speed_check_start = time.time()
        SPEED_CHECK_INTERVAL = 3  # check speed every 3 seconds

        with open(fpath, "wb") as f:
            for chunk in r.iter_content(CHUNK_SIZE):
                if deadline is not None and time.monotonic() >= deadline:
                    f.close()
                    r.close()
                    try: os.remove(fpath)
                    except OSError: pass
                    rollback_budget()
                    return "", 0, "deadline"
                if not chunk: continue
                if budget is not None:
                    if not budget.reserve(len(chunk)):
                        f.close(); r.close()
                        try: os.remove(fpath)
                        except OSError: pass
                        rollback_budget()
                        return "",0,"batch_cap"
                    budget_reserved+=len(chunk)
                f.write(chunk)
                total += len(chunk)
                speed_check_bytes += len(chunk)

                # Check speed every 3 seconds
                elapsed = time.time() - speed_check_start
                if elapsed >= SPEED_CHECK_INTERVAL:
                    speed_kbps = (speed_check_bytes / elapsed) / 1024
                    if speed_kbps < MIN_SPEED_KBPS and total > CHUNK_SIZE * 2:
                        # Server too slow — delete partial and skip
                        f.close()
                        r.close()
                        try: os.remove(fpath)
                        except: pass
                        rollback_budget()
                        return "", 0, f"too_slow_{speed_kbps:.0f}kbps"
                    speed_check_bytes = 0
                    speed_check_start = time.time()

                # Hard size cap
                if total > MAX_FILE_SIZE_MB * 1024 * 1024:
                    f.close()
                    r.close()
                    try: os.remove(fpath)
                    except OSError: pass
                    rollback_budget()
                    return "", 0, "too_big"

        r.close()
        if total < 512:
            try: os.remove(fpath)
            except: pass
            rollback_budget()
            return "", 0, "empty"

        return fname, total, "ok"

    except Exception as e:
        rollback_budget()
        try:
            for name in os.listdir(dest_folder):
                if name.startswith("file_"+hashlib.sha256(norm_url(url).encode()).hexdigest()[:16]):
                    os.remove(os.path.join(dest_folder,name))
        except: pass
        return "", 0, str(e)[:25]

def download_file_fast(url, dest_folder, allowed_exts, keywords, context="", min_rel=0.2, deadline=None, budget=None):
    """Run a download while respecting the per-host concurrency limit."""
    with host_slot(url) as acquired:
        if not acquired:
            return "", 0, "host_busy"
        return _download_file_fast(url,dest_folder,allowed_exts,keywords,context,min_rel,deadline,budget)

def media_extension(content_type):
    """Map common media MIME types to extensions accepted by the four modes."""
    value=(content_type or "").split(";",1)[0].strip().lower()
    known={"image/jpeg":".jpg","image/jpg":".jpg","image/png":".png","image/gif":".gif",
        "image/webp":".webp","image/bmp":".bmp","image/tiff":".tiff","image/avif":".avif",
        "image/heic":".heic","image/heif":".heif","audio/mpeg":".mp3",
        "audio/mp3":".mp3","audio/wav":".wav","audio/x-wav":".wav","audio/ogg":".ogg",
        "audio/flac":".flac","audio/x-flac":".flac","audio/mp4":".m4a","audio/aac":".aac",
        "audio/opus":".opus","audio/midi":".mid","audio/x-ms-wma":".wma","audio/amr":".amr",
        "video/mp4":".mp4","video/webm":".webm","video/quicktime":".mov","video/mpeg":".mpeg",
        "video/3gpp":".3gp","video/mp2t":".ts",
        "video/x-msvideo":".avi","video/x-matroska":".mkv","video/x-flv":".flv"}
    ext=known.get(value) or mimetypes.guess_extension(value) or ""
    aliases={".jpe":".jpg",".oga":".ogg",".mpga":".mp3"}
    ext=aliases.get(ext,ext)
    return ext

# ── PARALLEL MEDIA DOWNLOADER ──
def download_batch(urls_and_contexts, dest_folder, allowed_exts, keywords, min_rel, log=None, deadline=None, max_total_bytes=None):
    """
    Download multiple files in parallel using ThreadPoolExecutor.
    Returns list of (fname, size, reason) tuples.
    """
    results = []
    if not urls_and_contexts: return results
    budget=TransferBudget(max_total_bytes) if max_total_bytes is not None else None
    deduped=[]; seen=set()
    for url,context in urls_and_contexts:
        key=norm_url(str(url))
        if key and key not in seen:
            deduped.append((url,context)); seen.add(key)
        if len(deduped)>=MAX_DOWNLOADS_PER_BATCH:
            break
    deduped.sort(key=lambda item:relevance_score(keywords,
        urlparse(str(item[0])).path.replace("-"," ").replace("_"," ")+" "+str(item[1] or "")),reverse=True)
    deduped=deduped[:MAX_DOWNLOADS_PER_BATCH]
    urls_and_contexts=deduped
    if not urls_and_contexts: return results

    def dl_one(item):
        url, context = item
        fname, size, reason = download_file_fast(
            url, dest_folder, allowed_exts, keywords, context, min_rel, deadline,budget
        )
        return url, fname, size, reason

    with ThreadPoolExecutor(max_workers=min(PARALLEL_WORKERS, len(urls_and_contexts))) as executor:
        futures = {executor.submit(dl_one, item): item for item in urls_and_contexts}
        for future in as_completed(futures):
            try:
                source_url, fname, size, reason = future.result()
                results.append((source_url, fname, size, reason))
            except Exception as e:
                results.append((futures[future][0], "", 0, str(e)[:20]))
    return results

# ── DISCOVERY ──
def discover(topic, mode="text", max_links=24, log=None, round_num=1, seen_urls=None, deadline=None, source_contexts=None):
    clean = norm_topic(topic)
    keywords = extract_keywords(clean)
    if seen_urls is None: seen_urls = set()
    source_contexts=source_contexts if source_contexts is not None else {}
    query_terms={
        "text":["articles","research papers","case studies","reports","datasets","open data","full text documents","publications","data archive","downloadable resources","news coverage","technical guides","review studies","educational resources","interviews","books and reports","open dataset collections","government data","csv data source","survey results","public research data","developer documentation","historical archives","statistics portal"],
        "image":["photographs","open images","photo collection","image dataset","creative commons images","jpg png images","high resolution pictures","image gallery","public domain photos","illustrations","scientific images","visual dataset","nature photographs","historical pictures","diagram images","artwork collection","open image archive","research image collection","wildlife photos","satellite images","medical image dataset","museum image collection","public photography","visual research data"],
        "audio":["speech recordings","voice recordings","spoken English","English speech corpus","audio dataset","public domain recordings","podcast audio","wav mp3 audio","oral history","audiobook speech","interviews","language recordings","lecture audio","radio recordings","open audio archive","sound collection","open speech dataset","music recordings","field recordings","environmental sounds","public radio archive","audio research corpus","historical audio","voice dataset"],
        "video":["video collection","video dataset","public domain footage","documentary video","video clips","mp4 webm video","lecture recordings","open video archive","news footage","educational videos","nature film","historical footage","sports recordings","scientific video","animation collection","interviews video","open video dataset","research video archive","wildlife footage","public lectures video","museum film archive","video research corpus","public domain movies","educational footage"],
    }
    terms=query_terms.get(mode,query_terms["text"])
    start=((max(1,round_num)-1)*SEARCH_QUERIES_PER_ROUND)%len(terms)
    queries=[f"{clean} {terms[(start+i)%len(terms)]}" for i in range(min(SEARCH_QUERIES_PER_ROUND,len(terms)))]

    found = []
    host_counts = {}
    def add(lst, name):
        n=0
        for candidate in lst:
            if isinstance(candidate,dict):
                raw=str(candidate.get("url") or candidate.get("href") or "").strip()
                context=str(candidate.get("context") or candidate.get("title") or "").strip()
            else:
                raw=str(candidate or "").strip(); context=""
            u2=raw if raw.startswith("archive_id:") else norm_url(raw)
            valid=u2.startswith("archive_id:") or valid_url(u2)
            if not u2 or u2 in seen_urls or not valid or len(found)>=max_links:
                continue
            is_direct=(u2.startswith("archive_id:") or file_ext(u2) in IMAGE_EXTS|AUDIO_EXTS|VIDEO_EXTS)
            host=(urlparse(u2).hostname or "").lower() if not u2.startswith("archive_id:") else ""
            host=host[4:] if host.startswith("www.") else host
            if not is_direct and host_counts.get(host,0)>=MAX_SEEDS_PER_DOMAIN:
                continue
            seen_urls.add(u2); found.append(u2)
            if context: source_contexts[u2]=context
            if not is_direct: host_counts[host]=host_counts.get(host,0)+1
            n+=1
        if log and n>0: log(f"  ✓ {name}: +{n} URLs")

    # Public file catalogs give direct media URLs without restricting the
    # general web search to a hand-picked domain list.
    if mode in {"image","audio","video"}:
        media_type={"image":"image","audio":"audio","video":"movies"}[mode]
        try:
            if deadline is not None and time.monotonic()>=deadline: raise TimeoutError("deadline")
            r=get_session().get("https://archive.org/advancedsearch.php",
                params=[("q",f"{clean} AND mediatype:{media_type}"),
                        ("fl[]","identifier"),("fl[]","title"),("fl[]","description"),
                        ("rows",100),("page",1),("output","json")],
                timeout=request_timeout(deadline,4))
            r.raise_for_status()
            docs=r.json().get("response",{}).get("docs",[])
            add([{"url":"archive_id:"+str(d.get("identifier","")),
                  "context":str(d.get("title", ""))+" "+str(d.get("description", ""))[:600]}
                 for d in docs if d.get("identifier")],"Internet Archive catalog")
        except Exception as exc:
            if log: log(f"  ℹ️ Internet Archive search unavailable ({type(exc).__name__})")

        try:
            params={"action":"query","generator":"search","gsrsearch":clean,
                    "gsrnamespace":6,"gsrlimit":50,"prop":"imageinfo",
                    "iiprop":"url|extmetadata","format":"json","formatversion":2}
            r=get_session().get("https://commons.wikimedia.org/w/api.php",params=params,
                headers={"User-Agent":"DataForgePro/1.0 (public media discovery; contact project owner)"},
                timeout=request_timeout(deadline,4))
            r.raise_for_status()
            pages=r.json().get("query",{}).get("pages",[])
            candidates=[]
            for page in pages:
                info=(page.get("imageinfo") or [{}])[0]
                media_url=info.get("url","")
                ext=file_ext(page.get("title", "")) or file_ext(media_url)
                if not media_url or ext not in {"image":IMAGE_EXTS,"audio":AUDIO_EXTS,"video":VIDEO_EXTS}[mode]:
                    continue
                meta=info.get("extmetadata",{})
                license_name=re.sub(r"<[^>]+>"," ",str((meta.get("LicenseShortName") or {}).get("value","")))
                desc=re.sub(r"<[^>]+>"," ",str((meta.get("ImageDescription") or {}).get("value","")))
                context=html.unescape((str(page.get("title", ""))+" "+desc[:400]+" License: "+license_name).strip())
                candidates.append({"url":media_url,"context":context})
            add(candidates,"Wikimedia Commons")
        except Exception as exc:
            if log: log(f"  ℹ️ Wikimedia Commons search unavailable ({type(exc).__name__})")

    # Search several independent indexes when the owner supplies their keys.
    def ddg_search(q):
        if not DDGS_OK or (deadline is not None and time.monotonic() >= deadline): return []
        try:
            remaining = 3 if deadline is None else max(1, min(3, int(deadline-time.monotonic())))
            with DDGS(timeout=remaining) as d:
                if mode=="image":
                    return [{"url":x.get("url","") or x.get("image",""),"context":x.get("title","")} for x in d.images(q,max_results=50,safesearch="moderate")]
                return [{"url":x.get("href","") or x.get("url",""),"context":str(x.get("title",""))+" "+str(x.get("body",""))} for x in d.text(q,max_results=50,safesearch="off")]
        except Exception: return []

    def brave_search(q):
        r=get_session().get("https://api.search.brave.com/res/v1/web/search",
            headers={"X-Subscription-Token":BRAVE_SEARCH_API_KEY},
            params={"q":q,"count":20,"search_lang":"en"},timeout=request_timeout(deadline,6))
        r.raise_for_status()
        return [{"url":x.get("url",""),"context":str(x.get("title",""))+" "+str(x.get("description",""))}
                for x in r.json().get("web",{}).get("results",[])]

    def tavily_search(q):
        r=get_session().post("https://api.tavily.com/search",
            headers={"Authorization":"Bearer "+TAVILY_API_KEY},
            json={"query":q,"search_depth":"basic","max_results":20,
                  "include_answer":False,"include_raw_content":False},
            timeout=request_timeout(deadline,8))
        r.raise_for_status()
        return [{"url":x.get("url",""),"context":str(x.get("title",""))+" "+str(x.get("content",""))}
                for x in r.json().get("results",[])]

    providers=[("DDG",ddg_search)]
    if BRAVE_SEARCH_API_KEY: providers.append(("Brave",brave_search))
    if TAVILY_API_KEY: providers.append(("Tavily",tavily_search))
    tasks=[(name,query,search_fn) for query in queries for name,search_fn in providers]
    with ThreadPoolExecutor(max_workers=min(12,len(tasks))) as ex:
        futs = {ex.submit(search_fn,q):(name,q) for name,q,search_fn in tasks}
        for fut in as_completed(futs):
            provider,query=futs[fut]
            try: add(fut.result(),f"{provider}:{query[:25]}")
            except Exception as exc:
                if log: log(f"  ℹ️ {provider} search unavailable ({type(exc).__name__})")

    if mode=="text":
        try:
            if deadline is not None and time.monotonic() >= deadline: raise TimeoutError("deadline")
            s=get_session()
            r=s.get(f"https://news.google.com/rss/search?q={quote_plus(clean)}&hl=en",timeout=request_timeout(deadline,3))
            r.raise_for_status()
            feed=feedparser.parse(r.content)
            add([decode_gnews(e.get("link","")) for e in feed.entries[:50]],"Google News")
        except: pass
        try:
            if deadline is not None and time.monotonic() >= deadline: raise TimeoutError("deadline")
            s=get_session()
            r=s.get(f"https://api.gdeltproject.org/api/v2/doc/doc?query={quote_plus(clean)}&mode=artlist&maxrecords=250&format=json",timeout=request_timeout(deadline,3))
            add([a.get("url","") for a in r.json().get("articles",[])],"GDELT")
        except: pass

    if mode=="audio":
        try:
            if deadline is not None and time.monotonic() >= deadline: raise TimeoutError("deadline")
            s=get_session()
            r=s.get(f"https://freesound.org/search/?q={quote_plus(clean)}&f=type:mp3",timeout=request_timeout(deadline,3))
            if BS4_OK:
                soup=BeautifulSoup(r.text,HTML_PARSER)
                add([urljoin("https://freesound.org",a["href"]) for a in soup.select("a[href*='/sounds/']") if a.get("href")],"Freesound.org")
        except: pass

    if log: log(f"  → Round {round_num}: {len(found)} fresh sources from {len(host_counts)} web hosts")
    return found[:max_links], clean, keywords

# ── FAST PAGE SCRAPER ──
def _scrape_page_fast(url, mode, dest, allowed_exts, keywords, visited, base_domain, q, min_rel, deadline=None):
    """Single page scrape — fast version with short timeout."""
    result = {"url":url,"title":"","text":"","media_urls":[],"filtered":0,"bytes":0,"relevance_score":0.0}
    ext=file_ext(url)
    if ext in allowed_exts:
        result["media_urls"].append((url,urlparse(url).path.replace("-"," ").replace("_"," ")))
        return result
    resp = None
    try:
        if deadline is not None and time.monotonic() >= deadline:
            return result
        s = get_session()
        resp = s.get(url, timeout=request_timeout(deadline, PAGE_TIMEOUT), stream=True)
        ct = resp.headers.get("content-type","").lower()
        ext = file_ext(url)
        direct_media=(ext in allowed_exts or
                      (mode=="image" and ct.startswith("image/")) or
                      (mode=="audio" and ct.startswith("audio/")) or
                      (mode=="video" and ct.startswith("video/")))
        if direct_media:
            resp.close()
            result["media_urls"].append((url, urlparse(url).path.replace("-"," ").replace("_"," ")+" "+result["title"]))
            return result
        if "text/html" not in ct:
            resp.close()
            return result
        resp.raise_for_status()
        page_bytes = bytearray()
        max_html_bytes = 1_500_000
        for chunk in resp.iter_content(65536):
            if deadline is not None and time.monotonic() >= deadline:
                break
            if not chunk:
                continue
            page_bytes.extend(chunk[:max_html_bytes-len(page_bytes)])
            if len(page_bytes) >= max_html_bytes:
                break
        resp.close()
    except:
        if resp is not None:
            resp.close()
        return result
    if not page_bytes or (deadline is not None and time.monotonic() >= deadline):
        return result
    if not BS4_OK: return result

    soup = BeautifulSoup(bytes(page_bytes), HTML_PARSER)
    if soup.title: result["title"] = (soup.title.string or "").strip()
    for tag in soup(["script","style","nav","footer","header","aside","noscript"]): tag.decompose()
    page_text = soup.get_text(" ", strip=True)[:6000]
    page_score=relevance_score(keywords,page_text)
    result["relevance_score"]=page_score
    page_rel = not keywords or page_score>=relevance_cutoff(min_rel,"page")

    if mode == "text":
        paras = [p.get_text(" ").strip() for p in soup.find_all(["p","h1","h2","h3","article","li"])]
        text = " ".join(p for p in paras if len(p)>30)[:50000]
        text_score=relevance_score(keywords,result["title"]+" "+text)
        result["relevance_score"]=text_score
        if keywords and text_score<relevance_cutoff(min_rel,"text")*0.72:
            result["filtered"] += 1
        else:
            result["text"] = text
            result["bytes"] = len(text.encode())

    elif mode == "image":
        for img in soup.find_all("img"):
            src = img.get("src") or img.get("data-src") or img.get("data-original") or ""
            srcset=img.get("srcset","")
            if not src and srcset:
                src=srcset.split(",")[-1].strip().split(" ")[0]
            if not src: continue
            img_url = urljoin(url, src)
            ext2 = file_ext(img_url)
            if ext2 and ext2 not in allowed_exts: continue
            alt = img.get("alt","").strip()
            parent_text = (img.parent.get_text(" ")[:200] if img.parent else "")
            ctx = alt+" "+parent_text+" "+result["title"]
            ctx += " "+page_text[:500]
            if keywords and not page_rel and relevance_score(keywords,ctx)<relevance_cutoff(min_rel,"media")*0.45:
                result["filtered"] += 1; continue
            result["media_urls"].append((img_url, ctx))
        for meta in soup.find_all("meta",attrs={"property":re.compile(r"^og:image",re.I)}):
            src=meta.get("content","").strip()
            if not src: continue
            image_url=urljoin(url,src)
            ext2=file_ext(image_url)
            if ext2 and ext2 not in allowed_exts: continue
            ctx=(meta.get("alt","")+" "+result["title"]+" "+page_text[:500])
            if (page_rel or not keywords or relevance_score(keywords,ctx)>=relevance_cutoff(min_rel,"media")*0.45):
                result["media_urls"].append((image_url,ctx))

    elif mode in ("audio","video"):
        found = {}
        for tag in soup.find_all(["audio","video","source"]):
            src = tag.get("src","") or tag.get("data-src","")
            if src: found[urljoin(url,src)] = (tag.get("title","")+" "+result["title"]+" "+page_text[:500])
            srcset=tag.get("srcset","")
            for candidate in srcset.split(","):
                candidate=candidate.strip().split(" ")[0]
                if candidate: found[urljoin(url,candidate)]=result["title"]+" "+page_text[:500]
        for a in soup.find_all("a", href=True):
            full = urljoin(url, a["href"])
            if file_ext(full) in allowed_exts:
                found[full] = a.get_text()[:80]+" "+result["title"]+" "+page_text[:500]
        if not page_rel and not found: result["filtered"]+=1; return result
        result["media_urls"] = list(found.items())

    # Follow internal links
    for a in soup.find_all("a", href=True):
        href = a.get("href","").strip()
        if not href or href.startswith("#") or href.startswith("mailto:"): continue
        full = norm_url(urljoin(url, href))
        if valid_url(full) and urlparse(full).netloc==base_domain and full not in visited:
            q.append(full)
    return result

def scrape_page_fast(url, mode, dest, allowed_exts, keywords, visited, base_domain, q, min_rel, deadline=None):
    """Scrape a page with a per-host concurrency limit."""
    with host_slot(url) as acquired:
        if not acquired:
            return {"url":url,"title":"","text":"","media_urls":[],"filtered":0,"bytes":0,"relevance_score":0.0}
        return _scrape_page_fast(url,mode,dest,allowed_exts,keywords,visited,base_domain,q,min_rel,deadline)

# ── NLP ──
def scrape_seed_pages(seed, mode, dest, allowed_exts, keywords, min_rel, max_pages, deadline, seed_context=""):
    """Scrape one seed and a small same-host frontier for parallel round processing."""
    domain=urlparse(seed).netloc
    queue=deque([seed]); local_seen=set(); results=[]
    started=time.monotonic()
    while queue and len(results)<max_pages and time.monotonic()<deadline and time.monotonic()-started<20:
        url=queue.popleft()
        if url in local_seen:
            continue
        local_seen.add(url)
        try:
            result=scrape_page_fast(url,mode,dest,allowed_exts,keywords,
                                    local_seen,domain,queue,min_rel,deadline)
        except Exception:
            result={"url":url,"title":"","text":"","media_urls":[],"filtered":0,"bytes":0}
        if seed_context and result.get("media_urls"):
            result["media_urls"]=[(media_url,(str(context)+" "+seed_context).strip())
                                  for media_url,context in result["media_urls"]]
        results.append((url,domain,result))
    return results

def sentiment(text):
    if not VADER_OK: return "NEUTRAL",0.0,0.0,0.0,0.0
    s=_sia.polarity_scores(text); c=s["compound"]
    return ("POSITIVE" if c>=0.05 else "NEGATIVE" if c<=-0.05 else "NEUTRAL"),round(c,4),round(s["pos"],4),round(s["neg"],4),round(s["neu"],4)

def cluster_texts(texts,k=5):
    if not SK_OK or len(texts)<max(2,k): return [0]*len(texts),{i:f"topic_{i}" for i in range(k)}
    try:
        v=TfidfVectorizer(max_features=5000,stop_words="english",ngram_range=(1,2))
        X=v.fit_transform(texts); k2=min(k,len(texts))
        km=KMeans(n_clusters=k2,random_state=42,n_init=10); lbs=km.fit_predict(X)
        terms=v.get_feature_names_out()
        names={i:"_".join(terms[j] for j in c.argsort()[-3:][::-1]) for i,c in enumerate(km.cluster_centers_)}
        return lbs.tolist(),names
    except: return [0]*len(texts),{}

# ── MAIN PIPELINE ──
def run_pipeline(job_id, topic, mode, num_clusters, delay, formats, text_formats, min_relevance, target_gb):
    job=JOBS[job_id]; logs=job["logs"]
    def log(msg):
        logs.append({"ts":datetime.now().strftime("%H:%M:%S"),"msg":msg})
        if len(logs)>500: del logs[:-500]
        job["last_update"]=time.time()
        try: persist_job(job_id,job)
        except Exception: pass

    try:
        target_bytes = int(target_gb * 1024**3)
        start_time=time.time()
        deadline_at=job.get("deadline_at",start_time+JOB_RUNTIME_SECONDS)
        deadline=time.monotonic()+max(0,deadline_at-time.time())
        work_deadline=deadline-FINALIZE_RESERVE_SECONDS
        job["status"]="running"; job["target_bytes"]=target_bytes; job["target_gb"]=target_gb
        job["started_at"]=start_time; job["timed_out"]=False; job["size_limited"]=False
        persist_job(job_id,job)

        log(f"🚀 DataForge Pro — {mode.upper()} collection started")
        log(f"⏱️ Output window: up to {JOB_RUNTIME_SECONDS} seconds; results are packaged at the deadline")
        log(f"🎯 Size target: up to {target_gb} GB (bounded by the run time and collection cap)")
        log(f"⚡ {CRAWL_WORKERS} page workers + {PARALLEL_WORKERS} download workers | {MAX_HOST_CONNECTIONS} requests/host max")
        log(f"📦 Output cap: {MAX_JOB_BYTES//1024//1024}MB/job | {MAX_FILE_SIZE_MB}MB/file")
        log(f"🔍 Relevance filter: {int(min_relevance*100)}%")

        search_sources=[]
        if DDGS_OK: search_sources.append("DuckDuckGo")
        if BRAVE_SEARCH_API_KEY: search_sources.append("Brave API")
        if TAVILY_API_KEY: search_sources.append("Tavily API")
        if mode in {"image","audio","video"}: search_sources.extend(["Internet Archive","Wikimedia Commons"])
        if mode=="audio": search_sources.append("Freesound")
        if mode=="text": search_sources.extend(["Google News RSS","GDELT"])
        log(f"Search sources: {', '.join(search_sources) if search_sources else 'none available'} | {SEARCH_QUERIES_PER_ROUND} queries/round")

        allowed_exts=(set(formats) if formats else
                      {"image":IMAGE_EXTS,"audio":AUDIO_EXTS,"video":VIDEO_EXTS}.get(mode,set()))
        if mode!="text": log(f"📁 Formats: {', '.join(sorted(allowed_exts))}")

        safe=re.sub(r"[^a-zA-Z0-9_]","_",norm_topic(topic))[:25]
        ts=datetime.now().strftime("%Y%m%d_%H%M%S")
        job_folder=os.path.join(OUTPUT_DIR,f"{mode}_{safe}_{ts}")
        os.makedirs(job_folder,exist_ok=True)
        media_folder=None
        if mode!="text":
            mname={"image":"images","audio":"audio","video":"videos"}[mode]
            media_folder=os.path.join(job_folder,mname)
            os.makedirs(media_folder,exist_ok=True)

        text_rows=[]; media_rows=[]; visited=set(); source_hosts=set()
        total_pages=0; total_media=0; total_filtered=0; total_skipped=0
        collected_bytes=0; seen_urls=set(); seen_media_files=set()
        sources_exhausted=False; timed_out=False; size_limited=False
        suggestions,catalog_status=discover_dataset_suggestions(topic,log,work_deadline)
        job["suggestions"]=suggestions
        job["catalog_status"]=catalog_status
        persist_job(job_id,job)

        # ── TARGET LOOP ──
        round_num=0
        while collected_bytes < target_bytes and not size_limited:
            if time.monotonic() >= work_deadline:
                timed_out=True
                break
            round_num+=1
            if round_num>MAX_ROUNDS:
                log(f"ℹ️ Discovery round limit reached ({MAX_ROUNDS}).")
                log(f"📊 Collected: {fmt_size(collected_bytes)} of {target_gb}GB target.")
                break

            log(f"")
            log(f"🔄 ROUND {round_num} — discovering fresh sources...")
            source_contexts={}
            seeds,clean,keywords=discover(topic,mode,MAX_SEEDS_PER_ROUND*4,log,round_num,
                                          seen_urls,work_deadline,source_contexts)

            if not seeds:
                consecutive=job.get("cons_empty",0)+1; job["cons_empty"]=consecutive
                if consecutive>=MAX_EMPTY_ROUNDS:
                    log(f"")
                    log(f"ℹ️ Configured search indexes returned no new URLs for {consecutive} rounds.")
                    log(f"📊 Maximum: {fmt_size(collected_bytes)} of {target_gb}GB.")
                    log(f"💡 Add a Brave/Tavily API key or adjust the search topic to widen discovery.")
                    sources_exhausted=True; break
                log(f"⚠️ No new sources this round, retrying...")
                time.sleep(min(.25,max(0,work_deadline-time.monotonic())))
                continue
            else:
                job["cons_empty"]=0

            seeds=seeds[:MAX_SEEDS_PER_ROUND]
            log(f"✅ {len(seeds)} fresh sources to process with up to {CRAWL_WORKERS} page workers")

            # Fetch Archive.org metadata and web pages concurrently, then merge
            # the results in this thread so counters and output rows stay safe.
            archive_seeds=[s for s in seeds if s.startswith("archive_id:")]
            source_seeds=[s for s in seeds if not s.startswith("archive_id:")]
            download_context={}
            if archive_seeds:
                with ThreadPoolExecutor(max_workers=min(CRAWL_WORKERS,len(archive_seeds))) as pool:
                    futures={pool.submit(get_archive_direct_files,s.replace("archive_id:",""),allowed_exts,None,work_deadline):s for s in archive_seeds}
                    for future in as_completed(futures):
                        seed=futures[future]; identifier=seed.replace("archive_id:","")
                        try: direct_files=future.result()
                        except Exception: direct_files=[]
                        for source_url,context in direct_files:
                            download_context.setdefault(norm_url(source_url),
                                (source_url,str(context),f"https://archive.org/details/{identifier}","archive.org"))

            page_results=[]
            if source_seeds and time.monotonic()<work_deadline:
                with ThreadPoolExecutor(max_workers=min(CRAWL_WORKERS,len(source_seeds))) as pool:
                    futures={pool.submit(scrape_seed_pages,seed,mode,media_folder or "",allowed_exts,
                                         keywords,min_relevance,MAX_PAGES_PER_SITE,work_deadline,
                                         source_contexts.get(seed,"")):seed
                             for seed in source_seeds}
                    completed=0
                    for future in as_completed(futures):
                        completed+=1
                        try: page_results.extend(future.result())
                        except Exception: pass
                        if completed%10==0 or completed==len(futures):
                            job["scraped"]=total_pages+len(page_results)
                            job["progress"]=min(99,max(job.get("progress",0),
                                int((time.time()-start_time)/max(JOB_RUNTIME_SECONDS,1)*100)))
                            persist_job(job_id,job)

            round_candidates=0
            for url,domain,res in page_results:
                if url in visited:
                    continue
                visited.add(url)
                host=(domain or "").lower()
                host=host[4:] if host.startswith("www.") else host
                if host: source_hosts.add(host)
                total_pages+=1
                total_filtered+=res.get("filtered",0)
                if mode=="text" and len(res.get("text",""))>100:
                    bytes_added=res.get("bytes",0)
                    if collected_bytes+bytes_added>MAX_JOB_BYTES:
                        size_limited=True
                        continue
                    sl,ss,sp,sn,snu=sentiment(res["text"])
                    text_rows.append({
                        "id":len(text_rows)+1,"topic":clean,"url":url,"domain":domain,
                        "title":res["title"],"text":res["text"],
                        "relevance_score":round(res.get("relevance_score",0.0),3),
                        "word_count":len(res["text"].split()),"scraped_at":datetime.now().isoformat(),
                        "sentiment_label":sl,"sentiment_score":ss,"sentiment_pos":sp,
                        "sentiment_neg":sn,"sentiment_neu":snu,"topic_cluster_id":0,"topic_label":"",
                    })
                    collected_bytes+=bytes_added
                elif mode!="text":
                    for media_url,context in res.get("media_urls",[]):
                        key=norm_url(str(media_url))
                        if key:
                            download_context.setdefault(key,(str(media_url),str(context or ""),url,domain))
                            round_candidates+=1

            if mode!="text" and download_context and time.monotonic()<work_deadline:
                download_items=[(item[0],item[1]) for item in download_context.values()]
                log(f"⬇️ Processing up to {min(len(download_items),MAX_DOWNLOADS_PER_BATCH)} relevant media candidates")
                dl_results=download_batch(download_items,media_folder or "",allowed_exts,keywords,
                                          min_relevance,log,work_deadline,
                                          max_total_bytes=max(0,min(MAX_JOB_BYTES-collected_bytes,target_bytes-collected_bytes)))
                for source_url,fname,size,reason in dl_results:
                    if fname and size>0:
                        if fname in seen_media_files:
                            continue
                        if collected_bytes+size>MAX_JOB_BYTES:
                            try: os.remove(os.path.join(media_folder,fname))
                            except OSError: pass
                            size_limited=True
                            continue
                        seen_media_files.add(fname)
                        context,page_url,domain=download_context.get(norm_url(source_url),(source_url,"","",""))[1:]
                        if domain: source_hosts.add(str(domain).lower().removeprefix("www."))
                        media_rows.append({
                            "id":len(media_rows)+1,"topic":clean,"page_url":page_url,"domain":domain,
                            "file_url":source_url,"filename":fname,"format":file_ext(fname),
                            "description":context[:500],"relevance_score":round(relevance_score(keywords,context),3),
                            "file_size_bytes":size,"downloaded_at":datetime.now().isoformat(),
                        })
                        total_media+=1; collected_bytes+=size
                    elif "too_slow" in reason or "too_big" in reason:
                        total_skipped+=1

            job["collected_bytes"]=collected_bytes
            job["scraped"]=total_pages
            job["sources"]=len(source_hosts)
            byte_progress=int(collected_bytes/max(target_bytes,1)*100)
            time_progress=int((time.time()-start_time)/max(JOB_RUNTIME_SECONDS,1)*100)
            job["progress"]=min(99,max(byte_progress,time_progress))
            job["timed_out"]=timed_out; job["size_limited"]=size_limited
            persist_job(job_id,job)
            if delay>0 and time.monotonic()<work_deadline:
                time.sleep(min(delay,work_deadline-time.monotonic()))

            if collected_bytes>=MAX_JOB_BYTES and collected_bytes<target_bytes:
                size_limited=True
            # Speed report per round
            elapsed=time.time()-start_time
            avg_speed=fmt_speed(collected_bytes/max(elapsed,1))
            log(f"📊 Round {round_num} done | {fmt_size(collected_bytes)}/{target_gb}GB | avg speed: {avg_speed} | skipped slow: {total_skipped}")

        if time.monotonic() >= work_deadline and collected_bytes < target_bytes:
            timed_out=True
        if collected_bytes >= MAX_JOB_BYTES and collected_bytes < target_bytes:
            size_limited=True
        if timed_out:
            log(f"⏱️ Time limit reached. Saving the partial dataset now.")
        if size_limited:
            log(f"📦 Collection size cap reached. Saving the collected files now.")
        job["timed_out"]=timed_out; job["size_limited"]=size_limited
        job["internet_exhausted"]=False
        job["sources_exhausted"]=sources_exhausted

        # ── SAVE ──
        log(f""); log(f"💾 Saving dataset...")
        stats_domains={}

        if mode=="text":
            if text_rows:
                log(f"🧩 Clustering {len(text_rows)} articles into {num_clusters} topics...")
            else:
                log("ℹ️ No readable articles were found; creating an empty dataset package.")
            txts=[r["text"] for r in text_rows]
            cids,cnames=cluster_texts(txts,k=num_clusters)
            for i,r in enumerate(text_rows):
                r["topic_cluster_id"]=cids[i]; r["topic_label"]=cnames.get(cids[i],f"topic_{cids[i]}")
            text_columns=["id","topic","url","domain","title","text","relevance_score","word_count","scraped_at",
                          "sentiment_label","sentiment_score","sentiment_pos","sentiment_neg","sentiment_neu",
                          "topic_cluster_id","topic_label"]
            df=pd.DataFrame(text_rows,columns=text_columns)
            if "csv" in text_formats or not text_formats:
                p=os.path.join(job_folder,"text_dataset.csv"); df.to_csv(p,index=False)
                log(f"  ✓ text_dataset.csv — {len(df)} rows | {os.path.getsize(p)/(1024**2):.1f}MB")
            if "json" in text_formats or not text_formats:
                p=os.path.join(job_folder,"text_dataset.json"); df.to_json(p,orient="records",indent=2,force_ascii=False)
                log(f"  ✓ text_dataset.json — {os.path.getsize(p)/(1024**2):.1f}MB")
            p=os.path.join(job_folder,"text_dataset.jsonl")
            with open(p,"w",encoding="utf-8") as f:
                for row in text_rows: f.write(json.dumps(row,ensure_ascii=False)+"\n")
            log(f"  ✓ text_dataset.jsonl — HuggingFace ready")
            stats_domains=df["domain"].value_counts().head(5).to_dict()
            job["stats"]={
                "pages":len(text_rows),"words":int(df["word_count"].sum()),
                "size_mb":round(collected_bytes/1e6,2),"zip_mb":0,
                "sentiment_pos":int((df["sentiment_label"]=="POSITIVE").sum()),
                "sentiment_neg":int((df["sentiment_label"]=="NEGATIVE").sum()),
                "sentiment_neu":int((df["sentiment_label"]=="NEUTRAL").sum()),
                "filtered":total_filtered,"top_domains":stats_domains,
                "unique_sources":len(source_hosts),
                "rounds":round_num,"sources_exhausted":sources_exhausted,
                "timed_out":timed_out,"size_limited":size_limited,
                "elapsed_seconds":round(time.time()-start_time,1),
            }
        else:
            media_columns=["id","topic","page_url","domain","file_url","filename","format",
                           "description","relevance_score","file_size_bytes","downloaded_at"]
            df=pd.DataFrame(media_rows,columns=media_columns)
            mname={"image":"image_dataset","audio":"audio_dataset","video":"video_dataset"}[mode]
            df.to_csv(os.path.join(job_folder,f"{mname}.csv"),index=False)
            df.to_json(os.path.join(job_folder,f"{mname}.json"),orient="records",indent=2,force_ascii=False)
            log(f"  ✓ {mname}.csv + .json — {len(df)} files")
            if not df.empty:
                stats_domains=df["domain"].value_counts().head(5).to_dict()
            actual=len(os.listdir(media_folder)) if media_folder and os.path.exists(media_folder) else 0
            log(f"  ✓ {actual} {mode} files downloaded | {total_skipped} slow/big files skipped")
            job["stats"]={
                "pages":total_pages,
                "images":actual if mode=="image" else 0,
                "audio_files":actual if mode=="audio" else 0,
                "video_files":actual if mode=="video" else 0,
                "size_mb":round(collected_bytes/(1024**2),2),"zip_mb":0,
                "filtered":total_filtered,"top_domains":stats_domains,
                "unique_sources":len(source_hosts),
                "rounds":round_num,"sources_exhausted":sources_exhausted,
                "timed_out":timed_out,"size_limited":size_limited,
                "elapsed_seconds":round(time.time()-start_time,1),
            }

        suggestion_rows=job.get("suggestions",[])
        pd.DataFrame(suggestion_rows,columns=["source","title","description","url","relevance","external_id"]).to_csv(
            os.path.join(job_folder,"dataset_suggestions.csv"),index=False)
        with open(os.path.join(job_folder,"dataset_suggestions.json"),"w",encoding="utf-8") as suggestion_file:
            json.dump(suggestion_rows,suggestion_file,ensure_ascii=False,indent=2)

        log(f"📦 Creating ZIP..."); 
        zip_name=f"{mode}_{safe}_{ts}_{job_id}.zip"; zip_path=os.path.join(OUTPUT_DIR,zip_name)
        with zipfile.ZipFile(zip_path,"w",compression=zipfile.ZIP_DEFLATED,compresslevel=1) as zf:
            for root,dirs,files in os.walk(job_folder):
                for f in files:
                    fp=os.path.join(root,f); ext=file_ext(fp)
                    codec=zipfile.ZIP_STORED if ext in IMAGE_EXTS|AUDIO_EXTS|VIDEO_EXTS else zipfile.ZIP_DEFLATED
                    zf.write(fp,os.path.relpath(fp,OUTPUT_DIR),compress_type=codec,
                             compresslevel=1 if codec==zipfile.ZIP_DEFLATED else None)
        zip_mb=os.path.getsize(zip_path)/(1024**2)
        if job["stats"]: job["stats"]["zip_mb"]=round(zip_mb,2)
        job["status"]="done"; job["filename"]=zip_name
        job["filepath"]=zip_path; job["progress"]=100
        job["internet_exhausted"]=False
        job["sources_exhausted"]=sources_exhausted
        job["timed_out"]=timed_out; job["size_limited"]=size_limited
        elapsed_total=time.time()-start_time
        avg_speed=fmt_speed(collected_bytes/max(elapsed_total,1))

        log(f"📊 FINAL: {fmt_size(collected_bytes)} collected in {elapsed_total:.1f}s | avg {avg_speed}")
        log(f"📦 ZIP: {zip_name} ({zip_mb:.1f}MB) | download is ready")
        persist_job(job_id,job)
        JOBS.pop(job_id, None)

    except Exception as e:
        import traceback
        job["error"]=str(e)[:300]
        log(f"❌ {str(e)}")
        log(traceback.format_exc()[:500])
        job["status"]="error"
        try: persist_job(job_id,job)
        except Exception: pass
        JOBS.pop(job_id, None)

# ── ROUTES ──
@app.route("/")
def landing(): return render_template("landing.html")

@app.route("/app")
def dashboard(): return render_template("app.html", job_runtime_seconds=JOB_RUNTIME_SECONDS)

@app.route("/healthz")
def healthz():
    return jsonify({
        "status":"ok" if BS4_OK else "degraded",
        "active_jobs":MAX_ACTIVE_JOBS,
        "capabilities":{"html_parser":BS4_OK,"search":DDGS_OK,
                         "brave_search_api":bool(BRAVE_SEARCH_API_KEY),
                         "tavily_search_api":bool(TAVILY_API_KEY),
                         "internet_archive_catalog":True,"wikimedia_commons_catalog":True,
                         "sentiment":VADER_OK,"clustering":SK_OK,
                         "github_catalog":True,"kaggle_catalog":kaggle_credentials_available(),
                         "google_dataset_search_link":True},
    })

@app.route("/api/start",methods=["POST"])
def start():
    d=request.get_json(silent=True)
    if not isinstance(d,dict):
        return jsonify({"error":"Send the job options as JSON."}),400
    topic=d.get("topic","")
    topic=topic.strip() if isinstance(topic,str) else ""
    if not topic: return jsonify({"error":"Topic required"}),400
    if len(topic)>300: return jsonify({"error":"Topic must be 300 characters or less."}),400
    mode=d.get("mode","text")
    if not isinstance(mode,str) or mode not in {"text","image","audio","video"}:
        return jsonify({"error":"Mode must be text, image, audio, or video."}),400
    missing=[]
    if not BS4_OK:
        missing.append("beautifulsoup4")
    if not DDGS_OK:
        missing.append("ddgs")
    if missing:
        return jsonify({
            "error":"Missing required module(s): "+", ".join(missing)+". Install the project requirements with: python -m pip install -r requirements.txt"
        }),503
    try:
        clusters=max(2,min(15,int(d.get("num_clusters",5))))
        raw_min_rel=float(d.get("min_relevance",0.3))
        if not math.isfinite(raw_min_rel):
            raise ValueError("Relevance must be finite.")
        min_rel=max(0.0,min(1.0,raw_min_rel))
        target_gb=float(d.get("target_gb",1.0))
    except (TypeError,ValueError):
        return jsonify({"error":"Invalid cluster count, relevance, or target size."}),400
    if not math.isfinite(target_gb) or not 0.01 <= target_gb <= 10000:
        return jsonify({"error":"Target size must be between 0.01 and 10,000 GB."}),400
    delay=0.0
    ext_map={"image":IMAGE_EXTS,"audio":AUDIO_EXTS,"video":VIDEO_EXTS}
    raw_formats=d.get("formats",[])
    if not isinstance(raw_formats,list): raw_formats=[]
    formats=sorted(set(str(x).lower() for x in raw_formats)&ext_map.get(mode,set()))
    raw_text_formats=d.get("text_formats",["csv","json","jsonl"])
    if not isinstance(raw_text_formats,list): raw_text_formats=[]
    text_formats=sorted(set(str(x).lower() for x in raw_text_formats)&{"csv","json","jsonl"})
    jid=uuid.uuid4().hex[:12]
    now=time.time()
    job={
        "status":"queued","logs":[],"progress":0,"scraped":0,
        "total_links":0,"filename":None,"filepath":None,"stats":None,
        "last_update":now,"created_at":now,"deadline_at":now+JOB_RUNTIME_SECONDS,
        "time_limit_seconds":JOB_RUNTIME_SECONDS,"timed_out":False,"size_limited":False,
        "target_gb":target_gb,
        "target_bytes":int(target_gb*1024**3),
        "collected_bytes":0,"sources":0,"internet_exhausted":False,"sources_exhausted":False,"cons_empty":0,"error":None,
        "suggestions":[],"catalog_status":[],
    }
    if not reserve_job(jid,job):
        return jsonify({"error":f"The service is busy. Try again when one of the {MAX_ACTIVE_JOBS} active jobs finishes."}),429
    JOBS[jid]=job
    try:
        JOB_EXECUTOR.submit(run_pipeline,jid,topic,mode,clusters,delay,formats,text_formats,min_rel,target_gb)
    except Exception as exc:
        job.update(status="error",error=str(exc)[:300])
        persist_job(jid,job)
        JOBS.pop(jid, None)
        return jsonify({"error":"Could not queue the collection job."}),503
    return jsonify({"job_id":jid,"time_limit_seconds":JOB_RUNTIME_SECONDS}),202

@app.route("/api/status/<jid>")
def status(jid):
    j=load_job(jid)
    if not j: return jsonify({"error":"Not found"}),404
    return jsonify({
        "status":j["status"],"progress":j["progress"],
        "scraped":j.get("scraped",0),"sources":j.get("sources",0),"logs":j.get("logs",[])[-80:],
        "filename":j["filename"],"stats":j["stats"],
        "target_gb":j.get("target_gb"),"target_bytes":j.get("target_bytes"),
        "collected_bytes":j.get("collected_bytes",0),
        "internet_exhausted":False,"sources_exhausted":j.get("sources_exhausted",False),
        "timed_out":j.get("timed_out",False),"size_limited":j.get("size_limited",False),
        "time_limit_seconds":j.get("time_limit_seconds",JOB_RUNTIME_SECONDS),
        "suggestions":j.get("suggestions",[]),"catalog_status":j.get("catalog_status",[]),
        "error":j.get("error"),
    })

@app.route("/api/download/<jid>")
def download(jid):
    j=load_job(jid)
    if not j or j.get("status")!="done" or not j.get("filepath"):
        return jsonify({"error":"The dataset package is not ready."}),404
    if not os.path.isfile(j["filepath"]):
        return jsonify({"error":"The package is no longer available on this server."}),410
    return send_file(j["filepath"],as_attachment=True,download_name=j["filename"],mimetype="application/zip")

if __name__=="__main__":
    print("\n"+"="*55)
    print("  DataForge Pro v6 — SPEED OPTIMIZED")
    print(f"  ⚡ {PARALLEL_WORKERS} parallel workers | {MAX_FILE_SIZE_MB}MB max file size")
    print(f"  🚫 Auto-skip servers below {MIN_SPEED_KBPS}KB/s")
    print("  Landing: http://localhost:5000")
    print("  App:     http://localhost:5000/app")
    print("="*55+"\n")
    app.run(debug=False,host="0.0.0.0",port=5000,threaded=True)
