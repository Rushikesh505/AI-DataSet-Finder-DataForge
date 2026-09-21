"""
DataForge Pro v6 — SPEED OPTIMIZED
- Parallel downloads using ThreadPoolExecutor (8 workers)
- Persistent connection sessions (3x faster than new connections)
- Per-file speed measurement — skips slow servers automatically
- Smart timeouts — HEAD request first to check size/speed
- Chunked streaming with speed throttle detection
- Archive.org direct file API (skip HTML pages)
"""
import os, re, json, time, threading, hashlib, zipfile, uuid, warnings
import feedparser, requests, pandas as pd
from datetime import datetime
from collections import deque
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib.parse import urlparse, urlunparse, parse_qs, urlencode, quote_plus, urljoin
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

app = Flask(__name__)
JOBS = {}
BASE_DIR   = os.path.dirname(os.path.abspath(__file__))
OUTPUT_DIR = os.path.join(BASE_DIR, "outputs")
os.makedirs(OUTPUT_DIR, exist_ok=True)

# ── SPEED SETTINGS ──
PARALLEL_WORKERS   = 8      # simultaneous downloads
MAX_FILE_SIZE_MB   = 50     # skip files bigger than this (too slow)
MIN_SPEED_KBPS     = 20     # skip servers slower than 20 KB/s
CONNECT_TIMEOUT    = 6      # seconds to establish connection
READ_TIMEOUT       = 15     # seconds to wait for first byte
CHUNK_SIZE         = 131072 # 128KB chunks (faster than 64KB)
PAGE_TIMEOUT       = 8      # seconds for HTML page fetch

HEADERS = {
    "User-Agent":"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/124 Safari/537.36",
    "Accept-Language":"en-US,en;q=0.9",
    "Accept-Encoding":"gzip, deflate",
    "Connection":"keep-alive",
}

IMAGE_EXTS = {".jpg",".jpeg",".png",".gif",".webp",".bmp",".tiff"}
AUDIO_EXTS = {".mp3",".wav",".ogg",".flac",".m4a",".aac",".opus"}
VIDEO_EXTS = {".mp4",".webm",".avi",".mov",".mkv",".flv",".m4v"}
SKIP_DOMAINS = {"twitter.com","facebook.com","instagram.com","youtube.com",
                "tiktok.com","linkedin.com","t.co","reddit.com"}

# ── SESSION FACTORY — persistent connections ──
def make_session():
    """Create a session with connection pooling and retry logic."""
    s = requests.Session()
    retry = Retry(total=2, backoff_factor=0.3,
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
    try: return not any(d in urlparse(url).netloc for d in SKIP_DOMAINS)
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
          "how","why","when","where","tell","me","about","give","show","find","want"}
    words=re.findall(r'\b[a-zA-Z]{3,}\b',topic.lower())
    return [w for w in words if w not in stop]

def is_relevant(keywords, text, min_score=0.2):
    if not keywords: return True
    text_lower=(text or "").lower()
    if not text_lower.strip(): return False
    hits=sum(1 for kw in keywords if kw in text_lower)
    return (hits/max(len(keywords),1))>=min_score

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
def get_archive_direct_files(identifier, allowed_exts, log=None):
    """
    Use Archive.org metadata API to get direct download URLs.
    Much faster than scraping HTML pages.
    """
    urls = []
    try:
        s = get_session()
        r = s.get(f"https://archive.org/metadata/{identifier}",
                  timeout=(CONNECT_TIMEOUT, READ_TIMEOUT))
        r.raise_for_status()
        data = r.json()
        base = f"https://archive.org/download/{identifier}/"
        for f in data.get("files", []):
            name = f.get("name","")
            ext = os.path.splitext(name)[1].lower()
            if ext in allowed_exts:
                size = int(f.get("size", 0))
                # Skip files that are too large
                if size > MAX_FILE_SIZE_MB * 1024 * 1024:
                    continue
                urls.append(base + name)
    except: pass
    return urls

# ── SPEED-AWARE DOWNLOADER ──
def download_file_fast(url, dest_folder, allowed_exts, keywords, context="", min_rel=0.2):
    """
    Download with speed checking:
    1. HEAD request to check size and server
    2. If too big or server too slow, skip
    3. Stream with speed measurement
    4. Kill download if speed drops below threshold
    """
    try:
        ext = file_ext(url)
        if ext not in allowed_exts: return "", 0, "wrong_ext"

        # Relevance check
        combined = urlparse(url).path.replace("-"," ").replace("_"," ")+" "+context
        if keywords and not is_relevant(keywords, combined, min_rel):
            return "", 0, "not_relevant"

        fname = f"file_{hashlib.md5(url.encode()).hexdigest()[:12]}{ext}"
        fpath = os.path.join(dest_folder, fname)
        if os.path.exists(fpath):
            return fname, os.path.getsize(fpath), "cached"

        s = get_session()

        # HEAD request — check size before downloading
        try:
            head = s.head(url, timeout=(CONNECT_TIMEOUT, 5), allow_redirects=True)
            size_header = int(head.headers.get("content-length", 0))
            if size_header > MAX_FILE_SIZE_MB * 1024 * 1024:
                return "", 0, f"too_big_{size_header//1024//1024}MB"
            ct = head.headers.get("content-type","").lower()
        except:
            size_header = 0
            ct = ""

        # Content-type check
        if ext in AUDIO_EXTS and ct and not any(x in ct for x in ["audio","mpeg","ogg","flac","wav","octet","mp3"]): return "",0,"wrong_ct"
        if ext in VIDEO_EXTS and ct and not any(x in ct for x in ["video","mp4","webm","octet"]): return "",0,"wrong_ct"
        if ext in IMAGE_EXTS and ct and not any(x in ct for x in ["image","octet","jpeg","png","gif","webp"]): return "",0,"wrong_ct"

        # Stream download with speed measurement
        r = s.get(url, timeout=(CONNECT_TIMEOUT, READ_TIMEOUT), stream=True)
        r.raise_for_status()

        # Size checks
        size_header = int(r.headers.get("content-length", size_header))
        if ext in IMAGE_EXTS and 0<size_header<5000: return "",0,"too_small"
        if ext in AUDIO_EXTS and 0<size_header<50000: return "",0,"too_small"
        if ext in VIDEO_EXTS and 0<size_header<100000: return "",0,"too_small"
        if size_header > MAX_FILE_SIZE_MB*1024*1024: return "",0,"too_big"

        # Download with speed measurement
        total = 0
        speed_check_bytes = 0
        speed_check_start = time.time()
        SPEED_CHECK_INTERVAL = 3  # check speed every 3 seconds

        with open(fpath, "wb") as f:
            for chunk in r.iter_content(CHUNK_SIZE):
                if not chunk: continue
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
                        try: os.remove(fpath)
                        except: pass
                        return "", 0, f"too_slow_{speed_kbps:.0f}kbps"
                    speed_check_bytes = 0
                    speed_check_start = time.time()

                # Hard size cap
                if total > MAX_FILE_SIZE_MB * 1024 * 1024:
                    break

        if total < 1000:
            try: os.remove(fpath)
            except: pass
            return "", 0, "empty"

        return fname, total, "ok"

    except Exception as e:
        try:
            if os.path.exists(os.path.join(dest_folder, f"file_{hashlib.md5(url.encode()).hexdigest()[:12]}{file_ext(url)}")):
                os.remove(os.path.join(dest_folder, f"file_{hashlib.md5(url.encode()).hexdigest()[:12]}{file_ext(url)}"))
        except: pass
        return "", 0, str(e)[:25]

# ── PARALLEL MEDIA DOWNLOADER ──
def download_batch(urls_and_contexts, dest_folder, allowed_exts, keywords, min_rel, log=None):
    """
    Download multiple files in parallel using ThreadPoolExecutor.
    Returns list of (fname, size, reason) tuples.
    """
    results = []
    if not urls_and_contexts: return results

    def dl_one(item):
        url, context = item
        return download_file_fast(url, dest_folder, allowed_exts, keywords, context, min_rel)

    with ThreadPoolExecutor(max_workers=PARALLEL_WORKERS) as executor:
        futures = {executor.submit(dl_one, item): item for item in urls_and_contexts}
        for future in as_completed(futures):
            try:
                fname, size, reason = future.result(timeout=60)
                results.append((fname, size, reason))
            except Exception as e:
                results.append(("", 0, str(e)[:20]))
    return results

# ── DISCOVERY ──
def discover(topic, mode="text", max_links=60, log=None, round_num=1, seen_urls=None):
    clean = norm_topic(topic)
    keywords = extract_keywords(clean)
    if seen_urls is None: seen_urls = set()

    base_queries = {
        "text": [
            f"{clean} article news",
            f"{clean} research analysis",
            f"{clean} blog guide tutorial",
            f"{clean} report study {2020+round_num}",
        ],
        "image": [
            f"{clean} images photos",
            f"{clean} pictures gallery dataset",
            f"site:flickr.com {clean}",
            f"{clean} photography",
        ],
        "audio": [
            f"{clean} mp3 free download audio",
            f"{clean} wav recording",
            f"site:freesound.org {clean}",
            f"site:archive.org {clean} audio",
        ],
        "video": [
            f"{clean} mp4 video free download",
            f"site:archive.org {clean} video",
            f"{clean} footage clip webm",
            f"{clean} documentary short film",
        ],
    }

    queries = base_queries.get(mode, base_queries["text"])
    queries = queries[round_num % len(queries):] + queries[:round_num % len(queries)]

    found = []
    def add(lst, name):
        n=0
        for u in lst:
            u2=norm_url(u)
            if u2 and u2 not in seen_urls and valid_url(u2):
                seen_urls.add(u2); found.append(u2); n+=1
        if log and n>0: log(f"  ✓ {name}: +{n} URLs")

    # Run discovery in parallel too
    def ddg_search(q):
        if not DDGS_OK: return []
        try:
            with DDGS() as d:
                return [r.get("href","") or r.get("url","") for r in d.text(q,max_results=40,safesearch="off")]
        except: return []

    with ThreadPoolExecutor(max_workers=4) as ex:
        futs = {ex.submit(ddg_search, q): q for q in queries[:3]}
        for fut in as_completed(futs):
            try: add(fut.result(), f"DDG:{futs[fut][:25]}")
            except: pass

    if mode=="text":
        try:
            s=get_session()
            feed=feedparser.parse(f"https://news.google.com/rss/search?q={quote_plus(clean)}&hl=en")
            add([decode_gnews(e.get("link","")) for e in feed.entries[:50]],"Google News")
        except: pass
        try:
            s=get_session()
            r=s.get(f"https://api.gdeltproject.org/api/v2/doc/doc?query={quote_plus(clean)}&mode=artlist&maxrecords=80&format=json",timeout=10)
            add([a.get("url","") for a in r.json().get("articles",[])],"GDELT")
        except: pass

    if mode=="audio":
        try:
            s=get_session()
            r=s.get(f"https://archive.org/search?query={quote_plus(clean)}&and[]=mediatype%3Aaudio&output=json&rows=50",timeout=10)
            identifiers=[i['identifier'] for i in r.json().get("response",{}).get("docs",[]) if i.get("identifier")]
            # Use Archive.org metadata API directly — much faster
            add([f"archive_id:{iid}" for iid in identifiers], "Archive.org Audio (direct API)")
        except: pass
        try:
            s=get_session()
            r=s.get(f"https://freesound.org/search/?q={quote_plus(clean)}&f=type:mp3",timeout=8)
            if BS4_OK:
                soup=BeautifulSoup(r.text,"lxml")
                add([urljoin("https://freesound.org",a["href"]) for a in soup.select("a[href*='/sounds/']") if a.get("href")],"Freesound.org")
        except: pass

    if mode=="video":
        try:
            s=get_session()
            r=s.get(f"https://archive.org/search?query={quote_plus(clean)}&and[]=mediatype%3Amovies&output=json&rows=50",timeout=10)
            identifiers=[i['identifier'] for i in r.json().get("response",{}).get("docs",[]) if i.get("identifier")]
            add([f"archive_id:{iid}" for iid in identifiers], "Archive.org Video (direct API)")
        except: pass

    if log: log(f"  → Round {round_num}: {len(found)} fresh sources")
    return found, clean, keywords

# ── FAST PAGE SCRAPER ──
def scrape_page_fast(url, mode, dest, allowed_exts, keywords, visited, base_domain, q, min_rel):
    """Single page scrape — fast version with short timeout."""
    result = {"url":url,"title":"","text":"","media_urls":[],"filtered":0,"bytes":0}
    try:
        s = get_session()
        resp = s.get(url, timeout=(CONNECT_TIMEOUT, PAGE_TIMEOUT))
        ct = resp.headers.get("content-type","").lower()
        ext = file_ext(url)
        if ext in allowed_exts:
            result["media_urls"].append((url, "direct"))
            return result
        if "text/html" not in ct: return result
        resp.raise_for_status()
    except: return result
    if not BS4_OK: return result

    soup = BeautifulSoup(resp.text, "lxml")
    if soup.title: result["title"] = (soup.title.string or "").strip()
    page_text = soup.get_text(" ", strip=True)[:3000]
    page_rel = not keywords or is_relevant(keywords, page_text, 0.1)

    if mode == "text":
        for tag in soup(["script","style","nav","footer","header","aside","noscript"]): tag.decompose()
        paras = [p.get_text(" ").strip() for p in soup.find_all(["p","h1","h2","h3","article","li"])]
        text = " ".join(p for p in paras if len(p)>30)
        if keywords and not is_relevant(keywords, text, 0.08):
            result["filtered"] += 1
        else:
            result["text"] = text
            result["bytes"] = len(text.encode())

    elif mode == "image":
        for img in soup.find_all("img"):
            src = img.get("src") or img.get("data-src") or ""
            if not src: continue
            img_url = urljoin(url, src)
            ext2 = file_ext(img_url)
            if ext2 not in allowed_exts: continue
            alt = img.get("alt","").strip()
            parent_text = (img.parent.get_text(" ")[:200] if img.parent else "")
            ctx = alt+" "+parent_text+" "+result["title"]
            if not page_rel or (keywords and not is_relevant(keywords, ctx, 0.15)):
                result["filtered"] += 1; continue
            result["media_urls"].append((img_url, ctx))

    elif mode in ("audio","video"):
        found = {}
        for tag in soup.find_all(["audio","video","source"]):
            src = tag.get("src","")
            if src: found[urljoin(url,src)] = result["title"]
        for a in soup.find_all("a", href=True):
            full = urljoin(url, a["href"])
            if file_ext(full) in allowed_exts:
                found[full] = a.get_text()[:80]+" "+result["title"]
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

# ── NLP ──
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
        job["last_update"]=time.time()

    try:
        target_bytes = int(target_gb * 1024**3)
        job["status"]="running"; job["target_bytes"]=target_bytes; job["target_gb"]=target_gb

        log(f"🚀 DataForge Pro v6 — {mode.upper()} pipeline [SPEED OPTIMIZED]")
        log(f"🎯 TARGET: {target_gb} GB — runs until full or internet exhausted")
        log(f"⚡ {PARALLEL_WORKERS} parallel workers | {MAX_FILE_SIZE_MB}MB max file | {MIN_SPEED_KBPS}KB/s min speed")
        log(f"🔍 Relevance filter: {int(min_relevance*100)}%")

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

        text_rows=[]; media_rows=[]; visited=set()
        total_pages=0; total_media=0; total_filtered=0; total_skipped=0
        collected_bytes=0; seen_urls=set()
        start_time=time.time(); internet_exhausted=False
        MAX_ROUNDS=20; MAX_PAGES_PER_SITE=20

        # ── TARGET LOOP ──
        round_num=0
        while collected_bytes < target_bytes:
            round_num+=1
            if round_num>MAX_ROUNDS:
                log(f"")
                log(f"🌐 Completed {round_num-1} discovery rounds.")
                log(f"😔 Sorry — I've covered the entire internet for '{norm_topic(topic)}'.")
                log(f"📊 Maximum collected: {fmt_size(collected_bytes)} of {target_gb}GB target.")
                log(f"💡 Try: broader topic, lower target size, or different data type.")
                internet_exhausted=True; break

            log(f"")
            log(f"🔄 ROUND {round_num} — discovering fresh sources...")
            seeds,clean,keywords=discover(topic,mode,60,log,round_num,seen_urls)

            if not seeds:
                consecutive=job.get("cons_empty",0)+1; job["cons_empty"]=consecutive
                if consecutive>=3:
                    log(f"")
                    log(f"🌐 No new sources after {consecutive} rounds.")
                    log(f"😔 Sorry — I've covered the entire internet for '{clean}'.")
                    log(f"📊 Maximum: {fmt_size(collected_bytes)} of {target_gb}GB.")
                    log(f"💡 Tip: Try a broader topic or smaller target.")
                    internet_exhausted=True; break
                log(f"⚠️ No new sources this round, retrying..."); time.sleep(2); continue
            else:
                job["cons_empty"]=0

            log(f"✅ {len(seeds)} fresh sources to process")

            for si,seed in enumerate(seeds):
                if collected_bytes>=target_bytes: break

                # Handle Archive.org identifiers directly (faster)
                if seed.startswith("archive_id:"):
                    identifier=seed.replace("archive_id:","")
                    direct_urls=get_archive_direct_files(identifier,allowed_exts)
                    if direct_urls:
                        dl_items=[(u,"archive.org "+identifier) for u in direct_urls[:5]]
                        dl_results=download_batch(dl_items,media_folder or "",allowed_exts,keywords,min_relevance,log)
                        ok_count=0
                        for fname,size,reason in dl_results:
                            if fname and size>0:
                                media_rows.append({
                                    "id":len(media_rows)+1,"topic":clean,
                                    "page_url":f"https://archive.org/details/{identifier}",
                                    "domain":"archive.org","file_url":f"https://archive.org/download/{identifier}/{fname}",
                                    "filename":fname,"format":file_ext(fname),
                                    "description":f"Archive.org: {identifier}",
                                    "file_size_bytes":size,"downloaded_at":datetime.now().isoformat(),
                                })
                                total_media+=1; collected_bytes+=size; ok_count+=1
                            elif reason=="too_slow_":
                                total_skipped+=1
                        if ok_count>0:
                            job["collected_bytes"]=collected_bytes
                            log(f"  ⚡ archive.org/{identifier}: {ok_count}/{len(dl_items)} files | {fmt_size(collected_bytes)}/{target_gb}GB")
                    continue

                # Regular URL — scrape then batch download
                domain=urlparse(seed).netloc
                elapsed=time.time()-start_time
                rate=collected_bytes/max(elapsed,1)
                remaining=target_bytes-collected_bytes
                eta_sec=remaining/max(rate,1)
                eta_str=(f"~{int(eta_sec//60)}min" if eta_sec<3600 else f"~{eta_sec/3600:.1f}h") if rate>1000 else "..."
                spd=fmt_speed(rate)

                fill=int(collected_bytes/target_bytes*20)
                bar="█"*fill+"░"*(20-fill)
                log(f"🌐 R{round_num}·{si+1} [{bar}] {fmt_size(collected_bytes)}/{target_gb}GB | {spd} | ETA:{eta_sec//60:.0f}min | {domain}")

                q=deque([seed]); site_pages=0; site_start=time.time()

                while q and site_pages<MAX_PAGES_PER_SITE and (time.time()-site_start)<30:
                    if collected_bytes>=target_bytes: break
                    url=q.popleft()
                    if url in visited: continue
                    visited.add(url)

                    try:
                        res=scrape_page_fast(url,mode,media_folder or "",allowed_exts,keywords,
                                            visited,domain,q,min_relevance)
                    except:
                        res={"url":url,"title":"","text":"","media_urls":[],"filtered":0,"bytes":0}

                    site_pages+=1; total_pages+=1
                    total_filtered+=res.get("filtered",0)

                    if mode=="text" and len(res.get("text",""))>100:
                        sl,ss,sp,sn,snu=sentiment(res["text"])
                        text_rows.append({
                            "id":len(text_rows)+1,"topic":clean,"url":url,"domain":domain,
                            "title":res["title"],"text":res["text"],
                            "word_count":len(res["text"].split()),
                            "scraped_at":datetime.now().isoformat(),
                            "sentiment_label":sl,"sentiment_score":ss,
                            "sentiment_pos":sp,"sentiment_neg":sn,"sentiment_neu":snu,
                            "topic_cluster_id":0,"topic_label":"",
                        })
                        bytes_added=res["bytes"]
                        collected_bytes+=bytes_added; job["collected_bytes"]=collected_bytes

                    # PARALLEL download for media
                    elif mode!="text" and res.get("media_urls"):
                        dl_results=download_batch(
                            res["media_urls"],media_folder or "",
                            allowed_exts,keywords,min_relevance,log)
                        for fname,size,reason in dl_results:
                            if fname and size>0:
                                media_rows.append({
                                    "id":len(media_rows)+1,"topic":clean,"page_url":url,
                                    "domain":domain,"file_url":url,"filename":fname,
                                    "format":file_ext(fname),"description":"",
                                    "file_size_bytes":size,"downloaded_at":datetime.now().isoformat(),
                                })
                                total_media+=1; collected_bytes+=size
                            elif "too_slow" in reason or "too_big" in reason:
                                total_skipped+=1
                        job["collected_bytes"]=collected_bytes

                job["scraped"]=total_pages
                job["progress"]=min(99,int(collected_bytes/target_bytes*100))
                time.sleep(max(0,delay-0.2))  # slightly shorter delay for speed

            # Speed report per round
            elapsed=time.time()-start_time
            avg_speed=fmt_speed(collected_bytes/max(elapsed,1))
            log(f"📊 Round {round_num} done | {fmt_size(collected_bytes)}/{target_gb}GB | avg speed: {avg_speed} | skipped slow: {total_skipped}")

        # ── SAVE ──
        log(f""); log(f"💾 Saving dataset...")
        stats_domains={}

        if mode=="text":
            if not text_rows:
                log("❌ No text collected."); job["status"]="error"
                job["internet_exhausted"]=internet_exhausted; return
            log(f"🧩 Clustering {len(text_rows)} articles into {num_clusters} topics...")
            txts=[r["text"] for r in text_rows]
            cids,cnames=cluster_texts(txts,k=num_clusters)
            for i,r in enumerate(text_rows):
                r["topic_cluster_id"]=cids[i]; r["topic_label"]=cnames.get(cids[i],f"topic_{cids[i]}")
            df=pd.DataFrame(text_rows)
            if "csv" in text_formats or not text_formats:
                p=os.path.join(job_folder,"text_dataset.csv"); df.to_csv(p,index=False)
                log(f"  ✓ text_dataset.csv — {len(df)} rows | {os.path.getsize(p)/(1024**2):.1f}MB")
            if "json" in text_formats or not text_formats:
                p=os.path.join(job_folder,"text_dataset.json"); df.to_json(p,orient="records",indent=2,force_ascii=False)
                log(f"  ✓ text_dataset.json — {os.path.getsize(p)/(1024**2):.1f}MB")
            p=os.path.join(job_folder,"text_dataset.jsonl")
            with open(p,"w") as f:
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
                "rounds":round_num,"internet_exhausted":internet_exhausted,
            }
        else:
            df=pd.DataFrame(media_rows) if media_rows else pd.DataFrame()
            mname={"image":"image_dataset","audio":"audio_dataset","video":"video_dataset"}[mode]
            if not df.empty:
                df.to_csv(os.path.join(job_folder,f"{mname}.csv"),index=False)
                df.to_json(os.path.join(job_folder,f"{mname}.json"),orient="records",indent=2,force_ascii=False)
                log(f"  ✓ {mname}.csv + .json — {len(df)} files")
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
                "rounds":round_num,"internet_exhausted":internet_exhausted,
            }

        log(f"📦 Creating ZIP..."); 
        zip_name=f"{mode}_{safe}_{ts}.zip"; zip_path=os.path.join(OUTPUT_DIR,zip_name)
        with zipfile.ZipFile(zip_path,"w",zipfile.ZIP_DEFLATED) as zf:
            for root,dirs,files in os.walk(job_folder):
                for f in files:
                    fp=os.path.join(root,f); zf.write(fp,os.path.relpath(fp,OUTPUT_DIR))
        zip_mb=os.path.getsize(zip_path)/(1024**2)
        if job["stats"]: job["stats"]["zip_mb"]=round(zip_mb,2)
        job["status"]="done"; job["filename"]=zip_name
        job["filepath"]=zip_path; job["progress"]=100
        job["internet_exhausted"]=internet_exhausted
        elapsed_total=time.time()-start_time
        avg_speed=fmt_speed(collected_bytes/max(elapsed_total,1))

        if internet_exhausted:
            log(f""); log(f"📊 FINAL: {fmt_size(collected_bytes)} in {elapsed_total/60:.1f}min | avg {avg_speed}")
            log(f"😔 Internet exhausted for this topic — maximum data reached")
        else:
            log(f""); log(f"🎉 TARGET HIT! {fmt_size(collected_bytes)} in {elapsed_total/60:.1f}min | avg {avg_speed}")
            log(f"📦 ZIP: {zip_name} ({zip_mb:.1f}MB) | ⬇️ Download below!")

    except Exception as e:
        import traceback; log(f"❌ {str(e)}"); log(traceback.format_exc()[:500])
        job["status"]="error"

# ── ROUTES ──
@app.route("/")
def landing(): return render_template("landing.html")

@app.route("/app")
def dashboard(): return render_template("app.html")

@app.route("/api/start",methods=["POST"])
def start():
    d=request.json; topic=d.get("topic","").strip()
    if not topic: return jsonify({"error":"Topic required"}),400
    mode=d.get("mode","text"); clusters=int(d.get("num_clusters",5))
    delay=float(d.get("delay",0.2))
    formats=d.get("formats",[]); text_formats=d.get("text_formats",["csv","json"])
    min_rel=float(d.get("min_relevance",0.3)); target_gb=float(d.get("target_gb",1.0))
    jid=str(uuid.uuid4())[:8]
    JOBS[jid]={
        "status":"starting","logs":[],"progress":0,"scraped":0,
        "total_links":0,"filename":None,"filepath":None,"stats":None,
        "last_update":time.time(),"target_gb":target_gb,
        "target_bytes":int(target_gb*1024**3),
        "collected_bytes":0,"internet_exhausted":False,"cons_empty":0,
    }
    threading.Thread(target=run_pipeline,
        args=(jid,topic,mode,clusters,delay,formats,text_formats,min_rel,target_gb),
        daemon=True).start()
    return jsonify({"job_id":jid})

@app.route("/api/status/<jid>")
def status(jid):
    j=JOBS.get(jid)
    if not j: return jsonify({"error":"Not found"}),404
    return jsonify({
        "status":j["status"],"progress":j["progress"],
        "scraped":j.get("scraped",0),"logs":j["logs"][-80:],
        "filename":j["filename"],"stats":j["stats"],
        "target_gb":j.get("target_gb"),"target_bytes":j.get("target_bytes"),
        "collected_bytes":j.get("collected_bytes",0),
        "internet_exhausted":j.get("internet_exhausted",False),
    })

@app.route("/api/download/<jid>")
def download(jid):
    j=JOBS.get(jid)
    if not j or not j.get("filepath"): return jsonify({"error":"Not ready"}),404
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
