# DataForge Pro

DataForge searches for relevant public web content and builds a downloadable package for text, image, audio, or video collection. It also shows related public dataset links from GitHub, Kaggle, and Google Dataset Search.

## Run locally on Windows

Open PowerShell in this folder and run:

```powershell
py -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
python -m pip install -r requirements.txt
python app.py
```

Open `http://localhost:5000`; the collection dashboard is at `http://localhost:5000/app`.

## Collection limits

- A job runs for five minutes by default. An operator can raise `DATAFORGE_JOB_SECONDS` up to one hour; the job saves and packages what it collected when its time expires.
- The selected GB amount is a target, not a guarantee. Results depend on network speed, available sources, file sizes, source permissions, and provider limits.
- The default collection limit is 2,048 MB per job. `DATAFORGE_MAX_OUTPUT_MB` can be set from 8 MB to 8,192 MB.
- Page scraping and media downloads run in separate bounded worker pools. Requests are capped per host so one site is not flooded by the whole pool.
- A 1 GB target is allowed, but the app cannot guarantee that it finishes within one to five minutes. Available bandwidth, source response speed, relevant public files, and the time needed to package the output set the actual limit.
- The app searches public pages and files that can be accessed without bypassing sign-in, paywalls, access controls, or source restrictions. Sites may block automated requests or limit their results.
- Discovery combines general DuckDuckGo searches with direct Internet Archive and Wikimedia Commons catalogs. Query wording rotates across rounds, and accepted HTML page seeds are limited per host so a few popular domains cannot consume the whole source list. Queries from a host that has reached its per-round limit are not marked as seen, so they can be reconsidered in a later round.
- Optional Brave Search and Tavily search APIs add independent web indexes. Set `BRAVE_SEARCH_API_KEY` and/or `TAVILY_API_KEY` in the server environment; never paste keys into the browser or commit them to source control. Provider quotas or charges may apply.
- When a job stops below its target, the dashboard reports whether it hit the time/size cap or the configured search indexes stopped returning new URLs. “No new URLs” does not mean the whole internet was searched.
- Relevance uses query-term coverage and local TF-IDF similarity on page text and media descriptions. Each output row includes a relevance score to make filtering easier to review.
- Text mode writes CSV, JSON, and JSONL. Image, audio, and video modes write media files plus CSV/JSON metadata. All modes include `dataset_suggestions.csv` and `dataset_suggestions.json`.

## Dataset catalog access

- GitHub search uses the public repositories API. Set `GITHUB_TOKEN` to an authorized personal access token if you need the higher authenticated API rate limit. The token is read from the server environment and is not stored in the project files.
- Kaggle search uses the official Kaggle API client. Provide your own authorized credentials using `KAGGLE_USERNAME` and `KAGGLE_KEY`, or the standard `kaggle.json` file in your Kaggle configuration directory. The app does not bypass login, access restrictions, or dataset terms. Review the dataset license and access requirements at Kaggle before downloading.
- Google Dataset Search is shown as a topic-specific link because it does not provide this project with an open, general result API. Its link opens the catalog in a browser; the app does not scrape its login-protected or restricted content.
- The catalog cards are suggestions that link to the original dataset page. They do not automatically download an entire Kaggle or GitHub repository into the scraped collection.

## Configuration

- `DATAFORGE_JOB_SECONDS`: run window, from 30 to 3,600 seconds; default 300 (five minutes).
- `DATAFORGE_MAX_ACTIVE_JOBS`: concurrent jobs on one host, from 1 to 16; default 4.
- `DATAFORGE_MAX_OUTPUT_MB`: collected-content limit per job, from 8 to 8,192 MB; default 2,048.
- `DATAFORGE_CRAWL_WORKERS`: concurrent page workers, from 2 to 64; default 16.
- `DATAFORGE_DOWNLOAD_WORKERS`: concurrent media downloads, from 2 to 64; default 16.
- `DATAFORGE_DOWNLOADS_PER_BATCH`: candidate downloads per discovery round, from 8 to 1,000; default 256.
- `DATAFORGE_MAX_FILE_MB`: per-file download limit, from 16 to 4,096 MB; default 1,024.
- `DATAFORGE_MAX_HOST_CONNECTIONS`: concurrent requests to one host, from 1 to 16; default 3.
- `DATAFORGE_EMPTY_ROUNDS`: empty discovery rounds before stopping, from 1 to 12; default 4.
- `DATAFORGE_MAX_SEEDS_PER_DOMAIN`: HTML seed pages per host in one round, from 1 to 50; default 6.
- `DATAFORGE_QUERIES_PER_ROUND`: rotating search queries per collection round, from 1 to 12; default 6.
- `DATAFORGE_MAX_ROUNDS`, `DATAFORGE_MAX_SEEDS_PER_ROUND`, and `DATAFORGE_MAX_PAGES_PER_SITE`: bound discovery and same-site crawling; defaults are 20, 200, and 5.
- `DATAFORGE_OUTPUT_DIR`: folder for job status and generated ZIP files. Use persistent storage if downloads must survive a deployment restart.
- `DATAFORGE_JOB_DB`: optional path for the SQLite job database.

The job store and generated files are shared between worker processes on one host. Multiple application hosts need a shared job queue and shared file storage.
