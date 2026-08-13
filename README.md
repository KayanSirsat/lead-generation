# Lead Scraping, Enrichment & Verification Pipeline

An automated B2B lead-generation pipeline that sources prospects from **Apollo.io**, fills in and verifies email addresses via **Hunter.io**, and exports a clean, deliverable-only lead list to CSV — driven entirely by a single JSON config file.

Built for RevOps / growth teams who need repeatable, ICP-targeted lead lists without manually stitching together multiple tools.

---

## Table of Contents

- [How It Works](#how-it-works)
- [Features](#features)
- [Project Structure](#project-structure)
- [Prerequisites](#prerequisites)
- [Installation](#installation)
- [Configuration](#configuration)
  - [.env — API Keys](#env--api-keys)
  - [config.json — ICP & Run Settings](#configjson--icp--run-settings)
- [Usage](#usage)
- [Output](#output)
- [Error Handling & Rate Limiting](#error-handling--rate-limiting)
- [Important API Notes](#important-api-notes)
- [Extending the Pipeline](#extending-the-pipeline)
- [Troubleshooting](#troubleshooting)
- [Disclaimer](#disclaimer)
- [License](#license)

---

## How It Works

```
config.json (ICP)                 .env (API keys)
      │                                 │
      └───────────────┬─────────────────┘
                      ▼
             ┌─────────────────┐
             │  Apollo.io API  │  →  raw leads matching role / location / company size
             └────────┬────────┘
                      ▼
      For each lead: does Apollo have an email?
             ┌────────┴────────┐
            YES                NO
             │                  │
             ▼                  ▼
     Hunter.io Verifier   Hunter.io Email Finder
     (deliverability      (first_name + last_name
     + confidence score)  + domain → candidate email)
             │                  │
             │                  ▼
             │          Hunter.io Verifier
             │          (verify the found email)
             └────────┬─────────┘
                      ▼
              Normalize into pandas DataFrame
                      ▼
        Filter out "undeliverable" (configurable)
                      ▼
      {client_name}_leads_{timestamp}.csv
```

1. **Fetch** — Queries Apollo's people-search endpoint using the ICP defined in `config.json` (job titles, locations, company headcount).
2. **Enrich** — For leads with no email on file, Hunter's Email Finder attempts to locate one from the person's name and company domain.
3. **Verify** — Every email (whether sourced from Apollo or found by Hunter) is run through Hunter's Email Verifier to get a deliverability status and 0–100 confidence score.
4. **Clean & Export** — Results are assembled into a pandas DataFrame, undeliverable emails are dropped by default, and the final list is written to a timestamped CSV.

---

## Features

- ⚙️ **Config-driven** — change target roles, locations, company size, and lead volume without touching code.
- 🔐 **Secure secrets handling** — API keys loaded from `.env`, never hardcoded or logged.
- 🔁 **Automatic retries** — exponential backoff on HTTP 429 (rate limit) and 5xx server errors via `tenacity`.
- 🛡️ **Fails fast on bad credentials** — distinguishes invalid API keys (401/403) from transient failures.
- 🧹 **Deliverability filtering** — undeliverable emails are excluded from the final export by default.
- 📊 **Structured output** — a consistent 12-column schema, ready to import into a CRM or outreach tool.
- 📜 **Real-time logging** — human-readable progress for every lead as it's scraped, enriched, and verified.
- 🧩 **Single-file, dependency-light** — one `main.py`, four third-party packages, no database required.

---

## Project Structure

```
.
├── main.py              # Entire pipeline: config loading, API clients, orchestration, export
├── config.json          # ICP / run parameters (safe to commit — no secrets)
├── .env.example         # Template for required API keys (copy to .env, do NOT commit .env)
├── requirements.txt     # Python dependencies
└── README.md            # This file
```

Running the pipeline produces additional (git-ignored) output files:

```
{client_name}_leads_{YYYYMMDD_HHMMSS}.csv
```

---

## Prerequisites

- **Python 3.9+**
- An **Apollo.io** account with API access ([get your key](https://app.apollo.io/#/settings/integrations/api))
- A **Hunter.io** account with API access ([get your key](https://hunter.io/api-keys))

> Free tiers work for testing, but are credit-limited (Hunter's free plan, for example, allows a small number of verifier/finder calls per month). See [Important API Notes](#important-api-notes) below before running against your full quota.

---

## Installation

```bash
# 1. Clone the repository
git clone https://github.com/<your-username>/<your-repo>.git
cd <your-repo>

# 2. (Recommended) create a virtual environment
python3 -m venv venv
source venv/bin/activate        # Windows: venv\Scripts\activate

# 3. Install dependencies
pip install -r requirements.txt
```

---

## Configuration

### .env — API Keys

Copy the example file and fill in your real keys:

```bash
cp .env.example .env
```

```dotenv
APOLLO_API_KEY=your_apollo_api_key_here
HUNTER_API_KEY=your_hunter_api_key_here
```

`.env` is loaded via `python-dotenv` and is **not** committed to version control (see [`.gitignore`](#gitignore)).

### config.json — ICP & Run Settings

```json
{
  "client_name": "Ahmedabad_Tech_Agency",
  "target_icp": {
    "target_roles": ["CTO", "VP of Engineering", "Head of Product", "Founder"],
    "locations": ["United States", "United Kingdom", "Canada"],
    "company_size": ["11-50", "51-200"],
    "trigger_note": "Hiring for React/Node or raised Seed/Series A"
  },
  "max_leads": 10,
  "output_format": "csv",
  "include_undeliverable": false,
  "min_confidence_score": 0,
  "apollo_page_size": 25,
  "request_delay_seconds": 1.2
}
```

| Field                     | Type                | Description                                                                                           |
| ------------------------- | ------------------- | ----------------------------------------------------------------------------------------------------- |
| `client_name`             | string              | Used to prefix the output filename.                                                                   |
| `target_icp.target_roles` | list[string]        | Job titles to search for on Apollo.                                                                   |
| `target_icp.locations`    | list[string]        | Target geographies.                                                                                   |
| `target_icp.company_size` | list[string]        | Employee-count ranges, e.g. `"11-50"`. Auto-normalized to Apollo's required `"11,50"` format.         |
| `target_icp.trigger_note` | string              | Freeform note carried through to every output row (e.g. a qualifying trigger/context for outreach).   |
| `max_leads`               | int                 | Maximum number of leads to pull and process.                                                          |
| `output_format`           | `"csv"` \| `"json"` | Export format.                                                                                        |
| `include_undeliverable`   | bool                | If `false` (default), rows verified as `undeliverable` are dropped before export.                     |
| `min_confidence_score`    | int (0–100)         | Optional minimum Hunter confidence score; rows below this are filtered out. `0` disables this filter. |
| `apollo_page_size`        | int                 | Results requested per Apollo API page (max leads are still capped by `max_leads`).                    |
| `request_delay_seconds`   | float               | Delay between outbound API calls to stay within free-tier rate limits.                                |

---

## Usage

Run with the default `config.json` / `.env` in the current directory:

```bash
python main.py
```

Or point to alternate files (useful for running multiple clients from one codebase):

```bash
python main.py --config clients/acme_config.json --env .env
```

Sample console output:

```
[INFO] Starting pipeline for client: Ahmedabad_Tech_Agency
[INFO] ICP -> roles=['CTO', 'Founder'] | locations=['United States'] | company_size=['11-50'] | max_leads=10
[INFO] Querying Apollo mixed_people/api_search (page 1)...
[INFO] Apollo returned 10 raw lead(s).
[INFO] [1/10] Jane Doe | No email from Apollo search (expected -- api_search doesn't return emails) | Falling back to Hunter email-finder...
[INFO] [1/10] Hunter found email: jane@acme.com | Verifying...
[INFO] [1/10] Scraped & verified Jane Doe | Status: deliverable | Score: 92
...
[INFO] Filtered 2 lead(s) that did not meet quality thresholds.
[INFO] Pipeline complete. 8 verified lead(s) exported to: Ahmedabad_Tech_Agency_leads_20260813_120248.csv
```

---

## Output

Each row in the exported file corresponds to one verified lead:

| Column                     | Description                                                                        |
| -------------------------- | ---------------------------------------------------------------------------------- |
| `Full Name`                | Concatenated first + last name.                                                    |
| `First Name` / `Last Name` | As returned by Apollo.                                                             |
| `Job Title`                | Current title at their company.                                                    |
| `Company Name`             | Employer name.                                                                     |
| `Domain`                   | Company website domain (scheme/`www` stripped).                                    |
| `Email`                    | Sourced from Apollo or found via Hunter Email Finder.                              |
| `Verification Status`      | `deliverable`, `risky`, `undeliverable`, `not_found`, or `unknown`.                |
| `Confidence Score`         | Hunter deliverability score, 0–100.                                                |
| `LinkedIn URL`             | Person's LinkedIn profile, if available.                                           |
| `Trigger Note`             | The `trigger_note` value from `config.json`, carried through for outreach context. |
| `Date Scraped`             | UTC timestamp of when the record was processed.                                    |

By default, rows with `Verification Status == "undeliverable"` are excluded from the file entirely (set `include_undeliverable: true` in `config.json` to keep them for auditing).

---

## Error Handling & Rate Limiting

- **Invalid/missing API keys** — the pipeline validates both keys are present (and not the placeholder values) before making any network calls, and raises immediately if the API responds with `401`/`403`.
- **Rate limits (HTTP 429)** — automatically retried with exponential backoff (`tenacity`), respecting the `Retry-After` header when present.
- **Server errors (5xx) / transient failures** — retried with backoff, up to 4 attempts, before the run fails gracefully.
- **Timeouts / connection errors** — caught and logged; a single failed lead does not crash the entire run.
- **Configurable pacing** — `request_delay_seconds` adds a small delay between calls proactively, to avoid tripping rate limits on free-tier accounts in the first place.

---

## Important API Notes

- **Apollo no longer returns emails from search.** Apollo's people-search endpoint (`/api/v1/mixed_people/api_search`) does not return email addresses or phone numbers — that requires a separate, credit-consuming call to Apollo's People Enrichment endpoint, which this pipeline does **not** call by default (to avoid silently spending Apollo credits). In practice, this means **every lead routes through the Hunter.io Email Finder fallback**, not just leads where Apollo happened to omit an email. Budget your Hunter credits accordingly.
- **Free-tier quotas are limited.** Hunter's free plan, for example, allows a small number of verifier and finder calls per month. Running with a high `max_leads` can exhaust your monthly quota in a single run.
- **Employee size format.** Apollo's current API expects company-size ranges as `"min,max"` (e.g. `"11,50"`). This pipeline accepts the more readable `"11-50"` format in `config.json` and normalizes it automatically.

---

## Extending the Pipeline

Some natural next steps if you want to build on this:

- **CRM sync** — add a step after export to push rows into HubSpot/Salesforce/Pipedrive via their APIs.
- **Apollo People Enrichment** — wire in Apollo's enrichment endpoint as an opt-in config flag (`use_apollo_enrichment: true`) if you'd rather spend Apollo credits than lean entirely on Hunter.
- **Scheduling** — run on a cron job / GitHub Actions schedule for a continuously refreshed lead list.
- **Deduplication** — persist previously scraped emails/domains to a local file or database to avoid re-processing the same leads across runs.

---

## Troubleshooting

| Symptom                                                  | Likely Cause                                                                                                                                            |
| -------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `Missing/placeholder API key(s)` on startup              | `.env` wasn't created from `.env.example`, or still contains the placeholder text.                                                                      |
| `Apollo API error 422` mentioning a deprecated endpoint  | Apollo has changed their API again — check their [current docs](https://docs.apollo.io/reference/people-api-search) and update `ApolloClient.BASE_URL`. |
| Every lead ends up with `Verification Status: not_found` | Hunter couldn't find a match for the name + domain — this is common for less common name spellings or companies with strict catch-all email policies.   |
| Output file is empty / not written                       | All leads were filtered out (undeliverable or below `min_confidence_score`) — check the `[INFO] Filtered N lead(s)...` log line.                        |
| `InvalidAPIKeyError` at runtime                          | The key was accepted at startup validation but rejected by the live API — double-check for typos or an expired/revoked key.                             |

---

## Disclaimer

This tool is intended for legitimate B2B prospecting in compliance with Apollo.io's and Hunter.io's respective Terms of Service, as well as applicable data-privacy regulations (e.g. GDPR, CCPA) in your jurisdiction and your leads' jurisdictions. You are responsible for how the data this pipeline produces is stored, used, and for honoring opt-out/unsubscribe requests from contacted individuals.

---

## License

Add your preferred license here (e.g. MIT) — no license is currently specified.
