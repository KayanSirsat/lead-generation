#!/usr/bin/env python3
"""
main.py
========
Automated Lead Scraping, Enrichment, and Verification Pipeline.

Flow:
    1. Load client ICP config from config.json + API keys from .env
    2. Pull raw leads from Apollo.io (/v1/mixed_people/search)
    3. For each lead:
         - If Apollo returned an email -> verify it with Hunter.io (/v2/email-verifier)
         - If Apollo returned no email -> fall back to Hunter.io (/v2/email-finder),
           then verify whatever email (if any) comes back
    4. Normalize everything into a pandas DataFrame
    5. Filter out undeliverable emails (unless overridden in config)
    6. Export a timestamped CSV: {client_name}_leads_{timestamp}.csv

Author: Staff Data Engineering (RevOps Automation)
"""

from __future__ import annotations

import json
import logging
import os
import sys
import time
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import pandas as pd
import requests
from dotenv import load_dotenv
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
    before_sleep_log,
)

# --------------------------------------------------------------------------- #
# Logging setup
# --------------------------------------------------------------------------- #

logging.basicConfig(
    level=logging.INFO,
    format="[%(levelname)s] %(message)s",
    stream=sys.stdout,
)
logger = logging.getLogger("lead_pipeline")


# --------------------------------------------------------------------------- #
# Custom exceptions
# --------------------------------------------------------------------------- #

class PipelineError(Exception):
    """Base exception for unrecoverable pipeline failures."""


class InvalidAPIKeyError(PipelineError):
    """Raised when an API responds with 401/403 (bad or missing key)."""


class RateLimitExceededError(Exception):
    """Raised when an API responds with 429; caught internally and retried."""


# --------------------------------------------------------------------------- #
# Data model
# --------------------------------------------------------------------------- #

@dataclass
class Lead:
    first_name: str = ""
    last_name: str = ""
    job_title: str = ""
    company_name: str = ""
    domain: str = ""
    email: Optional[str] = None
    verification_status: str = "unknown"      # deliverable / risky / undeliverable / unknown
    confidence_score: Optional[int] = None      # 0-100
    linkedin_url: str = ""
    trigger_note: str = ""
    date_scraped: str = field(
        default_factory=lambda: datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    )

    @property
    def full_name(self) -> str:
        return f"{self.first_name} {self.last_name}".strip()

    def to_row(self) -> Dict[str, Any]:
        return {
            "Full Name": self.full_name,
            "First Name": self.first_name,
            "Last Name": self.last_name,
            "Job Title": self.job_title,
            "Company Name": self.company_name,
            "Domain": self.domain,
            "Email": self.email or "",
            "Verification Status": self.verification_status,
            "Confidence Score": self.confidence_score if self.confidence_score is not None else "",
            "LinkedIn URL": self.linkedin_url,
            "Trigger Note": self.trigger_note,
            "Date Scraped": self.date_scraped,
        }


# --------------------------------------------------------------------------- #
# Configuration loader
# --------------------------------------------------------------------------- #

class PipelineConfig:
    """Loads and validates config.json + .env secrets."""

    def __init__(self, config_path: str = "config.json", env_path: str = ".env"):
        self.config_path = config_path
        self._load_env(env_path)
        self._load_config(config_path)
        self._validate()

    def _load_env(self, env_path: str) -> None:
        if os.path.exists(env_path):
            load_dotenv(env_path)
        else:
            # Still attempt to load from process env (e.g. CI/CD secrets)
            load_dotenv()
        self.apollo_api_key = os.getenv("APOLLO_API_KEY", "").strip()
        self.hunter_api_key = os.getenv("HUNTER_API_KEY", "").strip()

    def _load_config(self, config_path: str) -> None:
        if not os.path.exists(config_path):
            raise PipelineError(f"Config file not found: {config_path}")
        try:
            with open(config_path, "r", encoding="utf-8") as f:
                raw: Dict[str, Any] = json.load(f)
        except json.JSONDecodeError as exc:
            raise PipelineError(f"config.json is not valid JSON: {exc}") from exc

        self.client_name: str = raw.get("client_name", "Unnamed_Client")
        icp: Dict[str, Any] = raw.get("target_icp", {})
        self.target_roles: List[str] = icp.get("target_roles", [])
        self.locations: List[str] = icp.get("locations", [])
        self.company_size: List[str] = icp.get("company_size", [])
        self.trigger_note: str = icp.get("trigger_note", "")

        self.max_leads: int = int(raw.get("max_leads", 10))
        self.output_format: str = raw.get("output_format", "csv").lower()
        self.include_undeliverable: bool = bool(raw.get("include_undeliverable", False))
        self.min_confidence_score: int = int(raw.get("min_confidence_score", 0))
        self.apollo_page_size: int = int(raw.get("apollo_page_size", 25))
        self.request_delay_seconds: float = float(raw.get("request_delay_seconds", 1.0))

    def _validate(self) -> None:
        missing = []
        if not self.apollo_api_key or self.apollo_api_key == "your_apollo_api_key_here":
            missing.append("APOLLO_API_KEY")
        if not self.hunter_api_key or self.hunter_api_key == "your_hunter_api_key_here":
            missing.append("HUNTER_API_KEY")
        if missing:
            raise InvalidAPIKeyError(
                f"Missing/placeholder API key(s) in environment: {', '.join(missing)}. "
                f"Copy .env.example to .env and fill in real keys."
            )
        if self.max_leads <= 0:
            raise PipelineError("max_leads must be a positive integer.")
        if self.output_format not in ("csv", "json"):
            raise PipelineError(f"Unsupported output_format: {self.output_format}")


# --------------------------------------------------------------------------- #
# Apollo.io client
# --------------------------------------------------------------------------- #

class ApolloClient:
    # NOTE: Apollo retired the legacy /v1/mixed_people/search endpoint for API callers.
    # The current endpoint lives under /api/v1 and is /mixed_people/api_search.
    # See: https://docs.apollo.io/reference/people-api-search
    #
    # IMPORTANT BEHAVIOR CHANGE: this endpoint does NOT return email addresses or
    # phone numbers (Apollo docs: "This endpoint doesn't return email addresses or
    # phone numbers"). Getting an email out of Apollo now requires a separate,
    # credit-consuming call to the People Enrichment endpoint. This pipeline does
    # NOT call that enrichment endpoint by default (to avoid silently burning
    # Apollo credits) -- instead every lead simply falls through to the Hunter.io
    # email-finder fallback, which is exactly the path the code already supports.
    BASE_URL = "https://api.apollo.io/api/v1"

    def __init__(self, api_key: str, request_delay: float = 1.0):
        self.api_key = api_key
        self.request_delay = request_delay
        self.session = requests.Session()
        self.session.headers.update(
            {
                "Content-Type": "application/json",
                "Cache-Control": "no-cache",
                "X-Api-Key": api_key,
            }
        )

    @retry(
        reraise=True,
        stop=stop_after_attempt(4),
        wait=wait_exponential(multiplier=2, min=2, max=30),
        retry=retry_if_exception_type(RateLimitExceededError),
        before_sleep=before_sleep_log(logger, logging.WARNING),
    )
    def _post(self, endpoint: str, payload: Dict[str, Any]) -> Dict[str, Any]:
        url = f"{self.BASE_URL}{endpoint}"
        # Apollo requires the key in the X-Api-Key header (set on self.session),
        # not in the JSON body — see https://docs.apollo.io/docs/test-api-key
        try:
            resp = self.session.post(url, json=payload, timeout=20)
        except requests.exceptions.Timeout as exc:
            logger.warning("Apollo request timed out; will not retry further for this call.")
            raise PipelineError(f"Apollo API timeout: {exc}") from exc
        except requests.exceptions.ConnectionError as exc:
            raise PipelineError(f"Apollo API connection error: {exc}") from exc

        if resp.status_code == 401 or resp.status_code == 403:
            raise InvalidAPIKeyError("Apollo API rejected the API key (401/403). Check APOLLO_API_KEY.")

        if resp.status_code == 429:
            retry_after = float(resp.headers.get("Retry-After", 5))
            logger.warning(f"Apollo rate limit hit (429). Backing off ~{retry_after:.1f}s before retry.")
            time.sleep(retry_after)
            raise RateLimitExceededError("Apollo rate limit exceeded.")

        if resp.status_code >= 500:
            raise RateLimitExceededError(f"Apollo server error {resp.status_code}; retrying.")

        if not resp.ok:
            raise PipelineError(f"Apollo API error {resp.status_code}: {resp.text[:300]}")

        try:
            return resp.json()
        except ValueError as exc:
            raise PipelineError(f"Apollo API returned non-JSON response: {exc}") from exc

    @staticmethod
    def _normalize_employee_ranges(ranges: List[str]) -> List[str]:
        """
        Apollo's api_search endpoint expects each range as 'min,max' (comma-separated),
        e.g. '11,50'. Human-friendly config files often use '11-50' (hyphenated) instead --
        normalize either format to what the API actually requires.
        """
        normalized = []
        for r in ranges:
            r = str(r).strip()
            if "-" in r and "," not in r:
                parts = r.split("-", 1)
                normalized.append(f"{parts[0].strip()},{parts[1].strip()}")
            else:
                normalized.append(r)
        return normalized

    def search_people(
        self,
        target_roles: List[str],
        locations: List[str],
        company_size: List[str],
        max_leads: int,
        page_size: int = 25,
    ) -> List[Dict[str, Any]]:
        """Paginate through /mixed_people/api_search until max_leads is reached."""
        collected: List[Dict[str, Any]] = []
        page = 1
        per_page = min(page_size, max_leads) if max_leads > 0 else page_size
        employee_ranges = self._normalize_employee_ranges(company_size)

        while len(collected) < max_leads:
            payload = {
                "person_titles": target_roles,
                "person_locations": locations,
                "organization_num_employees_ranges": employee_ranges,
                "page": page,
                "per_page": per_page,
            }
            logger.info(f"Querying Apollo mixed_people/api_search (page {page})...")
            try:
                data = self._post("/mixed_people/api_search", payload)
            except InvalidAPIKeyError:
                raise
            except (PipelineError, RateLimitExceededError) as exc:
                logger.error(f"Apollo search failed on page {page}: {exc}")
                break

            people = data.get("people", []) or data.get("contacts", [])
            if not people:
                logger.info("Apollo returned no further results; stopping pagination.")
                break

            collected.extend(people)
            pagination = data.get("pagination", {})
            total_pages = pagination.get("total_pages", page)

            time.sleep(self.request_delay)

            if page >= total_pages:
                break
            page += 1

        return collected[:max_leads]

    @staticmethod
    def _clean_domain(raw_domain: str) -> str:
        domain = raw_domain or ""
        domain = domain.replace("https://", "")
        domain = domain.replace("http://", "")
        domain = domain.rstrip("/")
        return domain

    @staticmethod
    def parse_person(raw: Dict[str, Any], trigger_note: str) -> Lead:
        org = raw.get("organization") or {}

        email = raw.get("email")
        if email in ("email_not_unlocked@domain.com", "", None):
            email = None

        first_name = raw.get("first_name", "") or ""
        last_name = raw.get("last_name", "") or ""
        job_title = raw.get("title", "") or ""
        company_name = org.get("name", "") or raw.get("organization_name", "") or ""
        raw_domain = org.get("primary_domain") or org.get("website_url") or ""
        domain = ApolloClient._clean_domain(raw_domain)
        linkedin_url = raw.get("linkedin_url", "") or ""

        lead = Lead()
        lead.first_name = first_name
        lead.last_name = last_name
        lead.job_title = job_title
        lead.company_name = company_name
        lead.domain = domain
        lead.email = email
        lead.linkedin_url = linkedin_url
        lead.trigger_note = trigger_note
        return lead


# --------------------------------------------------------------------------- #
# Hunter.io client
# --------------------------------------------------------------------------- #

class HunterClient:
    BASE_URL = "https://api.hunter.io/v2"

    def __init__(self, api_key: str, request_delay: float = 1.0):
        self.api_key = api_key
        self.request_delay = request_delay
        self.session = requests.Session()

    @retry(
        reraise=True,
        stop=stop_after_attempt(4),
        wait=wait_exponential(multiplier=2, min=2, max=30),
        retry=retry_if_exception_type(RateLimitExceededError),
        before_sleep=before_sleep_log(logger, logging.WARNING),
    )
    def _get(self, endpoint: str, params: Dict[str, Any]) -> Dict[str, Any]:
        url = f"{self.BASE_URL}{endpoint}"
        params = {**params, "api_key": self.api_key}
        try:
            resp = self.session.get(url, params=params, timeout=15)
        except requests.exceptions.Timeout as exc:
            raise PipelineError(f"Hunter API timeout: {exc}") from exc
        except requests.exceptions.ConnectionError as exc:
            raise PipelineError(f"Hunter API connection error: {exc}") from exc

        if resp.status_code == 401:
            raise InvalidAPIKeyError("Hunter API rejected the API key (401). Check HUNTER_API_KEY.")

        if resp.status_code == 429:
            retry_after = float(resp.headers.get("Retry-After", 5))
            logger.warning(f"Hunter rate limit hit (429). Backing off ~{retry_after:.1f}s before retry.")
            time.sleep(retry_after)
            raise RateLimitExceededError("Hunter rate limit exceeded.")

        if resp.status_code >= 500:
            raise RateLimitExceededError(f"Hunter server error {resp.status_code}; retrying.")

        if not resp.ok:
            # Hunter returns 4xx (e.g. 404 not found / 400 bad params) for "no result" cases too.
            logger.debug(f"Hunter API non-OK response {resp.status_code}: {resp.text[:200]}")
            return {}

        try:
            return resp.json()
        except ValueError:
            return {}

    def verify_email(self, email: str) -> Dict[str, Any]:
        """Calls /v2/email-verifier. Returns {'status': str, 'score': int} on success."""
        if not email:
            return {"status": "unknown", "score": None}
        try:
            data = self._get("/email-verifier", {"email": email})
        except (PipelineError, RateLimitExceededError) as exc:
            logger.error(f"Hunter verification failed for {email}: {exc}")
            return {"status": "unknown", "score": None}
        finally:
            time.sleep(self.request_delay)

        result = data.get("data", {})
        return {
            "status": result.get("status", "unknown"),
            "score": result.get("score"),
        }

    def find_email(self, first_name: str, last_name: str, domain: str) -> Optional[str]:
        """Calls /v2/email-finder. Returns discovered email string or None."""
        if not domain:
            return None
        try:
            data = self._get(
                "/email-finder",
                {"domain": domain, "first_name": first_name, "last_name": last_name},
            )
        except (PipelineError, RateLimitExceededError) as exc:
            logger.error(f"Hunter email-finder failed for {first_name} {last_name}@{domain}: {exc}")
            return None
        finally:
            time.sleep(self.request_delay)

        result = data.get("data", {})
        email = result.get("email")
        return email or None


# --------------------------------------------------------------------------- #
# Pipeline orchestration
# --------------------------------------------------------------------------- #

class LeadPipeline:
    def __init__(self, config: PipelineConfig):
        self.config = config
        self.apollo = ApolloClient(config.apollo_api_key, request_delay=config.request_delay_seconds)
        self.hunter = HunterClient(config.hunter_api_key, request_delay=config.request_delay_seconds)

    def run(self) -> pd.DataFrame:
        logger.info(f"Starting pipeline for client: {self.config.client_name}")
        logger.info(
            f"ICP -> roles={self.config.target_roles} | locations={self.config.locations} "
            f"| company_size={self.config.company_size} | max_leads={self.config.max_leads}"
        )

        raw_people = self._fetch_raw_leads()
        if not raw_people:
            logger.warning("No leads returned from Apollo. Exiting with empty result set.")
            return self._empty_dataframe()

        leads = self._enrich_and_verify(raw_people)
        df = self._to_dataframe(leads)
        df = self._apply_filters(df)
        return df

    def _fetch_raw_leads(self) -> List[Dict[str, Any]]:
        try:
            people = self.apollo.search_people(
                target_roles=self.config.target_roles,
                locations=self.config.locations,
                company_size=self.config.company_size,
                max_leads=self.config.max_leads,
                page_size=self.config.apollo_page_size,
            )
        except InvalidAPIKeyError:
            raise
        logger.info(f"Apollo returned {len(people)} raw lead(s).")
        return people

    def _enrich_and_verify(self, raw_people: List[Dict[str, Any]]) -> List[Lead]:
        leads: List[Lead] = []
        total = len(raw_people)

        for idx, raw in enumerate(raw_people, start=1):
            lead = ApolloClient.parse_person(raw, self.config.trigger_note)

            if lead.email:
                logger.info(
                    f"[{idx}/{total}] {lead.full_name} | Apollo email found | Verifying email..."
                )
                verification = self.hunter.verify_email(lead.email)
            else:
                logger.info(
                    f"[{idx}/{total}] {lead.full_name} | No email from Apollo search "
                    f"(expected -- api_search doesn't return emails) | "
                    f"Falling back to Hunter email-finder..."
                )
                found_email = self.hunter.find_email(lead.first_name, lead.last_name, lead.domain)
                if found_email:
                    lead.email = found_email
                    logger.info(f"[{idx}/{total}] Hunter found email: {found_email} | Verifying...")
                    verification = self.hunter.verify_email(found_email)
                else:
                    logger.info(f"[{idx}/{total}] Hunter could not find an email for {lead.full_name}.")
                    verification = {"status": "not_found", "score": None}

            lead.verification_status = verification.get("status", "unknown")
            lead.confidence_score = verification.get("score")

            logger.info(
                f"[{idx}/{total}] Scraped & verified {lead.full_name} "
                f"| Status: {lead.verification_status} | Score: {lead.confidence_score}"
            )
            leads.append(lead)

        return leads

    @staticmethod
    def _to_dataframe(leads: List[Lead]) -> pd.DataFrame:
        rows = [lead.to_row() for lead in leads]
        columns = [
            "Full Name", "First Name", "Last Name", "Job Title", "Company Name",
            "Domain", "Email", "Verification Status", "Confidence Score",
            "LinkedIn URL", "Trigger Note", "Date Scraped",
        ]
        return pd.DataFrame(rows, columns=columns)

    @staticmethod
    def _empty_dataframe() -> pd.DataFrame:
        columns = [
            "Full Name", "First Name", "Last Name", "Job Title", "Company Name",
            "Domain", "Email", "Verification Status", "Confidence Score",
            "LinkedIn URL", "Trigger Note", "Date Scraped",
        ]
        return pd.DataFrame(columns=columns)

    def _apply_filters(self, df: pd.DataFrame) -> pd.DataFrame:
        if df.empty:
            return df

        before = len(df)
        if not self.config.include_undeliverable:
            df = df[df["Verification Status"] != "undeliverable"]

        if self.config.min_confidence_score > 0:
            numeric_scores = pd.to_numeric(df["Confidence Score"], errors="coerce")
            df = df[
                (numeric_scores.isna()) | (numeric_scores >= self.config.min_confidence_score)
            ]

        after = len(df)
        if after != before:
            logger.info(f"Filtered {before - after} lead(s) that did not meet quality thresholds.")

        return df.reset_index(drop=True)


# --------------------------------------------------------------------------- #
# Output writer
# --------------------------------------------------------------------------- #

def export_dataframe(df: pd.DataFrame, client_name: str, output_format: str) -> str:
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    safe_client_name = "".join(c if c.isalnum() or c in ("_", "-") else "_" for c in client_name)
    filename = f"{safe_client_name}_leads_{timestamp}.{output_format}"

    if output_format == "csv":
        df.to_csv(filename, index=False, encoding="utf-8")
    elif output_format == "json":
        df.to_json(filename, orient="records", indent=2)
    else:
        raise PipelineError(f"Unsupported output_format: {output_format}")

    return filename


# --------------------------------------------------------------------------- #
# Entry point
# --------------------------------------------------------------------------- #

def main() -> int:
    import argparse

    parser = argparse.ArgumentParser(description="Automated Lead Scraping & Verification Pipeline")
    parser.add_argument("--config", default="config.json", help="Path to config.json")
    parser.add_argument("--env", default=".env", help="Path to .env file")
    args = parser.parse_args()

    try:
        config = PipelineConfig(config_path=args.config, env_path=args.env)
    except InvalidAPIKeyError as exc:
        logger.error(str(exc))
        return 1
    except PipelineError as exc:
        logger.error(f"Configuration error: {exc}")
        return 1

    pipeline = LeadPipeline(config)

    try:
        df = pipeline.run()
    except InvalidAPIKeyError as exc:
        logger.error(str(exc))
        return 1
    except PipelineError as exc:
        logger.error(f"Pipeline failed: {exc}")
        return 1
    except KeyboardInterrupt:
        logger.warning("Pipeline interrupted by user.")
        return 130

    if df.empty:
        logger.warning("Pipeline finished with 0 qualifying leads. No file written.")
        return 0

    try:
        filename = export_dataframe(df, config.client_name, config.output_format)
    except PipelineError as exc:
        logger.error(f"Failed to export results: {exc}")
        return 1

    logger.info(f"Pipeline complete. {len(df)} verified lead(s) exported to: {filename}")
    return 0


if __name__ == "__main__":
    sys.exit(main())