"""
MDM Pipeline - Stage 3: Company Verification & Enrichment

This module starts from Stage 2B usable validated records and adds:
1. Company-address verification using Azure Maps POI/search.
2. Fallback company lookup in the same region when the validated address is weak.
3. AI-assisted enrichment hooks for firmographic fields.
4. Coverage and manual-review reporting for the end-to-end POC.
"""

import argparse
import asyncio
import ast
import copy
import hashlib
import json
import os
import re
from collections import Counter
from datetime import datetime
from pathlib import Path
from typing import Optional

import aiohttp
import pandas as pd
from dotenv import load_dotenv


class CompanyVerificationClient:
    """Azure Maps backed company/address verification client."""

    def __init__(
        self,
        azure_key: Optional[str] = None,
        poi_url: Optional[str] = None,
        fuzzy_url: Optional[str] = None,
        use_simulation: bool = True,
    ):
        self.azure_key = azure_key
        self.poi_url = poi_url or os.environ.get("AZURE_MAPS_POI_URL")
        self.fuzzy_url = fuzzy_url or os.environ.get("AZURE_MAPS_FUZZY_URL") or fuzzy_url_from_poi_url(self.poi_url)
        self.use_simulation = use_simulation if not azure_key else False
        self.rate_limit_delay = 0.1
        self.call_count = 0
        self.cache_hits = 0
        self._cache = {}
        self._cache_lock = asyncio.Lock()

        if self.use_simulation:
            print("[CompanyVerification] Running in SIMULATION mode (no Azure key)")
        elif not self.poi_url:
            print("[CompanyVerification] Azure Maps POI URL missing; API calls will be marked API_ERROR")
        else:
            print("[CompanyVerification] Using Azure Maps POI/fuzzy search")

    async def verify_company_at_address(
        self,
        company_name: str,
        address: str,
        country: str,
        latitude: Optional[float],
        longitude: Optional[float],
        session: Optional[aiohttp.ClientSession] = None,
    ) -> dict:
        """Return a structured company-address verification result."""
        key = self._cache_key(company_name, address, country, latitude, longitude)
        async with self._cache_lock:
            cached = self._cache.get(key)
            if cached is not None:
                self.cache_hits += 1
                return copy.deepcopy(cached)

        if self.use_simulation:
            result = self._simulate_verification(company_name, address, country)
        else:
            result = await self._verify_live(company_name, address, country, latitude, longitude, session)

        async with self._cache_lock:
            if not result.get("api_error"):
                self._cache[key] = copy.deepcopy(result)
        return result

    async def _verify_live(
        self,
        company_name: str,
        address: str,
        country: str,
        latitude: Optional[float],
        longitude: Optional[float],
        session: aiohttp.ClientSession,
    ) -> dict:
        candidates = await self._search_poi(
            query=company_name,
            country=country,
            latitude=latitude,
            longitude=longitude,
            radius=3000,
            limit=10,
            session=session,
        )
        best = self._score_candidates(company_name, address, latitude, longitude, candidates)

        if best:
            status = self._status_from_candidate(best)
            if status in ("VERIFIED", "LIKELY_MATCH"):
                return self._verification_result(status, best, "azure_poi_name_near_address")

        address_candidates = await self._search_poi(
            query=address,
            country=country,
            latitude=latitude,
            longitude=longitude,
            radius=250,
            limit=5,
            session=session,
        )
        address_best = self._score_candidates(company_name, address, latitude, longitude, address_candidates)
        if address_best:
            status = self._status_from_candidate(address_best)
            if status in ("VERIFIED", "LIKELY_MATCH", "ADDRESS_MISMATCH"):
                return self._verification_result(status, address_best, "azure_poi_address_occupant")

        fallback = await self.locate_company(company_name, country, session, latitude, longitude)
        found_company = bool(fallback.get("matched_company_name"))
        fallback.update({
            "company_verification_status": "CORRECT_ADDRESS_CANDIDATE" if found_company else "UNVERIFIED",
            "verification_confidence": fallback.get("verification_confidence", 0.0),
            "occupant_at_address": "",
            "verification_method": "azure_fuzzy_company_lookup",
        })
        return fallback

    async def _search_poi(
        self,
        query: str,
        country: str,
        latitude: Optional[float],
        longitude: Optional[float],
        radius: int,
        limit: int,
        session: aiohttp.ClientSession,
    ) -> list:
        self.call_count += 1
        if not self.poi_url:
            return []
        params = {
            "api-version": "1.0",
            "subscription-key": self.azure_key,
            "query": self._compact_query(query, ""),
            "limit": limit,
        }
        if country and len(str(country).strip()) == 2:
            params["countrySet"] = str(country).strip().upper()
        if pd.notna(latitude) and pd.notna(longitude):
            params["lat"] = float(latitude)
            params["lon"] = float(longitude)
            params["radius"] = radius

        try:
            await asyncio.sleep(self.rate_limit_delay)
            async with session.get(
                self.poi_url,
                params=params,
                timeout=aiohttp.ClientTimeout(total=15),
            ) as resp:
                if resp.status != 200:
                    return []
                data = await resp.json()
                return self._parse_candidates(data)
        except Exception:
            return []

    async def locate_company(
        self,
        company_name: str,
        country: str,
        session: aiohttp.ClientSession,
        latitude: Optional[float] = None,
        longitude: Optional[float] = None,
    ) -> dict:
        """Locate a company by name as fallback when address verification fails."""
        self.call_count += 1
        if not self.fuzzy_url:
            return self._api_error(company_name, "", "Azure Maps fuzzy URL not configured")
        params = {
            "api-version": "1.0",
            "subscription-key": self.azure_key,
            "query": company_name,
            "limit": 3,
        }
        if country and len(str(country).strip()) == 2:
            params["countrySet"] = str(country).strip().upper()
        if pd.notna(latitude) and pd.notna(longitude):
            params["lat"] = float(latitude)
            params["lon"] = float(longitude)
            params["radius"] = 50000

        try:
            await asyncio.sleep(self.rate_limit_delay)
            async with session.get(
                self.fuzzy_url,
                params=params,
                timeout=aiohttp.ClientTimeout(total=15),
            ) as resp:
                if resp.status != 200:
                    error_text = await resp.text()
                    return self._api_error(company_name, "", f"HTTP {resp.status}: {error_text[:200]}")
                data = await resp.json()
        except asyncio.TimeoutError:
            return self._api_error(company_name, "", "Request timeout")
        except Exception as exc:
            return self._api_error(company_name, "", str(exc)[:200])

        candidates = self._parse_candidates(data)
        best = self._score_candidates(company_name, "", None, None, candidates)
        if not best or best["name_score"] < 0.45 or is_generic_place_name(best["name"]):
            return {
                "matched_company_name": "",
                "matched_address": "",
                "matched_latitude": None,
                "matched_longitude": None,
                "matched_category": "",
                "verification_confidence": 0.0,
                "verification_evidence": json.dumps(["no company lookup match"]),
                "api_error": None,
            }

        return {
            "matched_company_name": best["name"],
            "matched_address": best["address"],
            "matched_latitude": best["latitude"],
            "matched_longitude": best["longitude"],
            "matched_category": best["category"],
            "verification_confidence": best["confidence"],
            "verification_evidence": json.dumps(best["evidence"], ensure_ascii=False),
            "api_error": None,
        }

    @staticmethod
    def _verification_result(status: str, candidate: dict, method: str) -> dict:
        return {
            "company_verification_status": status,
            "verification_confidence": candidate["confidence"],
            "matched_company_name": candidate["name"],
            "matched_address": candidate["address"],
            "matched_latitude": candidate["latitude"],
            "matched_longitude": candidate["longitude"],
            "matched_category": candidate["category"],
            "occupant_at_address": candidate["name"],
            "verification_method": method,
            "verification_evidence": json.dumps(candidate["evidence"], ensure_ascii=False),
            "api_error": None,
        }

    def _parse_candidates(self, data: dict) -> list:
        candidates = []
        for item in data.get("results", []):
            poi = item.get("poi", {}) or {}
            address = item.get("address", {}) or {}
            position = item.get("position", {}) or {}
            candidates.append({
                "name": poi.get("name", "") or item.get("type", ""),
                "address": address.get("freeformAddress", ""),
                "latitude": position.get("lat"),
                "longitude": position.get("lon"),
                "score": float(item.get("score", 0) or 0),
                "category": ", ".join(poi.get("categories", []) or []),
            })
        return candidates

    def _score_candidates(
        self,
        company_name: str,
        address: str,
        latitude: Optional[float],
        longitude: Optional[float],
        candidates: list,
    ) -> Optional[dict]:
        best = None
        for candidate in candidates:
            name_score = token_similarity(company_name, candidate["name"])
            address_score = token_similarity(address, candidate["address"]) if address else 0.0
            distance_score = self._distance_score(latitude, longitude, candidate["latitude"], candidate["longitude"])
            api_score = min(candidate.get("score", 0.0), 1.0)

            if address:
                confidence = (0.45 * name_score) + (0.30 * address_score) + (0.20 * distance_score) + (0.05 * api_score)
            else:
                confidence = (0.85 * name_score) + (0.15 * api_score)

            evidence = [
                f"name_similarity={name_score:.2f}",
                f"address_similarity={address_score:.2f}",
                f"distance_score={distance_score:.2f}",
                f"api_score={api_score:.2f}",
            ]
            enriched = dict(candidate)
            enriched["confidence"] = round(confidence, 3)
            enriched["name_score"] = round(name_score, 3)
            enriched["address_score"] = round(address_score, 3)
            enriched["distance_score"] = round(distance_score, 3)
            enriched["api_score"] = round(api_score, 3)
            enriched["evidence"] = evidence
            if best is None or enriched["confidence"] > best["confidence"]:
                best = enriched
        return best

    @staticmethod
    def _distance_score(lat1, lon1, lat2, lon2) -> float:
        if any(pd.isna(v) for v in [lat1, lon1, lat2, lon2]):
            return 0.0
        delta = abs(float(lat1) - float(lat2)) + abs(float(lon1) - float(lon2))
        if delta <= 0.001:
            return 1.0
        if delta <= 0.01:
            return 0.75
        if delta <= 0.05:
            return 0.45
        return 0.10

    @staticmethod
    def _status_from_candidate(candidate: dict) -> str:
        name_score = candidate.get("name_score", 0.0)
        address_score = candidate.get("address_score", 0.0)
        distance_score = candidate.get("distance_score", 0.0)
        confidence = candidate.get("confidence", 0.0)

        if is_generic_place_name(candidate.get("name", "")):
            return "UNVERIFIED"
        if name_score >= 0.70 and address_score >= 0.60 and distance_score >= 0.75:
            return "VERIFIED"
        if name_score >= 0.45 and address_score >= 0.45 and distance_score >= 0.45 and confidence >= 0.50:
            return "LIKELY_MATCH"
        if address_score >= 0.55 and distance_score >= 0.75:
            return "ADDRESS_MISMATCH"
        return "UNVERIFIED"

    @staticmethod
    def _compact_query(company_name: str, address: str) -> str:
        return re.sub(r"\s+", " ", f"{company_name} {address}").strip()[:180]

    @staticmethod
    def _cache_key(company_name, address, country, latitude, longitude) -> str:
        raw = "|".join([
            normalize_text(company_name),
            normalize_text(address),
            str(country).upper(),
            str(round(float(latitude), 5)) if pd.notna(latitude) else "",
            str(round(float(longitude), 5)) if pd.notna(longitude) else "",
        ])
        return hashlib.sha256(raw.encode("utf-8", errors="ignore")).hexdigest()

    @staticmethod
    def _api_error(company_name: str, address: str, message: str) -> dict:
        return {
            "company_verification_status": "API_ERROR",
            "verification_confidence": 0.0,
            "matched_company_name": "",
            "matched_address": "",
            "matched_latitude": None,
            "matched_longitude": None,
            "matched_category": "",
            "occupant_at_address": "",
            "verification_method": "azure_maps",
            "verification_evidence": json.dumps([message]),
            "api_error": message,
        }

    @staticmethod
    def _simulate_verification(company_name: str, address: str, country: str) -> dict:
        name_score = token_similarity(company_name, address)
        status = "LIKELY_MATCH" if name_score > 0.20 else "UNVERIFIED"
        return {
            "company_verification_status": status,
            "verification_confidence": round(max(name_score, 0.25), 3),
            "matched_company_name": company_name if status != "UNVERIFIED" else "",
            "matched_address": address if status != "UNVERIFIED" else "",
            "matched_latitude": None,
            "matched_longitude": None,
            "matched_category": "",
            "occupant_at_address": "",
            "verification_method": "simulation",
            "verification_evidence": json.dumps(["simulation mode"]),
            "api_error": None,
        }


class FirmographicEnrichmentClient:
    """Optional OpenAI-compatible enrichment client with deterministic fallback."""

    def __init__(
        self,
        api_key: Optional[str] = None,
        api_url: Optional[str] = None,
        api_version: Optional[str] = None,
        deployment: Optional[str] = None,
    ):
        self.api_key = api_key
        self.api_url = normalize_openai_url(api_url)
        self.api_version = api_version or "2024-06-01"
        self.deployment = deployment or "gpt-4.1-mini"
        self.chat_url = build_chat_completion_url(self.api_url, self.deployment, self.api_version)
        self.enabled = bool(api_key and self.chat_url)
        self.call_count = 0
        self.cache_hits = 0
        self._cache = {}
        self._cache_lock = asyncio.Lock()
        if self.enabled:
            print("[FirmographicEnrichment] OpenAI-compatible enrichment enabled")
        elif api_key and api_url:
            print("[FirmographicEnrichment] Could not build chat-completions URL; using deterministic fallback")
        else:
            print("[FirmographicEnrichment] Using deterministic enrichment fallback")

    async def enrich(self, row: pd.Series, verification: dict, session: aiohttp.ClientSession) -> dict:
        key = self._cache_key(row, verification)
        async with self._cache_lock:
            cached = self._cache.get(key)
            if cached is not None:
                self.cache_hits += 1
                return copy.deepcopy(cached)

        if self.enabled and verification.get("company_verification_status") in ("VERIFIED", "LIKELY_MATCH"):
            result = await self._enrich_with_openai(row, verification, session)
        else:
            result = self._deterministic_enrichment(row, verification)

        async with self._cache_lock:
            self._cache[key] = copy.deepcopy(result)
        return result

    async def _enrich_with_openai(self, row: pd.Series, verification: dict, session: aiohttp.ClientSession) -> dict:
        self.call_count += 1
        prompt = {
            "task": "Return concise firmographic enrichment as JSON only.",
            "company_name": verification.get("matched_company_name") or row.get("cleaned_name") or row.get("SOURCE_NAME"),
            "validated_address": row.get("validated_address", ""),
            "country": row.get("norm_country", ""),
            "required_fields": [
                "legal_name", "website", "email_domain", "naics_code", "sic_code",
                "parent_company", "domestic_ultimate", "global_ultimate",
                "headquarters_address", "employee_count", "revenue_range",
                "year_established", "confidence_notes"
            ],
        }
        payload = {
            "messages": [
                {"role": "system", "content": "You enrich company records. Return a single JSON object and use empty strings when unknown."},
                {"role": "user", "content": json.dumps(prompt, ensure_ascii=False)},
            ],
            "temperature": 0,
        }
        headers = openai_headers(self.api_url, self.api_key)

        try:
            async with session.post(
                self.chat_url,
                headers=openai_headers(self.chat_url, self.api_key),
                json=payload,
                timeout=aiohttp.ClientTimeout(total=30),
            ) as resp:
                if resp.status != 200:
                    text = await resp.text()
                    fallback = self._deterministic_enrichment(row, verification)
                    fallback["enrichment_notes"] = f"OpenAI enrichment failed: HTTP {resp.status}: {text[:120]}"
                    return fallback
                try:
                    data = await resp.json()
                except Exception:
                    data = None
        except Exception as exc:
            fallback = self._deterministic_enrichment(row, verification)
            fallback["enrichment_notes"] = f"OpenAI enrichment failed: {str(exc)[:120]}"
            return fallback

        content = self._extract_openai_content(data)
        parsed = safe_json_loads(content)
        fallback = self._deterministic_enrichment(row, verification)
        if not parsed:
            fallback["enrichment_notes"] = "OpenAI enrichment returned non-JSON content"
            return fallback

        result = dict(fallback)
        result.update({
            "legal_name": parsed.get("legal_name", fallback["legal_name"]),
            "website": parsed.get("website", ""),
            "email_domain": parsed.get("email_domain", ""),
            "naics_code": parsed.get("naics_code", ""),
            "sic_code": parsed.get("sic_code", ""),
            "parent_company": parsed.get("parent_company", ""),
            "domestic_ultimate": parsed.get("domestic_ultimate", ""),
            "global_ultimate": parsed.get("global_ultimate", ""),
            "headquarters_address": parsed.get("headquarters_address", ""),
            "employee_count": parsed.get("employee_count", ""),
            "revenue_range": parsed.get("revenue_range", ""),
            "year_established": parsed.get("year_established", ""),
            "enrichment_method": "openai_assisted",
            "enrichment_notes": parsed.get("confidence_notes", ""),
        })
        return result

    @staticmethod
    def _extract_openai_content(data: dict) -> str:
        if not isinstance(data, dict):
            return ""
        if "choices" in data and data["choices"]:
            message = data["choices"][0].get("message", {})
            return message.get("content", "")
        if "output_text" in data:
            return data["output_text"]
        return ""

    @staticmethod
    def _deterministic_enrichment(row: pd.Series, verification: dict) -> dict:
        is_verified = verification.get("company_verification_status") in ("VERIFIED", "LIKELY_MATCH")
        name = (
            verification.get("matched_company_name")
            if is_verified and verification.get("matched_company_name")
            else row.get("cleaned_name") or row.get("SOURCE_NAME", "")
        )
        address = (
            verification.get("matched_address")
            if is_verified and verification.get("matched_address")
            else row.get("validated_address", "")
        )
        domain = infer_domain_from_name(name) if is_verified else ""
        notes = (
            "Deterministic enrichment from verified company/address match."
            if is_verified
            else "Verification unresolved; firmographic enrichment held for manual review."
        )
        return {
            "legal_name": name,
            "enriched_full_address": address,
            "website": "",
            "email_domain": domain,
            "naics_code": "",
            "sic_code": "",
            "parent_company": "",
            "domestic_ultimate": "",
            "global_ultimate": "",
            "headquarters_address": address,
            "employee_count": "",
            "revenue_range": "",
            "year_established": "",
            "enrichment_method": "deterministic_fallback",
            "enrichment_notes": notes,
        }

    @staticmethod
    def _cache_key(row: pd.Series, verification: dict) -> str:
        raw = "|".join([
            str(row.get("MDM_KEY", "")),
            str(verification.get("matched_company_name", "")),
            str(verification.get("matched_address", "")),
        ])
        return hashlib.sha256(raw.encode("utf-8", errors="ignore")).hexdigest()


class CompanyVerificationAdjudicator:
    """Use Azure OpenAI to adjudicate borderline company/address candidates."""

    PROMOTABLE_STATUSES = {"ADDRESS_MISMATCH", "CORRECT_ADDRESS_CANDIDATE"}

    def __init__(
        self,
        api_key: Optional[str] = None,
        api_url: Optional[str] = None,
        api_version: Optional[str] = None,
        deployment: Optional[str] = None,
        promotion_threshold: float = 0.75,
    ):
        self.api_key = api_key
        self.api_url = normalize_openai_url(api_url)
        self.api_version = api_version or "2024-06-01"
        self.deployment = deployment or "gpt-4.1-mini"
        self.chat_url = build_chat_completion_url(self.api_url, self.deployment, self.api_version)
        self.enabled = bool(api_key and self.chat_url)
        self.promotion_threshold = promotion_threshold
        self.call_count = 0
        self.cache_hits = 0
        self._cache = {}
        self._cache_lock = asyncio.Lock()
        if self.enabled:
            print("[CompanyAdjudication] Azure OpenAI adjudication enabled")
        else:
            print("[CompanyAdjudication] Disabled; using deterministic verification only")

    async def adjudicate(self, row: pd.Series, verification: dict, session: aiohttp.ClientSession) -> dict:
        base = self._default_result(verification)
        status = verification.get("company_verification_status", "")
        if not self.enabled or status not in self.PROMOTABLE_STATUSES:
            return base
        if not verification.get("matched_company_name") and not verification.get("occupant_at_address"):
            return base

        key = self._cache_key(row, verification)
        async with self._cache_lock:
            cached = self._cache.get(key)
            if cached is not None:
                self.cache_hits += 1
                return copy.deepcopy(cached)

        result = await self._adjudicate_with_openai(row, verification, session)
        async with self._cache_lock:
            self._cache[key] = copy.deepcopy(result)
        return result

    async def _adjudicate_with_openai(self, row: pd.Series, verification: dict, session: aiohttp.ClientSession) -> dict:
        self.call_count += 1
        prompt = {
            "task": "Adjudicate whether the source company and Azure Maps candidate represent the same business at the validated address. Return JSON only.",
            "allowed_decisions": [
                "VERIFIED",
                "LIKELY_MATCH",
                "ADDRESS_MISMATCH",
                "CORRECT_ADDRESS_CANDIDATE",
                "UNVERIFIED",
            ],
            "source_company": row.get("cleaned_name") or row.get("SOURCE_NAME", ""),
            "source_raw_name": row.get("SOURCE_NAME", ""),
            "validated_address": row.get("validated_address", ""),
            "country": row.get("norm_country", ""),
            "deterministic_status": verification.get("company_verification_status", ""),
            "deterministic_confidence": verification.get("verification_confidence", 0.0),
            "azure_candidate_name": verification.get("matched_company_name", ""),
            "azure_candidate_address": verification.get("matched_address", ""),
            "azure_candidate_category": verification.get("matched_category", ""),
            "azure_evidence": verification.get("verification_evidence", ""),
            "decision_rules": [
                "Return VERIFIED only when source and candidate are clearly the same legal/brand entity at the validated address.",
                "Return LIKELY_MATCH when names are alias/brand/location variants and address evidence is compatible.",
                "Return ADDRESS_MISMATCH when the address appears occupied by a different unrelated organization.",
                "Return CORRECT_ADDRESS_CANDIDATE when the company seems real but candidate address differs from the validated address.",
                "Return UNVERIFIED when evidence is too weak.",
            ],
            "required_json_fields": ["decision", "confidence", "reason"],
        }
        payload = {
            "messages": [
                {
                    "role": "system",
                    "content": (
                        "You are a cautious MDM company-address verification adjudicator. "
                        "Prefer precision over recall. Return one compact JSON object only."
                    ),
                },
                {"role": "user", "content": json.dumps(prompt, ensure_ascii=False)},
            ],
            "temperature": 0,
        }

        try:
            async with session.post(
                self.chat_url,
                headers=openai_headers(self.chat_url, self.api_key),
                json=payload,
                timeout=aiohttp.ClientTimeout(total=30),
            ) as resp:
                if resp.status != 200:
                    text = await resp.text()
                    result = self._default_result(verification)
                    result["ai_adjudication_reason"] = f"OpenAI adjudication failed: HTTP {resp.status}: {text[:120]}"
                    return result
                try:
                    data = await resp.json()
                except Exception:
                    data = None
        except Exception as exc:
            result = self._default_result(verification)
            result["ai_adjudication_reason"] = f"OpenAI adjudication failed: {str(exc)[:120]}"
            return result

        content = FirmographicEnrichmentClient._extract_openai_content(data)
        parsed = safe_json_loads(content)
        if not parsed:
            result = self._default_result(verification)
            result["ai_adjudication_reason"] = "OpenAI adjudication returned non-JSON content"
            return result

        decision = str(parsed.get("decision", "")).strip().upper()
        if decision not in {"VERIFIED", "LIKELY_MATCH", "ADDRESS_MISMATCH", "CORRECT_ADDRESS_CANDIDATE", "UNVERIFIED"}:
            decision = verification.get("company_verification_status", "UNVERIFIED")
        confidence = clamp_float(parsed.get("confidence", 0.0))
        reason = str(parsed.get("reason", "")).strip()[:500]
        promoted = decision in {"VERIFIED", "LIKELY_MATCH"} and confidence >= self.promotion_threshold

        final_status = decision if promoted else verification.get("company_verification_status", "UNVERIFIED")
        return {
            "ai_adjudication_status": decision,
            "ai_adjudication_confidence": confidence,
            "ai_adjudication_reason": reason,
            "ai_adjudication_method": "azure_openai_candidate_adjudication",
            "ai_promoted_match": promoted,
            "final_company_verification_status": final_status,
            "overall_confidence_score": self._overall_confidence(verification, decision, confidence, promoted),
        }

    def _default_result(self, verification: dict) -> dict:
        status = verification.get("company_verification_status", "UNVERIFIED")
        confidence = clamp_float(verification.get("verification_confidence", 0.0))
        return {
            "ai_adjudication_status": "NOT_RUN",
            "ai_adjudication_confidence": 0.0,
            "ai_adjudication_reason": "",
            "ai_adjudication_method": "",
            "ai_promoted_match": False,
            "final_company_verification_status": status,
            "overall_confidence_score": confidence,
        }

    @staticmethod
    def _overall_confidence(verification: dict, decision: str, ai_confidence: float, promoted: bool) -> float:
        deterministic = clamp_float(verification.get("verification_confidence", 0.0))
        if promoted:
            return round((0.45 * deterministic) + (0.55 * ai_confidence), 3)
        if decision in {"ADDRESS_MISMATCH", "CORRECT_ADDRESS_CANDIDATE", "UNVERIFIED"}:
            return round(max(deterministic, ai_confidence), 3)
        return deterministic

    @staticmethod
    def _cache_key(row: pd.Series, verification: dict) -> str:
        raw = "|".join([
            str(row.get("MDM_KEY", "")),
            str(row.get("SOURCE_NAME", "")),
            str(row.get("validated_address", "")),
            str(verification.get("matched_company_name", "")),
            str(verification.get("matched_address", "")),
        ])
        return hashlib.sha256(raw.encode("utf-8", errors="ignore")).hexdigest()


class Week3Pipeline:
    """Company verification and enrichment orchestration."""

    def __init__(self, azure_key: Optional[str], openai_key: Optional[str], openai_url: Optional[str], batch_size: int = 10):
        local_openai = load_test1_azure_openai_config()
        api_version = (
            os.environ.get("AZURE_OPENAI_API_VERSION")
            or os.environ.get("OPENAI_API_VERSION")
            or local_openai.get("api_version")
        )
        deployment = (
            os.environ.get("AZURE_OPENAI_DEPLOYMENT")
            or os.environ.get("OPENAI_MODEL")
            or local_openai.get("model")
        )
        self.verifier = CompanyVerificationClient(azure_key=azure_key)
        self.adjudicator = CompanyVerificationAdjudicator(
            api_key=openai_key,
            api_url=openai_url,
            api_version=api_version,
            deployment=deployment,
        )
        self.enricher = FirmographicEnrichmentClient(
            api_key=openai_key,
            api_url=openai_url,
            api_version=api_version,
            deployment=deployment,
        )
        self.batch_size = batch_size

    async def run(self, df: pd.DataFrame) -> pd.DataFrame:
        self._initialize_columns(df)
        async with aiohttp.ClientSession() as session:
            for start in range(0, len(df), self.batch_size):
                stop = min(start + self.batch_size, len(df))
                tasks = [
                    self._process_record(idx, df.loc[idx], session)
                    for idx in df.index[start:stop]
                ]
                results = await asyncio.gather(*tasks)
                for idx, result in results:
                    for key, value in result.items():
                        df.at[idx, key] = value
                print(f"  Processed {stop}/{len(df)} records")
        return df

    async def _process_record(self, idx: int, row: pd.Series, session: aiohttp.ClientSession) -> tuple:
        company_name = row.get("cleaned_name") or row.get("SOURCE_NAME", "")
        address = row.get("validated_address") or row.get("api_address", "")
        verification = await self.verifier.verify_company_at_address(
            company_name=company_name,
            address=address,
            country=row.get("norm_country", ""),
            latitude=row.get("latitude"),
            longitude=row.get("longitude"),
            session=session,
        )
        deterministic_status = verification.get("company_verification_status", "UNVERIFIED")
        adjudication = await self.adjudicator.adjudicate(row, verification, session)
        final_status = adjudication.get("final_company_verification_status", deterministic_status)
        final_verification = dict(verification)
        final_verification["company_verification_status"] = final_status
        final_verification["verification_confidence"] = adjudication.get(
            "overall_confidence_score",
            verification.get("verification_confidence", 0.0),
        )

        enrichment = await self.enricher.enrich(row, final_verification, session)
        result = {}
        result.update(verification)
        result["deterministic_verification_status"] = deterministic_status
        result.update(adjudication)
        result["company_verification_status"] = final_status
        result["verification_confidence"] = final_verification["verification_confidence"]
        result.update(enrichment)
        route = route_week3_record(result)
        result.update(route)
        result["week3_manual_review_flag"] = result["review_routing"].startswith("REVIEW_")
        result["stage3_timestamp"] = datetime.now().isoformat()
        return idx, result

    @staticmethod
    def _initialize_columns(df: pd.DataFrame):
        columns = [
            "company_verification_status", "verification_confidence",
            "deterministic_verification_status", "final_company_verification_status",
            "overall_confidence_score", "ai_adjudication_status",
            "ai_adjudication_confidence", "ai_adjudication_reason",
            "ai_adjudication_method", "ai_promoted_match",
            "matched_company_name", "matched_address", "matched_latitude",
            "matched_longitude", "matched_category", "occupant_at_address",
            "verification_method", "verification_evidence", "legal_name",
            "enriched_full_address", "website", "email_domain", "naics_code",
            "sic_code", "parent_company", "domestic_ultimate",
            "global_ultimate", "headquarters_address", "employee_count",
            "revenue_range", "year_established", "enrichment_method",
            "enrichment_notes", "review_routing", "review_priority",
            "review_reason", "week3_manual_review_flag", "stage3_timestamp",
        ]
        for column in columns:
            if column not in df.columns:
                df[column] = None


def run_week3(input_csv: str, output_dir: str = "output_week3", limit: Optional[int] = None) -> pd.DataFrame:
    """Run Week 3 company verification and enrichment."""
    load_dotenv()
    Path(output_dir).mkdir(parents=True, exist_ok=True)
    local_openai = load_test1_azure_openai_config()

    df = pd.read_csv(input_csv, low_memory=False)
    if limit:
        df = df.head(limit).copy()

    usable = df[df["validation_status"].isin(["VALIDATED", "CORRECTED", "PARTIAL_VALIDATED"])].copy()
    usable = usable.reset_index(drop=True)
    print("=" * 70)
    print("  MDM Pipeline - Week 3: Company Verification & Enrichment")
    print(f"  Input records:   {len(df)}")
    print(f"  Usable records:  {len(usable)}")
    print(f"  Output:          {output_dir}")
    print("=" * 70)

    pipeline = Week3Pipeline(
        azure_key=os.environ.get("AZURE_MAPS_KEY"),
        openai_key=(
            os.environ.get("AZURE_OPENAI_API_KEY")
            or local_openai.get("api_key")
            or os.environ.get("OPENAI_API_KEY")
        ),
        openai_url=(
            os.environ.get("AZURE_OPENAI_API_BASE")
            or local_openai.get("api_base")
            or os.environ.get("OPENAI_URL")
        ),
    )
    enriched = asyncio.run(pipeline.run(usable))

    output_path = Path(output_dir) / "stage3_enriched.csv"
    enriched.to_csv(output_path, index=False)
    print(f"[Output] Enriched records saved to {output_path}")

    verified = enriched[enriched["company_verification_status"].isin(["VERIFIED", "LIKELY_MATCH"])]
    verified_path = Path(output_dir) / "stage3_verified_enriched.csv"
    verified.to_csv(verified_path, index=False)
    print(f"[Output] Verified enriched records saved to {verified_path}")

    manual = enriched[enriched["week3_manual_review_flag"] == True]
    manual_path = Path(output_dir) / "stage3_manual_review.csv"
    manual.to_csv(manual_path, index=False)
    print(f"[Output] Week 3 manual review records saved to {manual_path}")

    action_files = {
        "stage3_auto_enriched.csv": enriched[enriched["review_routing"] == "AUTO_ENRICH"],
        "stage3_auto_no_enrich.csv": enriched[enriched["review_routing"] == "AUTO_NO_ENRICH_ADDRESS_MISMATCH"],
        "stage3_targeted_review.csv": enriched[enriched["review_routing"].fillna("").str.startswith("REVIEW_")],
        "stage3_deferred_no_evidence.csv": enriched[enriched["review_routing"] == "DEFER_NO_EVIDENCE"],
    }
    for filename, subset in action_files.items():
        path = Path(output_dir) / filename
        subset.to_csv(path, index=False)
        print(f"[Output] {filename}: {len(subset)} records")

    report = generate_week3_report(enriched, output_dir, pipeline)
    print_summary(report)
    return enriched


def generate_week3_report(df: pd.DataFrame, output_dir: str, pipeline: Week3Pipeline) -> dict:
    total = len(df)
    verified_mask = df["company_verification_status"].isin(["VERIFIED", "LIKELY_MATCH"])
    field_coverage = {}
    for field in [
        "legal_name", "enriched_full_address", "website", "email_domain",
        "naics_code", "sic_code", "parent_company", "domestic_ultimate",
        "global_ultimate", "headquarters_address", "employee_count",
        "revenue_range", "year_established",
    ]:
        field_coverage[field] = int(df[field].fillna("").astype(str).str.strip().ne("").sum())

    report = {
        "generated_at": datetime.now().isoformat(),
        "total_records": total,
        "company_verification_distribution": df["company_verification_status"].value_counts(dropna=False).to_dict(),
        "verification_success": {
            "verified_or_likely": int(verified_mask.sum()),
            "verified_or_likely_rate": round(100 * verified_mask.sum() / max(total, 1), 1),
        },
        "enrichment_method_distribution": df["enrichment_method"].value_counts(dropna=False).to_dict(),
        "enrichment_field_coverage": field_coverage,
        "ai_adjudication": {
            "distribution": df["ai_adjudication_status"].value_counts(dropna=False).to_dict(),
            "promoted_matches": int((df["ai_promoted_match"] == True).sum()),
            "average_overall_confidence": round(
                df["overall_confidence_score"].fillna(0).astype(float).mean(),
                3,
            ),
        },
        "review_routing": {
            "distribution": df["review_routing"].value_counts(dropna=False).to_dict(),
            "targeted_review_total": int(df["review_routing"].fillna("").str.startswith("REVIEW_").sum()),
            "auto_enrich_total": int((df["review_routing"] == "AUTO_ENRICH").sum()),
            "auto_no_enrich_total": int((df["review_routing"] == "AUTO_NO_ENRICH_ADDRESS_MISMATCH").sum()),
            "deferred_no_evidence_total": int((df["review_routing"] == "DEFER_NO_EVIDENCE").sum()),
        },
        "manual_review": {
            "total": int((df["week3_manual_review_flag"] == True).sum()),
            "by_status": df[df["week3_manual_review_flag"] == True]["company_verification_status"].value_counts().to_dict(),
            "by_priority": df[df["week3_manual_review_flag"] == True]["review_priority"].value_counts().to_dict(),
        },
        "api_usage": {
            "azure_maps_calls": pipeline.verifier.call_count,
            "azure_maps_cache_hits": pipeline.verifier.cache_hits,
            "openai_adjudication_calls": pipeline.adjudicator.call_count,
            "openai_adjudication_cache_hits": pipeline.adjudicator.cache_hits,
            "openai_calls": pipeline.enricher.call_count,
            "openai_cache_hits": pipeline.enricher.cache_hits,
        },
    }
    report_path = Path(output_dir) / "week3_summary_report.json"
    with open(report_path, "w", encoding="utf-8") as handle:
        json.dump(report, handle, indent=2, ensure_ascii=False, default=str)
    print(f"[Report] Week 3 summary saved to {report_path}")
    return report


def print_summary(report: dict):
    print("\n" + "=" * 70)
    print("  Week 3 Pipeline Complete")
    print("=" * 70)
    print(f"  Records processed:      {report['total_records']}")
    print(f"  Verification statuses:  {report['company_verification_distribution']}")
    print(f"  Verified/likely:        {report['verification_success']['verified_or_likely']} "
          f"({report['verification_success']['verified_or_likely_rate']}%)")
    print(f"  Manual review queue:    {report['manual_review']['total']}")
    print(f"  API usage:              {report['api_usage']}")
    print("=" * 70)


def normalize_text(value: str) -> str:
    value = str(value or "").lower()
    value = re.sub(r"[^a-z0-9\s]", " ", value)
    value = re.sub(r"\b(inc|llc|ltd|co|company|corp|corporation|gmbh|sa|srl|spa|plc)\b", " ", value)
    return re.sub(r"\s+", " ", value).strip()


def token_similarity(left: str, right: str) -> float:
    left_tokens = set(normalize_text(left).split())
    right_tokens = set(normalize_text(right).split())
    if not left_tokens or not right_tokens:
        return 0.0
    return len(left_tokens & right_tokens) / len(left_tokens | right_tokens)


def infer_domain_from_name(name: str) -> str:
    tokens = normalize_text(name).split()
    if not tokens:
        return ""
    compact = "".join(tokens[:3])
    if len(compact) < 4:
        return ""
    return f"{compact}.com"


def is_generic_place_name(name: str) -> bool:
    normalized = normalize_text(name)
    generic = {
        "street", "road", "avenue", "boulevard", "drive", "lane", "place",
        "route", "highway", "residential", "commercial", "building",
    }
    return normalized in generic or len(normalized) <= 2


def route_week3_record(result: dict) -> dict:
    status = result.get("company_verification_status", "UNVERIFIED")
    confidence = clamp_float(result.get("overall_confidence_score", result.get("verification_confidence", 0.0)))

    if status in ("VERIFIED", "LIKELY_MATCH"):
        return {
            "review_routing": "AUTO_ENRICH",
            "review_priority": "NONE",
            "review_reason": "Trusted company-address match; record eligible for firmographic enrichment.",
        }

    if status == "ADDRESS_MISMATCH" and confidence >= 0.75:
        return {
            "review_routing": "AUTO_NO_ENRICH_ADDRESS_MISMATCH",
            "review_priority": "NONE",
            "review_reason": "High-confidence evidence indicates a different occupant/entity at the validated address.",
        }

    if status == "ADDRESS_MISMATCH":
        return {
            "review_routing": "REVIEW_LOW_CONFIDENCE_MISMATCH",
            "review_priority": "MEDIUM",
            "review_reason": "Address mismatch evidence exists but confidence is below the automatic no-enrich threshold.",
        }

    if status == "CORRECT_ADDRESS_CANDIDATE":
        return {
            "review_routing": "REVIEW_CORRECT_ADDRESS_CANDIDATE",
            "review_priority": "HIGH",
            "review_reason": "Company candidate was found at a different address; review for possible source address correction.",
        }

    if status == "API_ERROR":
        return {
            "review_routing": "REVIEW_API_ERROR",
            "review_priority": "MEDIUM",
            "review_reason": "Verification failed due to API error; retry or inspect service response.",
        }

    return {
        "review_routing": "DEFER_NO_EVIDENCE",
        "review_priority": "LOW",
        "review_reason": "No reliable company/address candidate was found; needs additional source coverage before human review.",
    }


def clamp_float(value, minimum: float = 0.0, maximum: float = 1.0) -> float:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return minimum
    if number > 1 and maximum == 1.0:
        number = number / 100
    return round(max(minimum, min(maximum, number)), 3)


def normalize_openai_url(api_url: Optional[str]) -> Optional[str]:
    if not api_url:
        return None
    value = api_url.strip()
    if not value:
        return None
    if not re.match(r"^https?://", value, flags=re.IGNORECASE):
        value = f"https://{value}"
    return value


def build_chat_completion_url(api_url: Optional[str], deployment: str, api_version: str) -> Optional[str]:
    if not api_url:
        return None
    value = api_url.rstrip("/")
    lowered = value.lower()
    if "/chat/completions" in lowered or "/responses" in lowered:
        return value
    return (
        f"{value}/openai/deployments/{deployment}/chat/completions"
        f"?api-version={api_version}"
    )


def openai_headers(api_url: str, api_key: str) -> dict:
    headers = {"Content-Type": "application/json"}
    lowered_url = (api_url or "").lower()
    is_azure_openai = (
        "openai.azure.com" in lowered_url
        or "/openai/deployments/" in lowered_url
        or os.environ.get("AZURE_OPENAI_API_KEY")
        or os.environ.get("AZURE_OPENAI_API_BASE")
    )
    if is_azure_openai:
        headers["api-key"] = api_key
    else:
        headers["Authorization"] = f"Bearer {api_key}"
    return headers


def load_test1_azure_openai_config() -> dict:
    """Read local Azure OpenAI defaults from test1.py without executing it."""
    path = Path("test1.py")
    if not path.exists():
        return {}
    try:
        tree = ast.parse(path.read_text(encoding="utf-8"))
    except Exception:
        return {}

    mapping = {
        "API_KEY": "api_key",
        "API_BASE": "api_base",
        "API_VERSION": "api_version",
        "MODEL": "model",
    }
    values = {}
    for node in tree.body:
        if not isinstance(node, ast.Assign):
            continue
        value = literal_or_getenv_default(node.value)
        if not value:
            continue
        for target in node.targets:
            if isinstance(target, ast.Name) and target.id in mapping:
                values[mapping[target.id]] = value
    return values


def literal_or_getenv_default(node) -> Optional[str]:
    if isinstance(node, ast.Constant) and isinstance(node.value, str):
        return node.value
    if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute):
        if node.func.attr == "getenv" and len(node.args) >= 2:
            default = node.args[1]
            if isinstance(default, ast.Constant) and isinstance(default.value, str):
                return default.value
    return None


def fuzzy_url_from_poi_url(poi_url: Optional[str]) -> Optional[str]:
    if not poi_url:
        return None
    return re.sub(r"/search/poi/json/?$", "/search/fuzzy/json", poi_url.rstrip("/"))


def safe_json_loads(value: str) -> Optional[dict]:
    if not value:
        return None
    text = value.strip()
    match = re.search(r"\{.*\}", text, flags=re.DOTALL)
    if match:
        text = match.group(0)
    try:
        parsed = json.loads(text)
        return parsed if isinstance(parsed, dict) else None
    except json.JSONDecodeError:
        return None


def main():
    parser = argparse.ArgumentParser(description="Run Week 3 company verification and enrichment.")
    parser.add_argument("input_csv", nargs="?", default="output_week2_live_full/stage2b_validated_only.csv")
    parser.add_argument("output_dir", nargs="?", default="output_week3")
    parser.add_argument("--limit", type=int, default=None, help="Optional record limit for smoke tests.")
    args = parser.parse_args()
    run_week3(args.input_csv, args.output_dir, args.limit)


if __name__ == "__main__":
    main()
