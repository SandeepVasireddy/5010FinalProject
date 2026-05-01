"""
MDM Pipeline — Stage 2B: Address Validation & Correction Engine
Week 2 Deliverable

This module implements:
1. Address Validation API integration (Azure Maps Search Address API)
   - With simulation/mock mode for development without API keys
2. Validation result parsing and diagnostic extraction
3. Address correction logic (auto-fix using API suggestions)
4. Retry/escalation flow for failed corrections
5. Validation reporting with per-record diagnostics

Architecture:
  Stage 1 output → Enhanced Preprocessing (2A) → Validation API (2B)
    ├─ VALIDATED → pass to Stage 3 (company verification)
    ├─ CORRECTED → auto-corrected, re-validated, pass to Stage 3
    └─ FAILED → flagged with diagnostics for manual review
"""

import pandas as pd
import numpy as np
import re
import json
import time
import hashlib
import asyncio
import os
import copy
import aiohttp
from typing import Optional
from pathlib import Path
from datetime import datetime
from dotenv import load_dotenv

# ──────────────────────────────────────────────
# 1. AZURE MAPS ADDRESS SEARCH API CLIENT
# ──────────────────────────────────────────────

class AddressValidationClient:
    """
    Client for Azure Maps Search Address API.
    Falls back to simulation mode when API key is not available.
    """

    DEFAULT_AZURE_ADDRESS_URL = "https://atlas.microsoft.com/search/address/json"

    def __init__(self, api_key: Optional[str] = None, api_url: Optional[str] = None,
                 use_simulation: bool = True):
        self.api_key = api_key
        self.api_url = api_url or os.environ.get("AZURE_MAPS_ADDRESS_URL", self.DEFAULT_AZURE_ADDRESS_URL)
        self.use_simulation = use_simulation if not api_key else False
        self.call_count = 0
        self.cache_hits = 0
        self._cache = {}
        self._inflight = {}
        self._cache_lock = asyncio.Lock()
        self.rate_limit_delay = 0.1  # seconds between API calls

        if self.use_simulation:
            print("[AddressValidation] Running in SIMULATION mode (no API key)")
        else:
            print("[AddressValidation] Using Azure Maps Search Address API")

    async def validate_address(self, address: str, country: str,
                               session: Optional[aiohttp.ClientSession] = None) -> dict:
        """Validate a single address. Returns structured result."""
        key = self._cache_key(address, country)

        async with self._cache_lock:
            cached = self._cache.get(key)
            if cached is not None:
                self.cache_hits += 1
                return copy.deepcopy(cached)

            task = self._inflight.get(key)
            if task is None:
                task = asyncio.create_task(self._validate_address_uncached(address, country, session))
                self._inflight[key] = task
                owner = True
            else:
                owner = False

        result = await task

        if owner:
            async with self._cache_lock:
                self._inflight.pop(key, None)
                if not result.get("api_error"):
                    self._cache[key] = copy.deepcopy(result)
        else:
            self.cache_hits += 1

        return copy.deepcopy(result)

    @staticmethod
    def _cache_key(address: str, country: str) -> tuple:
        """Normalize cache keys so repeated rows share one API response."""
        address_key = re.sub(r"\s+", " ", str(address).strip().lower())
        country_key = str(country).strip().upper()
        return address_key, country_key

    async def _validate_address_uncached(self, address: str, country: str,
                                         session: Optional[aiohttp.ClientSession] = None) -> dict:
        """Validate a single address without consulting the in-memory cache."""
        self.call_count += 1

        if self.use_simulation:
            return self._simulate_validation(address, country)

        return await self._call_azure_maps_api(address, country, session)

    async def _call_azure_maps_api(self, address: str, country: str,
                                   session: aiohttp.ClientSession) -> dict:
        """Call Azure Maps Search Address API."""
        params = {
            "api-version": "1.0",
            "subscription-key": self.api_key,
            "query": address,
            "limit": 1,
        }
        if country and len(country.strip()) == 2:
            params["countrySet"] = country.strip().upper()

        try:
            await asyncio.sleep(self.rate_limit_delay)
            async with session.get(
                self.api_url,
                params=params,
                timeout=aiohttp.ClientTimeout(total=10),
            ) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    return self._parse_azure_maps_response(data, address)
                else:
                    error_text = await resp.text()
                    return {
                        "validation_status": "API_ERROR",
                        "api_error": f"HTTP {resp.status}: {error_text[:200]}",
                        "original_address": address,
                    }
        except asyncio.TimeoutError:
            return {
                "validation_status": "API_ERROR",
                "api_error": "Request timeout",
                "original_address": address,
            }
        except Exception as e:
            return {
                "validation_status": "API_ERROR",
                "api_error": str(e)[:200],
                "original_address": address,
            }

    def _parse_azure_maps_response(self, data: dict, original: str) -> dict:
        """Parse Azure Maps search response into the pipeline's standard format."""
        results = data.get("results", [])
        if not results:
            return {
                "validation_status": "FAILED",
                "formatted_address": "",
                "latitude": None,
                "longitude": None,
                "validation_granularity": "NONE",
                "components": {},
                "diagnostics": ["address: no Azure Maps match"],
                "has_unconfirmed": True,
                "has_inferred": False,
                "has_replaced": False,
                "original_address": original,
                "api_error": None,
            }

        best = results[0]
        address = best.get("address", {})
        position = best.get("position", {})
        score = float(best.get("score", 0) or 0)
        result_type = self._normalize_azure_type(best.get("type", ""))
        entity_type = self._normalize_azure_type(best.get("entityType", ""))
        match_type = self._normalize_azure_type(best.get("matchType", ""))
        formatted = address.get("freeformAddress", "")

        components = {
            "street_number": {"value": address.get("streetNumber", ""), "confirmed": bool(address.get("streetNumber"))},
            "street_name": {"value": address.get("streetName", ""), "confirmed": bool(address.get("streetName"))},
            "municipality": {"value": address.get("municipality", ""), "confirmed": bool(address.get("municipality"))},
            "country_subdivision": {"value": address.get("countrySubdivision", ""), "confirmed": bool(address.get("countrySubdivision"))},
            "postal_code": {"value": address.get("postalCode", ""), "confirmed": bool(address.get("postalCode"))},
            "country": {"value": address.get("countryCode", ""), "confirmed": bool(address.get("countryCode"))},
        }

        granularity = self._azure_granularity(result_type, entity_type, match_type, components)
        has_unconfirmed = score < 0.80 or granularity in ("LOCALITY", "NONE", "OTHER")
        has_inferred = (
            score >= 0.80
            and not self._addresses_equivalent(formatted, original)
        )
        has_replaced = match_type in ("pointaddress", "addressrange") and has_inferred

        if score >= 0.80 and granularity in ("PREMISE", "SUB_PREMISE"):
            status = "CORRECTED" if has_inferred else "VALIDATED"
        elif score >= 0.70 and granularity in ("PREMISE", "SUB_PREMISE"):
            status = "PARTIAL_VALIDATED"
        elif (
            score >= 0.90
            and granularity == "ROUTE"
            and components["street_name"]["confirmed"]
        ):
            status = "PARTIAL_VALIDATED"
        elif (
            score >= 0.85
            and granularity == "ROUTE"
            and components["street_name"]["confirmed"]
            and self._route_partial_candidate(original)
        ):
            status = "PARTIAL_VALIDATED"
        elif score >= 0.60 and granularity in ("ROUTE", "LOCALITY"):
            status = "PARTIAL_MATCH"
        else:
            status = "FAILED"

        diagnostics = []
        for comp_type, comp_data in components.items():
            if not comp_data.get("confirmed"):
                diagnostics.append(f"{comp_type}: unconfirmed")
        diagnostics.append(f"azure_score: {score:.3f}")
        if result_type:
            diagnostics.append(f"azure_type: {result_type}")
        if match_type:
            diagnostics.append(f"match_type: {match_type}")

        return {
            "validation_status": status,
            "formatted_address": formatted,
            "latitude": position.get("lat"),
            "longitude": position.get("lon"),
            "validation_granularity": granularity,
            "components": components,
            "diagnostics": diagnostics,
            "has_unconfirmed": has_unconfirmed,
            "has_inferred": has_inferred,
            "has_replaced": has_replaced,
            "original_address": original,
            "api_error": None,
        }

    @staticmethod
    def _normalize_azure_type(value: str) -> str:
        """Normalize Azure result labels like 'Point Address' for simple comparisons."""
        return re.sub(r"[^a-z0-9]", "", str(value).lower())

    @staticmethod
    def _addresses_equivalent(left: str, right: str) -> bool:
        """Compare addresses while ignoring API-only formatting and common abbreviations."""
        def canonical(value: str) -> str:
            text = str(value).lower()
            replacements = {
                r"\bn\b": "north", r"\bs\b": "south", r"\be\b": "east", r"\bw\b": "west",
                r"\brd\b": "road", r"\brd\.\b": "road",
                r"\bave\b": "avenue", r"\bave\.\b": "avenue",
                r"\bst\b": "street", r"\bst\.\b": "street",
                r"\bln\b": "lane", r"\bln\.\b": "lane",
                r"\bdr\b": "drive", r"\bdr\.\b": "drive",
                r"\bblvd\b": "boulevard", r"\bblvd\.\b": "boulevard",
                r"\bsgt\b": "sargeant", r"\bste\b": "suite",
            }
            for pattern, replacement in replacements.items():
                text = re.sub(pattern, replacement, text)
            # Azure freeformAddress often omits the trailing country already supplied as countrySet.
            text = re.sub(r"\b(us|usa|united states|ca|canada|gb|uk|de|fr|es|it|au)\b", " ", text)
            text = re.sub(r"[^a-z0-9]+", " ", text)
            return re.sub(r"\s+", " ", text).strip()

        left_c = canonical(left)
        right_c = canonical(right)
        return bool(left_c and right_c) and (left_c == right_c or left_c in right_c or right_c in left_c)

    @staticmethod
    def _route_partial_candidate(original: str) -> bool:
        """Allow lower route scores only when the submitted street looks concrete."""
        text = str(original)
        if re.search(r"\b(x+x+|closed|tbd|operational|unknown|n/?a)\b", text, re.IGNORECASE):
            return False

        first_component = text.split(",", 1)[0].strip()
        has_numeric_street_ref = bool(re.search(r"\d", first_component))
        has_number_word = bool(re.match(
            r"(?i)^\s*(one|two|three|four|five|six|seven|eight|nine|ten)\b",
            first_component,
        ))
        return has_numeric_street_ref or has_number_word

    @staticmethod
    def _azure_granularity(result_type: str, entity_type: str, match_type: str,
                           components: dict) -> str:
        """Map Azure search result metadata to this pipeline's granularity terms."""
        best_type = match_type or result_type or entity_type
        if best_type == "pointaddress":
            return "PREMISE"
        if best_type == "addressrange":
            return "PREMISE" if components["street_number"]["confirmed"] else "ROUTE"
        if best_type in ("address", "streetaddress") and components["street_number"]["confirmed"]:
            return "PREMISE"
        if best_type in ("street", "streetname"):
            return "ROUTE"
        if best_type in ("geography", "municipality", "municipalitysubdivision", "countrysecondarysubdivision"):
            return "LOCALITY"
        return "OTHER"

    def _simulate_validation(self, address: str, country: str) -> dict:
        """
        Simulate validation for development without API keys.
        Uses deterministic heuristics based on address structure.
        """
        # Parse address components for heuristic validation
        has_number = bool(re.search(r"\d", address))
        has_street_word = bool(re.search(
            r"\b(Street|Road|Avenue|Boulevard|Drive|Lane|Way|Place|Court|"
            r"Calle|Avenida|Via|Rue|Straße|Rua|Corso|Viale|Piazza)\b",
            address, re.IGNORECASE
        ))
        parts = [p.strip() for p in address.split(",") if p.strip()]
        num_parts = len(parts)
        addr_length = len(address)

        # Use address hash for deterministic randomness
        hash_val = int(hashlib.md5(address.encode()).hexdigest()[:8], 16)
        hash_frac = (hash_val % 1000) / 1000.0

        # Heuristic scoring
        score = 0
        if has_number:
            score += 30
        if has_street_word:
            score += 25
        if num_parts >= 3:
            score += 20
        if num_parts >= 4:
            score += 10
        if addr_length > 20:
            score += 10
        if country in ("US", "GB", "CA", "AU", "DE", "FR"):
            score += 5

        # Determine status based on score + some pseudo-random variation
        diagnostics = []
        corrections = {}

        if score >= 70:
            # Likely valid — simulate VALIDATED or minor CORRECTED
            if hash_frac < 0.65:
                status = "VALIDATED"
            elif hash_frac < 0.90:
                status = "CORRECTED"
                # Simulate minor corrections
                if not re.search(r"\d{5}", address) and country == "US":
                    diagnostics.append("postal_code: inferred")
                    corrections["postal_code_inferred"] = True
                if hash_frac > 0.80:
                    diagnostics.append("street_number: confirmed with correction")
            else:
                status = "PARTIAL_MATCH"
                diagnostics.append("address: matched to route level only")
        elif score >= 40:
            # Partial match — higher failure rate
            if hash_frac < 0.30:
                status = "CORRECTED"
                diagnostics.append("street: partially matched and corrected")
                if not has_number:
                    diagnostics.append("street_number: missing, inferred from context")
            elif hash_frac < 0.60:
                status = "PARTIAL_MATCH"
                diagnostics.append("address: matched to route level only")
                if not has_number:
                    diagnostics.append("street_number: missing")
            else:
                status = "FAILED"
                diagnostics.append("address: insufficient match confidence")
                if not has_number:
                    diagnostics.append("street_number: missing")
                if not has_street_word:
                    diagnostics.append("street_name: unrecognized format")
        else:
            # Low score — very likely failure
            status = "FAILED"
            if not has_number:
                diagnostics.append("street_number: missing")
            if not has_street_word:
                diagnostics.append("street_name: unrecognized")
            if num_parts < 3:
                diagnostics.append("address: insufficient components")

        # Generate simulated formatted address (slightly cleaned version)
        formatted = address.strip()
        if status in ("VALIDATED", "CORRECTED", "PARTIAL_VALIDATED"):
            # Title case city names, standardize country
            formatted_parts = [p.strip() for p in formatted.split(",")]
            if len(formatted_parts) >= 2:
                formatted_parts[-1] = formatted_parts[-1].strip().upper()
            formatted = ", ".join(formatted_parts)

        # Simulated geocoordinates (deterministic from address hash)
        lat_base = {
            "US": 39.0, "CA": 45.0, "GB": 51.5, "DE": 51.0, "FR": 46.0,
            "ES": 40.0, "IT": 42.0, "MX": 23.0, "CN": 35.0, "AU": -33.0,
            "BR": -15.0, "PT": 39.0, "BE": 50.8, "NL": 52.3, "RU": 55.7,
        }.get(country, 40.0)
        lng_base = {
            "US": -98.0, "CA": -75.0, "GB": -0.1, "DE": 10.0, "FR": 2.3,
            "ES": -3.7, "IT": 12.5, "MX": -102.0, "CN": 105.0, "AU": 151.2,
            "BR": -47.9, "PT": -8.0, "BE": 4.4, "NL": 4.9, "RU": 37.6,
        }.get(country, 0.0)

        lat = lat_base + (hash_frac - 0.5) * 5
        lng = lng_base + ((hash_val % 100) / 100.0 - 0.5) * 10

        return {
            "validation_status": status,
            "formatted_address": formatted if status not in ("FAILED", "PARTIAL_MATCH") else "",
            "latitude": round(lat, 6) if status not in ("FAILED", "PARTIAL_MATCH") else None,
            "longitude": round(lng, 6) if status not in ("FAILED", "PARTIAL_MATCH") else None,
            "validation_granularity": {
                "VALIDATED": "PREMISE",
                "CORRECTED": "PREMISE",
                "PARTIAL_VALIDATED": "ROUTE",
                "PARTIAL_MATCH": "ROUTE",
                "FAILED": "OTHER",
            }[status],
            "components": {},
            "diagnostics": diagnostics,
            "has_unconfirmed": status in ("PARTIAL_MATCH", "FAILED"),
            "has_inferred": "inferred" in " ".join(diagnostics),
            "has_replaced": status == "CORRECTED",
            "original_address": address,
            "api_error": None,
            "simulated": True,
        }


# ──────────────────────────────────────────────
# 2. ADDRESS CORRECTION ENGINE
# ──────────────────────────────────────────────

class AddressCorrectionEngine:
    """
    Attempts to correct addresses that fail validation.
    Strategies:
    1. Component-level fixes (zip code correction, city-state realignment)
    2. Address simplification (remove suite/unit, reduce to core address)
    3. Fallback geocode search (submit just city + country)
    """

    # Common corrections by diagnostic type
    CORRECTION_STRATEGIES = [
        "component_fix",      # Fix specific components flagged by API
        "simplify_address",   # Remove noise (suite, apt, building info)
        "broaden_search",     # Use just city + state + country
    ]

    def __init__(self, client: AddressValidationClient):
        self.client = client
        self.correction_count = 0
        self.success_count = 0

    async def attempt_correction(self, original_address: str, country: str,
                                 validation_result: dict,
                                 session: Optional[aiohttp.ClientSession] = None) -> dict:
        """
        Attempt to correct a failed/partial address.
        Tries multiple strategies in order until one succeeds.
        """
        self.correction_count += 1
        diagnostics = validation_result.get("diagnostics", [])

        attempts = []

        for strategy in self.CORRECTION_STRATEGIES:
            corrected = self._apply_strategy(strategy, original_address, country, diagnostics)

            if corrected and corrected != original_address:
                # Re-validate the corrected address
                result = await self.client.validate_address(corrected, country, session)
                result["correction_strategy"] = strategy
                result["correction_input"] = corrected
                attempts.append(result)

                if result["validation_status"] in ("VALIDATED", "CORRECTED", "PARTIAL_VALIDATED"):
                    self.success_count += 1
                    return {
                        "correction_status": result["validation_status"],
                        "corrected_address": result.get("formatted_address", corrected),
                        "correction_strategy": strategy,
                        "correction_attempts": len(attempts),
                        "validation_result": result,
                        "all_attempts": attempts,
                    }

        # All strategies failed
        return {
            "correction_status": "FAILED",
            "corrected_address": None,
            "correction_strategy": None,
            "correction_attempts": len(attempts),
            "validation_result": validation_result,
            "all_attempts": attempts,
            "failure_reason": self._diagnose_failure(original_address, diagnostics),
        }

    def _apply_strategy(self, strategy: str, address: str,
                        country: str, diagnostics: list) -> Optional[str]:
        """Apply a specific correction strategy and return the modified address."""

        if strategy == "component_fix":
            corrected = address
            # Fix: missing or wrong postal code → remove it, let API infer
            if any("postal" in d for d in diagnostics):
                # Strip postal code (last component if it looks like one)
                parts = [p.strip() for p in corrected.split(",")]
                if parts and re.match(r"^[\d\s-]{3,10}$", parts[-1]):
                    corrected = ", ".join(parts[:-1])
                elif parts and re.match(r"^[A-Z0-9\s]{3,10}$", parts[-1]):
                    corrected = ", ".join(parts[:-1])

            score = None
            for diagnostic in diagnostics:
                match = re.search(r"azure_score: ([0-9.]+)", diagnostic)
                if match:
                    score = float(match.group(1))
                    break

            # Fix: street number issues → only strip the number on weak matches.
            if any("street_number" in d for d in diagnostics) and (score is None or score < 0.75):
                # Remove leading numbers
                corrected = re.sub(r"^\d+[-/]?\d*\s*", "", corrected)

            return corrected if corrected != address else None

        elif strategy == "simplify_address":
            corrected = address
            # Remove suite/apt/unit/floor info
            corrected = re.sub(r",?\s*(Suite|Ste|Apt|Unit|Floor|Fl|Room|Rm|Bldg|Building)\.?\s*\S*",
                             "", corrected, flags=re.IGNORECASE)
            # Remove parenthetical info
            corrected = re.sub(r"\s*\([^)]*\)", "", corrected)
            # Remove "No." numbers that might be internal codes
            corrected = re.sub(r"\s*No\.?\s*\d+", "", corrected)
            corrected = corrected.strip().rstrip(",").strip()
            return corrected if corrected != address else None

        elif strategy == "broaden_search":
            # Extract just city + state + country
            parts = [p.strip() for p in address.split(",")]
            if len(parts) >= 3:
                # Take last 2-3 components (city, state, country)
                broad = ", ".join(parts[-3:])
                return broad if broad != address else None
            elif len(parts) == 2:
                return address  # Already broad enough
            return None

        return None

    def _diagnose_failure(self, address: str, diagnostics: list) -> str:
        """Generate human-readable failure diagnosis."""
        reasons = []

        if not re.search(r"\d", address):
            reasons.append("No street number found in address")
        if len(address.strip()) < 10:
            reasons.append("Address too short to validate")
        if any("unrecognized" in d for d in diagnostics):
            reasons.append("Street name not recognized by validation service")
        if any("insufficient" in d for d in diagnostics):
            reasons.append("Too few address components for reliable matching")
        if not reasons:
            reasons.append("Address could not be matched to a known location after multiple correction attempts")

        return "; ".join(reasons)


# ──────────────────────────────────────────────
# 3. VALIDATION PIPELINE ORCHESTRATOR
# ──────────────────────────────────────────────

class ValidationPipeline:
    """
    Orchestrates the full validation + correction flow:
    1. Submit address to validation API
    2. If VALIDATED/CORRECTED → done
    3. If PARTIAL_MATCH/FAILED → attempt corrections
    4. If still failed → flag for manual review with diagnostics
    """

    def __init__(self, api_key: Optional[str] = None, max_retries: int = 3):
        self.client = AddressValidationClient(api_key=api_key)
        self.corrector = AddressCorrectionEngine(self.client)
        self.max_retries = max_retries
        self.stats = {
            "total": 0,
            "validated": 0,
            "corrected": 0,
            "partial": 0,
            "failed": 0,
            "api_errors": 0,
            "skipped": 0,
        }

    async def validate_batch(self, df: pd.DataFrame, batch_size: int = 10) -> pd.DataFrame:
        """Validate all records in the DataFrame."""

        print("=" * 60)
        print("Stage 2B: Address Validation & Correction")
        print("=" * 60)

        # Initialize result columns
        result_cols = [
            "validation_status", "validated_address", "latitude", "longitude",
            "validation_granularity", "validation_diagnostics",
            "correction_status", "correction_strategy", "correction_attempts",
            "failure_reason", "manual_review_flag", "validation_timestamp",
        ]
        for col in result_cols:
            df[col] = None

        # Process in batches
        total = len(df)
        connector = aiohttp.TCPConnector(limit=batch_size)

        async with aiohttp.ClientSession(connector=connector) as session:
            for start in range(0, total, batch_size):
                end = min(start + batch_size, total)
                batch_indices = df.index[start:end]

                tasks = []
                for idx in batch_indices:
                    row = df.loc[idx]
                    tasks.append(self._process_record(idx, row, session))

                results = await asyncio.gather(*tasks, return_exceptions=True)

                for idx, result in zip(batch_indices, results):
                    if isinstance(result, Exception):
                        df.at[idx, "validation_status"] = "API_ERROR"
                        df.at[idx, "failure_reason"] = str(result)[:200]
                        df.at[idx, "manual_review_flag"] = True
                        self.stats["api_errors"] += 1
                    else:
                        for key, value in result.items():
                            if key in result_cols:
                                df.at[idx, key] = value

                # Progress
                processed = min(end, total)
                print(f"  Processed {processed}/{total} records "
                      f"(V:{self.stats['validated']} C:{self.stats['corrected']} "
                      f"P:{self.stats['partial']} F:{self.stats['failed']} "
                      f"S:{self.stats['skipped']})")

        self._print_summary(total)
        return df

    async def _process_record(self, idx: int, row: pd.Series,
                              session: aiohttp.ClientSession) -> dict:
        """Process a single record through validation + correction flow."""
        self.stats["total"] += 1
        timestamp = datetime.now().isoformat()

        # Get the API-ready address
        api_address = str(row.get("api_address", ""))
        country = str(row.get("suggested_country", row.get("norm_country", "")))
        completeness = str(row.get("completeness_class", ""))
        quality_tier = str(row.get("quality_tier", ""))

        # Skip records that can't be validated
        if not api_address.strip() or completeness == "EMPTY":
            self.stats["skipped"] += 1
            return {
                "validation_status": "SKIPPED",
                "failure_reason": "No usable address for API submission",
                "manual_review_flag": True,
                "validation_timestamp": timestamp,
            }

        # Step 1: Initial validation
        result = await self.client.validate_address(api_address, country, session)

        if result.get("api_error"):
            self.stats["api_errors"] += 1
            return {
                "validation_status": "API_ERROR",
                "failure_reason": result["api_error"],
                "manual_review_flag": True,
                "validation_timestamp": timestamp,
            }

        status = result["validation_status"]

        # Step 2: Handle results
        if status == "VALIDATED":
            self.stats["validated"] += 1
            return {
                "validation_status": "VALIDATED",
                "validated_address": result.get("formatted_address", ""),
                "latitude": result.get("latitude"),
                "longitude": result.get("longitude"),
                "validation_granularity": result.get("validation_granularity", ""),
                "validation_diagnostics": json.dumps(result.get("diagnostics", [])),
                "correction_status": None,
                "correction_strategy": None,
                "correction_attempts": 0,
                "failure_reason": None,
                "manual_review_flag": False,
                "validation_timestamp": timestamp,
            }

        elif status == "CORRECTED":
            self.stats["corrected"] += 1
            return {
                "validation_status": "CORRECTED",
                "validated_address": result.get("formatted_address", ""),
                "latitude": result.get("latitude"),
                "longitude": result.get("longitude"),
                "validation_granularity": result.get("validation_granularity", ""),
                "validation_diagnostics": json.dumps(result.get("diagnostics", [])),
                "correction_status": "API_CORRECTED",
                "correction_strategy": "api_auto",
                "correction_attempts": 1,
                "failure_reason": None,
                "manual_review_flag": False,
                "validation_timestamp": timestamp,
            }

        elif status == "PARTIAL_VALIDATED":
            self.stats["partial"] += 1
            return {
                "validation_status": "PARTIAL_VALIDATED",
                "validated_address": result.get("formatted_address", ""),
                "latitude": result.get("latitude"),
                "longitude": result.get("longitude"),
                "validation_granularity": result.get("validation_granularity", ""),
                "validation_diagnostics": json.dumps(result.get("diagnostics", [])),
                "correction_status": "API_PARTIAL",
                "correction_strategy": "api_partial",
                "correction_attempts": 0,
                "failure_reason": None,
                "manual_review_flag": False,
                "validation_timestamp": timestamp,
            }

        else:
            # Step 3: Attempt correction for PARTIAL_MATCH or FAILED
            correction = await self.corrector.attempt_correction(
                api_address, country, result, session
            )

            if correction["correction_status"] in ("CORRECTED", "PARTIAL_VALIDATED"):
                final_result = correction["validation_result"]
                final_status = correction["correction_status"]
                if final_status == "CORRECTED":
                    self.stats["corrected"] += 1
                else:
                    self.stats["partial"] += 1
                return {
                    "validation_status": final_status,
                    "validated_address": correction.get("corrected_address", ""),
                    "latitude": final_result.get("latitude"),
                    "longitude": final_result.get("longitude"),
                    "validation_granularity": final_result.get("validation_granularity", ""),
                    "validation_diagnostics": json.dumps(final_result.get("diagnostics", [])),
                    "correction_status": "ENGINE_CORRECTED" if final_status == "CORRECTED" else "ENGINE_PARTIAL",
                    "correction_strategy": correction.get("correction_strategy", ""),
                    "correction_attempts": correction.get("correction_attempts", 0),
                    "failure_reason": None,
                    "manual_review_flag": False,
                    "validation_timestamp": timestamp,
                }
            else:
                self.stats["failed"] += 1
                return {
                    "validation_status": "FAILED",
                    "validated_address": None,
                    "latitude": None,
                    "longitude": None,
                    "validation_granularity": result.get("validation_granularity", ""),
                    "validation_diagnostics": json.dumps(result.get("diagnostics", [])),
                    "correction_status": "EXHAUSTED",
                    "correction_strategy": None,
                    "correction_attempts": correction.get("correction_attempts", 0),
                    "failure_reason": correction.get("failure_reason", "Unknown"),
                    "manual_review_flag": True,
                    "validation_timestamp": timestamp,
                }

    def _print_summary(self, total: int):
        """Print validation summary statistics."""
        print(f"\n{'='*60}")
        print("Validation Summary")
        print(f"{'='*60}")
        print(f"  Total processed:      {self.stats['total']}")
        print(f"  Validated (clean):     {self.stats['validated']} "
              f"({100*self.stats['validated']/max(total,1):.1f}%)")
        print(f"  Corrected:             {self.stats['corrected']} "
              f"({100*self.stats['corrected']/max(total,1):.1f}%)")
        print(f"  Partial validated:     {self.stats['partial']} "
              f"({100*self.stats['partial']/max(total,1):.1f}%)")
        print(f"  Failed:                {self.stats['failed']} "
              f"({100*self.stats['failed']/max(total,1):.1f}%)")
        print(f"  Skipped:               {self.stats['skipped']} "
              f"({100*self.stats['skipped']/max(total,1):.1f}%)")
        print(f"  API errors:            {self.stats['api_errors']}")
        print(f"  API calls made:        {self.client.call_count}")
        print(f"  API cache hits:        {self.client.cache_hits}")
        print(f"  Correction attempts:   {self.corrector.correction_count}")
        print(f"  Correction successes:  {self.corrector.success_count}")

        exact_success = self.stats['validated'] + self.stats['corrected']
        usable_success = exact_success + self.stats['partial']
        print(f"\n  Exact success rate:    {exact_success}/{total} "
              f"({100*exact_success/max(total,1):.1f}%)")
        print(f"  Usable success rate:   {usable_success}/{total} "
              f"({100*usable_success/max(total,1):.1f}%)")
        manual_review = self.stats["failed"] + self.stats["skipped"] + self.stats["api_errors"]
        print(f"  Manual review queue:   {manual_review} records")
        print(f"{'='*60}")


# ──────────────────────────────────────────────
# 4. VALIDATION REPORT GENERATOR
# ──────────────────────────────────────────────

def generate_validation_report(df: pd.DataFrame, output_dir: str) -> dict:
    """Generate comprehensive validation report."""
    report = {
        "generated_at": datetime.now().isoformat(),
        "total_records": len(df),
    }

    # Validation status distribution
    report["validation_distribution"] = (
        df["validation_status"].value_counts().to_dict()
    )

    # Correction breakdown
    report["correction_distribution"] = (
        df["correction_status"].fillna("N/A").value_counts().to_dict()
    )

    exact_mask = df["validation_status"].isin(["VALIDATED", "CORRECTED"])
    usable_mask = df["validation_status"].isin(["VALIDATED", "CORRECTED", "PARTIAL_VALIDATED"])
    report["success_summary"] = {
        "exact_success": int(exact_mask.sum()),
        "exact_success_rate": round(100 * exact_mask.sum() / max(len(df), 1), 1),
        "usable_success": int(usable_mask.sum()),
        "usable_success_rate": round(100 * usable_mask.sum() / max(len(df), 1), 1),
        "partial_validated": int((df["validation_status"] == "PARTIAL_VALIDATED").sum()),
    }

    # Success by country
    country_stats = {}
    for country in df["norm_country"].unique():
        subset = df[df["norm_country"] == country]
        total = len(subset)
        validated = (subset["validation_status"] == "VALIDATED").sum()
        corrected = (subset["validation_status"] == "CORRECTED").sum()
        partial = (subset["validation_status"] == "PARTIAL_VALIDATED").sum()
        failed = (subset["validation_status"] == "FAILED").sum()
        country_stats[country] = {
            "total": total,
            "validated": int(validated),
            "corrected": int(corrected),
            "partial_validated": int(partial),
            "failed": int(failed),
            "exact_success_rate": round(100 * (validated + corrected) / max(total, 1), 1),
            "usable_success_rate": round(100 * (validated + corrected + partial) / max(total, 1), 1),
        }
    report["country_breakdown"] = country_stats

    # Success by quality tier
    tier_stats = {}
    for tier in df["quality_tier"].unique():
        subset = df[df["quality_tier"] == tier]
        total = len(subset)
        exact_success = subset["validation_status"].isin(["VALIDATED", "CORRECTED"]).sum()
        usable_success = subset["validation_status"].isin(["VALIDATED", "CORRECTED", "PARTIAL_VALIDATED"]).sum()
        tier_stats[tier] = {
            "total": total,
            "exact_success": int(exact_success),
            "exact_success_rate": round(100 * exact_success / max(total, 1), 1),
            "usable_success": int(usable_success),
            "usable_success_rate": round(100 * usable_success / max(total, 1), 1),
        }
    report["tier_breakdown"] = tier_stats

    # Success by completeness class
    comp_stats = {}
    for cls in df["completeness_class"].unique():
        subset = df[df["completeness_class"] == cls]
        total = len(subset)
        exact_success = subset["validation_status"].isin(["VALIDATED", "CORRECTED"]).sum()
        usable_success = subset["validation_status"].isin(["VALIDATED", "CORRECTED", "PARTIAL_VALIDATED"]).sum()
        comp_stats[cls] = {
            "total": total,
            "exact_success": int(exact_success),
            "exact_success_rate": round(100 * exact_success / max(total, 1), 1),
            "usable_success": int(usable_success),
            "usable_success_rate": round(100 * usable_success / max(total, 1), 1),
        }
    report["completeness_breakdown"] = comp_stats

    # Failure analysis
    failed = df[df["validation_status"] == "FAILED"]
    report["failure_analysis"] = {
        "total_failed": len(failed),
        "failure_reasons": failed["failure_reason"].value_counts().to_dict() if len(failed) > 0 else {},
        "sample_failures": [],
    }
    for _, row in failed.head(10).iterrows():
        report["failure_analysis"]["sample_failures"].append({
            "MDM_KEY": str(row["MDM_KEY"]),
            "SOURCE_NAME": row["SOURCE_NAME"],
            "api_address": row.get("api_address", ""),
            "country": row["norm_country"],
            "failure_reason": row.get("failure_reason", ""),
            "quality_tier": row.get("quality_tier", ""),
            "completeness_class": row.get("completeness_class", ""),
        })

    # Manual review queue
    manual = df[df["manual_review_flag"] == True]
    report["manual_review"] = {
        "total": len(manual),
        "by_country": manual["norm_country"].value_counts().to_dict() if len(manual) > 0 else {},
    }

    # Correction effectiveness
    corrections = df[df["correction_status"].notna() & (df["correction_status"] != "N/A")]
    report["correction_effectiveness"] = {
        "total_attempted": len(corrections),
        "strategy_breakdown": corrections["correction_strategy"].value_counts().to_dict() if len(corrections) > 0 else {},
    }

    # Save
    report_path = Path(output_dir) / "validation_report.json"
    with open(report_path, "w") as f:
        json.dump(report, f, indent=2, default=str)
    print(f"\n[Report] Saved to {report_path}")

    return report


# ──────────────────────────────────────────────
# 5. MAIN RUNNER
# ──────────────────────────────────────────────

async def run_validation_pipeline(input_csv: str, output_dir: str = "output",
                                   api_key: Optional[str] = None) -> pd.DataFrame:
    """Run the complete validation pipeline."""
    load_dotenv()
    if not api_key:
        api_key = os.environ.get("AZURE_MAPS_KEY", None)

    Path(output_dir).mkdir(parents=True, exist_ok=True)

    # Load enhanced preprocessed data
    df = pd.read_csv(input_csv)
    print(f"[Validation] Loaded {len(df)} records from {input_csv}")

    # Initialize and run pipeline
    pipeline = ValidationPipeline(api_key=api_key)
    df = await pipeline.validate_batch(df)

    # Generate report
    report = generate_validation_report(df, output_dir)

    # Save outputs
    output_path = Path(output_dir) / "stage2b_validated.csv"
    df.to_csv(output_path, index=False)
    print(f"[Output] Validated data saved to {output_path}")

    # Save usable validated subset, including route/medium-confidence partial matches.
    validated = df[df["validation_status"].isin(["VALIDATED", "CORRECTED", "PARTIAL_VALIDATED"])]
    validated_path = Path(output_dir) / "stage2b_validated_only.csv"
    validated.to_csv(validated_path, index=False)
    print(f"[Output] Usable validated records: {len(validated)}/{len(df)} saved to {validated_path}")

    # Save manual review queue
    manual = df[df["manual_review_flag"] == True]
    if len(manual) > 0:
        manual_path = Path(output_dir) / "stage2b_manual_review.csv"
        manual.to_csv(manual_path, index=False)
        print(f"[Output] Manual review queue: {len(manual)} saved to {manual_path}")

    return df


def main():
    import sys
    import os

    load_dotenv()

    input_csv = sys.argv[1] if len(sys.argv) > 1 else "output/stage2a_enhanced.csv"
    output_dir = sys.argv[2] if len(sys.argv) > 2 else "output"
    api_key = os.environ.get("AZURE_MAPS_KEY", None)

    df = asyncio.run(run_validation_pipeline(input_csv, output_dir, api_key))


if __name__ == "__main__":
    main()
