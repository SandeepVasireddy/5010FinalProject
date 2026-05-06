"""
MDM Pipeline — Stage 2A: Enhanced Preprocessing
Week 2 Deliverable

Builds on Stage 1 output to apply:
1. Category-specific preprocessing rules per address_sub_category/name_sub_category
2. Advanced street normalization (multilingual abbreviations, directionals, unit/suite)
3. Multilingual handling: language detection, Unicode API strategy, and translation gap flags
4. Address completeness scoring and flagging for records without callable addresses
5. Country-code cross-validation (detect mismatches between address content and tagged country)
"""

import pandas as pd
import numpy as np
import re
import json
import os
import urllib.error
import urllib.parse
import urllib.request
import uuid
from typing import Optional

# ──────────────────────────────────────────────
# 1. MULTILINGUAL ABBREVIATION EXPANSION
# ──────────────────────────────────────────────

# Extended abbreviation maps by language
ABBREVIATIONS = {
    "en": {
        r"\bSt\b(?!\.)": "Street", r"\bSt\.": "Street",
        r"\bRd\b": "Road", r"\bRd\.": "Road",
        r"\bBlvd\b": "Boulevard", r"\bBlvd\.": "Boulevard",
        r"\bAve\b": "Avenue", r"\bAve\.": "Avenue",
        r"\bDr\b(?!\.)": "Drive", r"\bDr\.": "Drive",
        r"\bLn\b": "Lane", r"\bLn\.": "Lane",
        r"\bCt\b": "Court", r"\bCt\.": "Court",
        r"\bPkwy\b": "Parkway", r"\bHwy\b": "Highway",
        r"\bPl\b": "Place", r"\bCir\b": "Circle",
        r"\bSte\b": "Suite", r"\bSte\.": "Suite",
        r"\bApt\b": "Apartment", r"\bApt\.": "Apartment",
        r"\bFl\b(?=\s*\d)": "Floor",  # "Fl 3" → "Floor 3", but not "Fl" alone
        r"\bN\b(?=\s+\w+\s+(St|Ave|Rd|Blvd))": "North",
        r"\bS\b(?=\s+\w+\s+(St|Ave|Rd|Blvd))": "South",
        r"\bE\b(?=\s+\w+\s+(St|Ave|Rd|Blvd))": "East",
        r"\bW\b(?=\s+\w+\s+(St|Ave|Rd|Blvd))": "West",
    },
    "es": {
        r"\bAv\.?(?=\s)": "Avenida",
        r"\bC/": "Calle ",
        r"\bRda\.": "Ronda",
        r"\bCtra\.": "Carretera",
        r"\bPza\.": "Plaza",
        r"\bPº\.?(?=\s)": "Paseo",
        r"\bUrb\.": "Urbanización",
        r"\bEdif\.": "Edificio",
        r"\bS/[Nn]": "Sin Número",
    },
    "de": {
        r"\bStr\.": "Straße",
        r"\bstr\.": "straße",
        r"\bPl\.": "Platz",
        r"\bpl\.": "platz",
    },
    "it": {
        r"\bV\.le\b": "Viale",
        r"\bV\.(?=\s)": "Via",
        r"\bP\.zza\b": "Piazza",
        r"\bPzza\.?(?=\s)": "Piazza",
        r"\bC\.so\b": "Corso",
        r"\bL\.go\b": "Largo",
    },
    "pt": {
        r"\bR\.(?=\s)": "Rua",
        r"\bAv\.(?=\s)": "Avenida",
        r"\bTrav\.": "Travessa",
        r"\bEstr\.": "Estrada",
    },
    "fr": {
        r"\bRue\b": "Rue",  # no-op, but keeps consistency
        r"\bBd\.?(?=\s)": "Boulevard",
        r"\bAv\.(?=\s)": "Avenue",
        r"\bPl\.(?=\s)": "Place",
    },
}

# Mexican / Latin American state abbreviations
MX_STATE_ABBREVS = {
    "AGS.": "Aguascalientes", "BC.": "Baja California", "BCS.": "Baja California Sur",
    "CAMP.": "Campeche", "CHIS.": "Chiapas", "CHIH.": "Chihuahua",
    "CDMX": "Ciudad de México", "COAH.": "Coahuila", "COL.": "Colima",
    "DGO.": "Durango", "GTO.": "Guanajuato", "GRO.": "Guerrero",
    "HGO.": "Hidalgo", "JAL.": "Jalisco", "MEX.": "México",
    "MICH.": "Michoacán", "MOR.": "Morelos", "NAY.": "Nayarit",
    "NL.": "Nuevo León", "OAX.": "Oaxaca", "PUE.": "Puebla",
    "QRO.": "Querétaro", "QROO.": "Quintana Roo", "SLP.": "San Luis Potosí",
    "SIN.": "Sinaloa", "SON.": "Sonora", "TAB.": "Tabasco",
    "TAMPS.": "Tamaulipas", "TLAX.": "Tlaxcala", "VER.": "Veracruz",
    "YUC.": "Yucatán", "ZAC.": "Zacatecas",
}

ES_STATE_ABBREVS = {
    "GU": "Guadalajara", "M": "Madrid", "B": "Barcelona", "V": "Valencia",
}


class AzureTranslatorClient:
    """Optional Azure Translator client for non-Latin address/name fields."""

    def __init__(self):
        enabled_flag = os.environ.get("ENABLE_TRANSLATION", "false").strip().lower()
        self.enabled = enabled_flag in ("1", "true", "yes")
        self.key = os.environ.get("AZURE_TRANSLATOR_KEY", "")
        self.endpoint = os.environ.get("AZURE_TRANSLATOR_ENDPOINT", "https://api.cognitive.microsofttranslator.com")
        self.region = os.environ.get("AZURE_TRANSLATOR_REGION", "")
        self.call_count = 0
        self.error_count = 0
        if self.enabled and self.key:
            print("[Translator] Azure Translator enabled")
        elif self.enabled:
            print("[Translator] ENABLE_TRANSLATION is true but AZURE_TRANSLATOR_KEY is missing")
            self.enabled = False

    def translate_fields(self, fields: dict, from_lang: str) -> dict:
        if not self.enabled:
            return {
                "translated_fields": {},
                "translation_handling_status": "TRANSLATION_NOT_CONFIGURED",
                "translation_gap_reason": "Azure Translator is not enabled/configured; Unicode source fields are retained.",
            }

        values = [str(value or "").strip() for value in fields.values()]
        names = list(fields.keys())
        nonempty = [(name, value) for name, value in zip(names, values) if value]
        if not nonempty:
            return {
                "translated_fields": {},
                "translation_handling_status": "TRANSLATION_SKIPPED_EMPTY_FIELDS",
                "translation_gap_reason": "No non-empty fields were available for translation.",
            }

        route = "/translate?" + urllib.parse.urlencode({"api-version": "3.0", "to": "en"})
        if from_lang and from_lang not in ("unknown", "nan"):
            route += "&" + urllib.parse.urlencode({"from": from_lang})
        request = urllib.request.Request(
            self.endpoint.rstrip("/") + route,
            data=json.dumps([{"text": value} for _, value in nonempty], ensure_ascii=False).encode("utf-8"),
            headers=self._headers(),
            method="POST",
        )

        try:
            with urllib.request.urlopen(request, timeout=20) as response:
                payload = json.loads(response.read().decode("utf-8"))
            self.call_count += 1
        except (urllib.error.URLError, urllib.error.HTTPError, TimeoutError, json.JSONDecodeError) as exc:
            self.error_count += 1
            return {
                "translated_fields": {},
                "translation_handling_status": "TRANSLATION_ERROR",
                "translation_gap_reason": f"Azure Translator failed; Unicode source fields retained. {str(exc)[:120]}",
            }

        translated = {}
        for (name, _), item in zip(nonempty, payload):
            translations = item.get("translations", []) if isinstance(item, dict) else []
            if translations:
                translated[name] = translations[0].get("text", "")

        return {
            "translated_fields": translated,
            "translation_handling_status": "TRANSLATED_TO_ENGLISH",
            "translation_gap_reason": "",
        }

    def _headers(self) -> dict:
        headers = {
            "Ocp-Apim-Subscription-Key": self.key,
            "Content-Type": "application/json",
            "X-ClientTraceId": str(uuid.uuid4()),
        }
        if self.region:
            headers["Ocp-Apim-Subscription-Region"] = self.region
        return headers


def expand_abbreviations(street: str, lang: str) -> tuple:
    """Expand street abbreviations based on detected language. Returns (expanded, actions)."""
    actions = []
    result = street

    # Always apply the detected language's abbreviations
    lang_key = lang if lang in ABBREVIATIONS else "en"
    for pattern, replacement in ABBREVIATIONS.get(lang_key, {}).items():
        if re.search(pattern, result):
            result = re.sub(pattern, replacement, result, count=1)
            actions.append(f"expanded_{pattern[:15]}")

    # Also apply English if not already English (many addresses mix languages)
    if lang_key != "en":
        for pattern, replacement in ABBREVIATIONS["en"].items():
            if re.search(pattern, result):
                result = re.sub(pattern, replacement, result, count=1)
                actions.append(f"expanded_en_{pattern[:15]}")

    return result, actions


def expand_state(state: str, country: str) -> tuple:
    """Expand state abbreviations based on country. Returns (expanded, action)."""
    upper = state.strip().upper()
    if country == "MX":
        # Try exact match, then with period
        for abbr, full in MX_STATE_ABBREVS.items():
            if upper == abbr or upper == abbr.rstrip("."):
                return full, f"expanded_state_{state}"
    if country == "ES" and upper in ES_STATE_ABBREVS:
        return ES_STATE_ABBREVS[upper], f"expanded_es_state_{state}"
    return state, None


# ──────────────────────────────────────────────
# 2. ADDRESS STRUCTURE NORMALIZATION
# ──────────────────────────────────────────────

def normalize_postal_code(postal: str, country: str) -> tuple:
    """Normalize postal codes by country format. Returns (normalized, action)."""
    postal = str(postal).strip()
    if not postal or postal == "nan":
        return "", None

    action = None

    if country == "US":
        # US ZIP: 5 digits or 5+4
        digits = re.sub(r"[^0-9]", "", postal)
        if len(digits) == 9:
            postal = f"{digits[:5]}-{digits[5:]}"
            action = "formatted_zip_plus4"
        elif len(digits) >= 5:
            postal = digits[:5]
            action = "normalized_zip5"

    elif country == "CA":
        # Canada: A1A 1A1
        clean = re.sub(r"\s+", "", postal).upper()
        if len(clean) == 6 and re.match(r"[A-Z]\d[A-Z]\d[A-Z]\d", clean):
            postal = f"{clean[:3]} {clean[3:]}"
            action = "formatted_ca_postal"

    elif country in ("GB", "GG", "JE"):
        # UK/Crown Dependency postcodes: ensure space before last 3 chars.
        clean = re.sub(r"\s+", "", postal).upper()
        if len(clean) >= 5:
            postal = f"{clean[:-3]} {clean[-3:]}"
            action = "formatted_uk_postal"

    elif country in ("DE", "ES", "IT", "FR", "BE", "NL", "AT"):
        # European: typically 4-5 digits
        digits = re.sub(r"[^0-9]", "", postal)
        if digits:
            postal = digits
            action = "cleaned_eu_postal"

    elif country == "BR":
        # Brazil: 8 digits, format as XXXXX-XXX
        digits = re.sub(r"[^0-9]", "", postal)
        if len(digits) == 8:
            postal = f"{digits[:5]}-{digits[5:]}"
            action = "formatted_br_postal"

    elif country in ("CN", "RU", "SA", "MX", "CL", "GT"):
        digits = re.sub(r"[^0-9]", "", postal)
        if digits:
            postal = digits
            action = f"cleaned_{country.lower()}_postal"

    return postal, action


def clean_street_structure(street: str) -> tuple:
    """
    Clean structural issues in street addresses:
    - Embedded newlines → comma separation
    - Double commas/spaces
    - Leading/trailing punctuation
    """
    actions = []

    # Embedded newlines
    if "\n" in street:
        street = re.sub(r"\s*\n+\s*", ", ", street).strip()
        actions.append("fixed_newlines")

    # Multiple commas
    if ",," in street or ", ," in street:
        street = re.sub(r",\s*,+", ",", street)
        actions.append("fixed_double_commas")

    # Leading/trailing commas or periods
    street = re.sub(r"^[,.\s]+|[,.\s]+$", "", street).strip()

    # Multiple spaces
    street = re.sub(r"\s{2,}", " ", street)

    # Normalize N°/Nº to No.
    if re.search(r"N[º°]", street):
        street = re.sub(r"N[º°]\.?\s*", "No. ", street)
        actions.append("normalized_number_sign")

    return street, actions


# ──────────────────────────────────────────────
# 3. COUNTRY CROSS-VALIDATION
# ──────────────────────────────────────────────

# CJK pattern for detecting Chinese addresses
CJK_PATTERN = re.compile(r"[\u4e00-\u9fff\u3400-\u4dbf]")

# Known city → country mappings for common mismatches
CITY_COUNTRY_MAP = {
    "beijing": "CN", "shanghai": "CN", "shenzhen": "CN", "guangzhou": "CN",
    "london": "GB", "manchester": "GB", "birmingham": "GB",
    "paris": "FR", "lyon": "FR", "marseille": "FR",
    "moscow": "RU", "saint petersburg": "RU",
}


def infer_country_from_components(street: str, city: str, state: str,
                                  tagged_country: str, postal: str) -> tuple:
    """Return a high-confidence corrected country code, if one is evident."""
    country = tagged_country.strip().upper()
    state_u = state.strip().upper()
    postal_u = re.sub(r"\s+", "", postal.strip().upper())
    combined = f"{street} {city} {state}".lower()

    if country == "GU":
        if postal_u.startswith("GY") or "guernsey" in combined:
            return "GG", "GU code is Guam; address content indicates Guernsey"
        if postal_u.startswith("JE") or "jersey" in combined:
            return "JE", "GU code is Guam; address content indicates Jersey"
        if "guatemala" in combined:
            return "GT", "GU code is Guam; address content indicates Guatemala"
        if state_u == "GU" or "guadalajara" in combined or postal_u.startswith(("18", "19")):
            return "ES", "GU appears to be Spanish province Guadalajara, not country Guam"

    return country, None

def cross_validate_country(street: str, city: str, state: str,
                           tagged_country: str, name: str, postal: str = "") -> dict:
    """
    Cross-validate the tagged country code against address content.
    Returns dict with validation result and suggested correction.
    """
    result = {
        "country_validated": True,
        "country_mismatch_reason": None,
        "suggested_country": tagged_country,
    }

    inferred_country, inferred_reason = infer_country_from_components(
        street, city, state, tagged_country, postal
    )
    if inferred_reason:
        result["country_validated"] = False
        result["country_mismatch_reason"] = inferred_reason
        result["suggested_country"] = inferred_country

    # Check 1: CJK characters in address → should be CN (not HK, TW unless city matches)
    combined = f"{street} {city} {name}"
    if CJK_PATTERN.search(combined):
        if result["suggested_country"] not in ("CN", "TW", "HK", "MO", "JP", "KR"):
            result["country_validated"] = False
            result["country_mismatch_reason"] = "CJK characters in address but country is not CJK"
            result["suggested_country"] = "CN"
        elif result["suggested_country"] == "HK" and ("北京" in combined or "上海" in combined or
                                          "深圳" in combined or "广州" in combined):
            result["country_validated"] = False
            result["country_mismatch_reason"] = f"Address contains mainland China city but tagged as HK"
            result["suggested_country"] = "CN"

    # Check 2: Known city → country mismatch
    city_lower = city.strip().lower()
    if city_lower in CITY_COUNTRY_MAP:
        expected = CITY_COUNTRY_MAP[city_lower]
        if result["suggested_country"] != expected:
            # Only flag if high confidence (exact city match)
            result["country_validated"] = False
            result["country_mismatch_reason"] = f"City '{city}' typically in {expected}, tagged as {tagged_country}"
            result["suggested_country"] = expected

    return result


# ──────────────────────────────────────────────
# 4. ADDRESS COMPLETENESS SCORING
# ──────────────────────────────────────────────

def score_address_completeness(street: str, city: str, state: str,
                                country: str, postal: str) -> dict:
    """
    Score address completeness for API readiness.
    Returns classification: FULL / PARTIAL / MINIMAL / EMPTY
    """
    scores = {
        "has_street": bool(street.strip()) and len(street.strip()) > 2,
        "has_street_number": bool(re.search(r"\d", street)) if street else False,
        "has_city": bool(city.strip()) and len(city.strip()) > 1,
        "has_state": bool(state.strip()) and state.strip() != "nan",
        "has_country": bool(country.strip()) and len(country.strip()) == 2,
        "has_postal": bool(postal.strip()) and postal.strip() != "nan" and len(postal.strip()) >= 3,
    }

    score = sum(scores.values())
    total = len(scores)

    # Classification logic
    if score >= 5 and scores["has_street"] and scores["has_street_number"]:
        classification = "FULL"
    elif score >= 3 and scores["has_city"]:
        classification = "PARTIAL"
    elif score >= 2:
        classification = "MINIMAL"
    else:
        classification = "EMPTY"

    return {
        "completeness_score": round(score / total, 2),
        "completeness_class": classification,
        "completeness_details": scores,
    }


# ──────────────────────────────────────────────
# 5. NON-LATIN SCRIPT TRANSLITERATION PREP
# ──────────────────────────────────────────────

def prepare_for_api(street: str, city: str, state: str, country: str,
                    postal: str, name: str, lang: str) -> dict:
    """
    Prepare address for API call.
    For non-Latin scripts, compose the address string in a format the API can handle.
    Azure Maps supports Unicode queries, but some scripts may still benefit from transliteration.
    """
    # Compose the full address string for API submission
    parts = []
    if street.strip():
        parts.append(street.strip())
    if city.strip():
        parts.append(city.strip())
    if state.strip() and state.strip() != "nan" and state.strip().upper() != country.strip().upper():
        parts.append(state.strip())
    if postal.strip() and postal.strip() != "nan":
        parts.append(postal.strip())
    if country.strip():
        parts.append(country.strip())

    api_address = ", ".join(parts)

    # Determine if translation/transliteration would be beneficial for downstream review.
    # Azure Maps can accept Unicode queries, so the pipeline keeps the original address
    # rather than guessing a lossy translation.
    needs_translation = lang in ("zh", "ja", "ko", "ru", "ar", "he", "th")
    api_strategy = "direct"  # default
    translation_handling_status = "NOT_NEEDED_LATIN_SCRIPT"
    translation_gap_reason = ""

    if needs_translation:
        if lang in ("zh", "ja", "ko"):
            # Submit CJK addresses as-is; Azure Maps can search Unicode address text.
            api_strategy = "direct_cjk"
            translation_handling_status = "DIRECT_UNICODE_NO_TRANSLATION"
            translation_gap_reason = "CJK record kept in Unicode for Azure Maps; full translation is not automated."
        else:
            api_strategy = "needs_transliteration"
            translation_handling_status = "TRANSLITERATION_RECOMMENDED"
            translation_gap_reason = "Non-Latin record flagged for transliteration/translation review; full translation is not automated."

    return {
        "api_address": api_address,
        "api_strategy": api_strategy,
        "needs_translation": needs_translation,
        "translation_handling_status": translation_handling_status,
        "translation_gap_reason": translation_gap_reason,
    }


# ──────────────────────────────────────────────
# 6. MAIN PREPROCESSING PIPELINE
# ──────────────────────────────────────────────

def run_enhanced_preprocessing(df: pd.DataFrame) -> pd.DataFrame:
    """
    Run enhanced preprocessing on Stage 1 output.
    Adds: expanded abbreviations, normalized postal codes, country validation,
    completeness scoring, and API-ready address composition.
    """
    print("=" * 60)
    print("Stage 2A: Enhanced Preprocessing")
    print("=" * 60)

    all_actions = []
    translator = AzureTranslatorClient()

    for idx in df.index:
        row = df.loc[idx]
        actions = []

        # 1. Expand abbreviations by language
        street = str(row.get("norm_street", row["parsed_street"]))
        lang = str(row.get("detected_lang", "en"))
        expanded_street, abbr_actions = expand_abbreviations(street, lang)
        actions.extend(abbr_actions)

        # 2. Clean street structure
        expanded_street, struct_actions = clean_street_structure(expanded_street)
        actions.extend(struct_actions)

        # 3. Expand state abbreviation
        state = str(row.get("norm_state", row["parsed_state"]))
        country = str(row.get("norm_country", row["parsed_country"]))
        expanded_state, state_action = expand_state(state, country)
        if state_action:
            actions.append(state_action)

        # 4. Normalize postal code
        postal = str(row.get("norm_postal", row["parsed_postal"]))
        norm_postal, postal_action = normalize_postal_code(postal, country)
        if postal_action:
            actions.append(postal_action)

        # Store enhanced fields
        city = str(row.get("norm_city", row["parsed_city"]))
        name = str(row.get("cleaned_name", row["SOURCE_NAME"]))
        needs_translation = lang in ("zh", "ja", "ko", "ru", "ar", "he", "th")

        translated = {
            "street": "",
            "city": "",
            "state": "",
            "name": "",
        }
        translation_status_override = None
        translation_reason_override = None
        if needs_translation:
            translated_result = translator.translate_fields(
                {
                    "street": expanded_street,
                    "city": city,
                    "state": expanded_state,
                    "name": name,
                },
                lang,
            )
            translated.update(translated_result.get("translated_fields", {}))
            translation_status_override = translated_result.get("translation_handling_status")
            translation_reason_override = translated_result.get("translation_gap_reason")

            expanded_street_for_api = translated.get("street") or expanded_street
            city_for_api = translated.get("city") or city
            state_for_api = translated.get("state") or expanded_state
        else:
            expanded_street_for_api = expanded_street
            city_for_api = city
            state_for_api = expanded_state

        df.at[idx, "enhanced_street"] = expanded_street
        df.at[idx, "enhanced_state"] = expanded_state
        df.at[idx, "enhanced_postal"] = norm_postal
        df.at[idx, "translated_street"] = translated["street"]
        df.at[idx, "translated_city"] = translated["city"]
        df.at[idx, "translated_state"] = translated["state"]
        df.at[idx, "translated_name"] = translated["name"]

        # 5. Country cross-validation
        cv = cross_validate_country(expanded_street, city, expanded_state, country, name, norm_postal)
        df.at[idx, "country_validated"] = cv["country_validated"]
        df.at[idx, "country_mismatch_reason"] = cv["country_mismatch_reason"] or ""
        df.at[idx, "suggested_country"] = cv["suggested_country"]

        # 6. Completeness scoring
        api_country = cv["suggested_country"]  # Use corrected country if mismatch detected
        cs = score_address_completeness(expanded_street, city, expanded_state, api_country, norm_postal)
        df.at[idx, "completeness_score"] = cs["completeness_score"]
        df.at[idx, "completeness_class"] = cs["completeness_class"]

        # 7. API preparation
        ap = prepare_for_api(expanded_street_for_api, city_for_api, state_for_api,
                             api_country, norm_postal, name, lang)
        if translation_status_override:
            ap["translation_handling_status"] = translation_status_override
        if translation_reason_override is not None:
            ap["translation_gap_reason"] = translation_reason_override
        df.at[idx, "api_address"] = ap["api_address"]
        df.at[idx, "api_strategy"] = ap["api_strategy"]
        df.at[idx, "needs_translation"] = ap["needs_translation"]
        df.at[idx, "translation_handling_status"] = ap["translation_handling_status"]
        df.at[idx, "translation_gap_reason"] = ap["translation_gap_reason"]

        # Store preprocessing actions
        df.at[idx, "enhanced_preprocessing_actions"] = json.dumps(actions)
        all_actions.extend(actions)

    # Summary
    print(f"\n[Enhanced Preprocessing] Applied {len(all_actions)} actions across {len(df)} records")
    print(f"  Completeness distribution:")
    for cls in ["FULL", "PARTIAL", "MINIMAL", "EMPTY"]:
        count = (df["completeness_class"] == cls).sum()
        print(f"    {cls}: {count} ({100*count/len(df):.1f}%)")

    mismatches = (df["country_validated"] == False).sum()
    print(f"  Country mismatches detected: {mismatches}")

    print(f"  API strategy distribution:")
    for strat in df["api_strategy"].unique():
        count = (df["api_strategy"] == strat).sum()
        print(f"    {strat}: {count}")

    print(f"  Translation handling distribution:")
    for status in df["translation_handling_status"].fillna("UNKNOWN").unique():
        count = (df["translation_handling_status"] == status).sum()
        print(f"    {status}: {count}")
    print(f"  Azure Translator calls: {translator.call_count}")
    print(f"  Azure Translator errors: {translator.error_count}")

    return df


if __name__ == "__main__":
    import sys
    input_csv = sys.argv[1] if len(sys.argv) > 1 else "output/stage1_processed.csv"
    df = pd.read_csv(input_csv)
    df = run_enhanced_preprocessing(df)
    df.to_csv("output/stage2a_enhanced.csv", index=False)
    print(f"\n[Output] Saved to output/stage2a_enhanced.csv")
