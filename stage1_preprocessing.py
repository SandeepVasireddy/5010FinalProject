"""
MDM Address Validation & Enrichment Pipeline
Stage 1: Data Ingestion, Parsing, Preprocessing & EDA

This module handles:
1. CSV ingestion and address field parsing
2. Encoding repair (mojibake fix via ftfy)
3. Company name cleaning (strip parentheticals, codes, dept descriptors)
4. Address normalization (abbreviation expansion, delimiter cleanup)
5. Language detection per record
6. Quality flagging (missing fields, non-street addresses, garbled text)
7. Full EDA report generation
"""

import pandas as pd
import numpy as np
import re
import json
import ftfy
from collections import Counter, defaultdict
from langdetect import detect, LangDetectException
from datetime import datetime
from pathlib import Path

# ──────────────────────────────────────────────
# 1. DATA INGESTION & ADDRESS PARSING
# ──────────────────────────────────────────────

ADDRESS_DELIMITER = "|#|#"

def load_and_parse(csv_path: str) -> pd.DataFrame:
    """Load raw MDM CSV and parse the delimited FULL_ADDRESS into components."""
    df = pd.read_csv(csv_path, dtype=str)
    df.columns = df.columns.str.strip()

    # Parse FULL_ADDRESS into components
    addr_parts = df["FULL_ADDRESS"].str.split(re.escape(ADDRESS_DELIMITER), expand=True)

    # Normalize to exactly 5 columns
    for i in range(5):
        if i not in addr_parts.columns:
            addr_parts[i] = ""

    df["parsed_street"]  = addr_parts[0].fillna("").str.strip()
    df["parsed_city"]    = addr_parts[1].fillna("").str.strip()
    df["parsed_state"]   = addr_parts[2].fillna("").str.strip()
    df["parsed_country"] = addr_parts[3].fillna("").str.strip()
    df["parsed_postal"]  = addr_parts[4].fillna("").str.strip()

    print(f"[Ingestion] Loaded {len(df)} records with {len(df.columns)} columns")
    return df


# ──────────────────────────────────────────────
# 2. ENCODING REPAIR
# ──────────────────────────────────────────────

# Common mojibake patterns: UTF-8 bytes decoded as Latin-1/Windows-1252
MOJIBAKE_MARKERS = re.compile(r"[Ã¡Ã©Ã³ÃºÃ±Ã¼Ã¶Ã¤Ã¨Ã²ÃŸÃ¢Ã®Ã´Ã»Ã§ÃÂ]")

def repair_encoding(text: str) -> str:
    """Use ftfy to fix mojibake and normalize unicode."""
    if pd.isna(text) or text == "":
        return text
    fixed = ftfy.fix_text(text)
    # Additional pass: fix double-encoded UTF-8
    try:
        if "Ã" in fixed or "Â" in fixed:
            fixed = fixed.encode("latin-1").decode("utf-8")
    except (UnicodeDecodeError, UnicodeEncodeError):
        pass
    return fixed

def apply_encoding_repair(df: pd.DataFrame) -> pd.DataFrame:
    """Repair encoding on all text fields."""
    text_cols = ["SOURCE_NAME", "FULL_ADDRESS",
                 "parsed_street", "parsed_city", "parsed_state",
                 "name_reason", "address_reason"]

    repair_count = 0
    for col in text_cols:
        if col in df.columns:
            original = df[col].copy()
            df[col] = df[col].apply(repair_encoding)
            changed = (original != df[col]).sum()
            repair_count += changed
            if changed > 0:
                print(f"  [Encoding] Repaired {changed} values in '{col}'")

    df["flag_encoding_repaired"] = False
    # Mark rows where street or name changed
    for col in ["SOURCE_NAME", "parsed_street", "parsed_city", "parsed_state"]:
        if col in df.columns:
            pass  # We track this in the flags step below

    print(f"[Encoding] Total field-level repairs: {repair_count}")
    return df


# ──────────────────────────────────────────────
# 3. COMPANY NAME CLEANING
# ──────────────────────────────────────────────

# Parenthetical patterns: "(carreras)", "(02544)", "(gal)", "( 706)"
PARENS_PATTERN = re.compile(r"\s*\([^)]*\)\s*$")

# Department/location codes at end: "- Recap W", "- Mot", "Markt 7540"
DEPT_CODE_PATTERNS = [
    re.compile(r"\s*-\s*Recap\s+\w+$", re.IGNORECASE),
    re.compile(r"\s*-\s*Mot$", re.IGNORECASE),
    re.compile(r"\s*-\s*\d+$"),
]

# Attention/care-of: "C/o Focus", "C/o Firenze"
CARE_OF_PATTERN = re.compile(r"\s+C/[oO]\s+.*$", re.IGNORECASE)

# Alphanumeric location codes: "Nbc213443", standalone number codes
ALPHANUM_CODE = re.compile(r"\s+[A-Z]{2,4}\d{4,}$")

# "Run Rate", "Willis Run Rate" type descriptors
RUN_RATE_PATTERN = re.compile(r"\s+Run\s+Rate$", re.IGNORECASE)

# Honeywell internal codes: "300505 -", "505 ("
HONEYWELL_CODE = re.compile(r"\s+\d{3,6}\s*[-–]?\s*$")

def clean_company_name(name: str, sub_category: str) -> dict:
    """
    Clean company name based on its sub_category.
    Returns dict with cleaned_name, removed_parts, and cleaning_actions.
    """
    if pd.isna(name):
        return {"cleaned_name": "", "removed_parts": [], "cleaning_actions": []}

    original = name.strip()
    cleaned = original
    removed = []
    actions = []

    # 1. Remove parenthetical descriptors
    match = PARENS_PATTERN.search(cleaned)
    if match:
        removed.append(match.group().strip())
        cleaned = PARENS_PATTERN.sub("", cleaned).strip()
        actions.append("removed_parenthetical")

    # 2. Remove C/O descriptors
    match = CARE_OF_PATTERN.search(cleaned)
    if match:
        removed.append(match.group().strip())
        cleaned = CARE_OF_PATTERN.sub("", cleaned).strip()
        actions.append("removed_care_of")

    # 3. Remove department/recap codes
    for pattern in DEPT_CODE_PATTERNS:
        match = pattern.search(cleaned)
        if match:
            removed.append(match.group().strip())
            cleaned = pattern.sub("", cleaned).strip()
            actions.append("removed_dept_code")

    # 4. Remove alphanumeric codes (Nbc213443)
    match = ALPHANUM_CODE.search(cleaned)
    if match:
        removed.append(match.group().strip())
        cleaned = ALPHANUM_CODE.sub("", cleaned).strip()
        actions.append("removed_alphanum_code")

    # 5. Remove "Run Rate" suffixes
    match = RUN_RATE_PATTERN.search(cleaned)
    if match:
        removed.append(match.group().strip())
        cleaned = RUN_RATE_PATTERN.sub("", cleaned).strip()
        actions.append("removed_run_rate")

    # 6. Clean up trailing hyphens/dashes left over
    cleaned = re.sub(r"\s*[-–—]\s*$", "", cleaned).strip()

    # 7. Normalize whitespace
    cleaned = re.sub(r"\s{2,}", " ", cleaned).strip()

    return {
        "cleaned_name": cleaned,
        "removed_parts": removed,
        "cleaning_actions": actions,
    }


def apply_name_cleaning(df: pd.DataFrame) -> pd.DataFrame:
    """Apply company name cleaning to all records."""
    results = df.apply(
        lambda row: clean_company_name(row["SOURCE_NAME"], row.get("name_sub_category", "")),
        axis=1,
    )
    results_df = pd.DataFrame(results.tolist())
    df["cleaned_name"] = results_df["cleaned_name"]
    df["name_removed_parts"] = results_df["removed_parts"].apply(json.dumps)
    df["name_cleaning_actions"] = results_df["cleaning_actions"].apply(json.dumps)

    modified = (df["SOURCE_NAME"].str.strip() != df["cleaned_name"]).sum()
    print(f"[Name Cleaning] Modified {modified}/{len(df)} company names")
    return df


# ──────────────────────────────────────────────
# 4. ADDRESS NORMALIZATION
# ──────────────────────────────────────────────

# Abbreviation mappings by language/region
STREET_ABBREVIATIONS = {
    # English
    "St": "Street", "St.": "Street", "Rd": "Road", "Rd.": "Road",
    "Blvd": "Boulevard", "Blvd.": "Boulevard", "Ave": "Avenue", "Ave.": "Avenue",
    "Dr": "Drive", "Dr.": "Drive", "Ln": "Lane", "Ln.": "Lane",
    "Ct": "Court", "Ct.": "Court", "Pkwy": "Parkway", "Hwy": "Highway",
    "Pl": "Place", "Pl.": "Place", "Cir": "Circle",
    # German
    "Str.": "Straße", "str.": "straße",
    # Spanish
    "Av.": "Avenida", "C/": "Calle ", "Rda.": "Ronda",
    "Ctra.": "Carretera", "Pza.": "Plaza",
    # Italian
    "V.le": "Viale", "V.": "Via",
    # Portuguese
    "R.": "Rua", "Av.": "Avenida",
}

# State abbreviation normalization for common cases
STATE_ABBREVIATIONS = {
    "VER.": "Veracruz", "MICH.": "Michoacán", "HGO.": "Hidalgo",
    "CDMX": "Ciudad de México", "JAL.": "Jalisco",
}

def normalize_address(street: str, city: str, state: str, country: str, postal: str) -> dict:
    """
    Normalize address components:
    - Expand abbreviations
    - Clean embedded newlines
    - Standardize formatting
    """
    actions = []

    # Clean embedded newlines in street
    if "\n" in str(street):
        street = re.sub(r"\n+", ", ", street).strip()
        actions.append("fixed_embedded_newlines")

    # Expand street abbreviations (word-boundary aware)
    normalized_street = street
    for abbr, full in STREET_ABBREVIATIONS.items():
        if abbr.endswith("."):
            pattern = re.escape(abbr)
        else:
            pattern = r"\b" + re.escape(abbr) + r"\b"
        if re.search(pattern, normalized_street):
            normalized_street = re.sub(pattern, full, normalized_street, count=1)
            actions.append(f"expanded_{abbr}")

    # Normalize "Nº" / "N°" to "No." (only actual number-sign patterns, not regular words)
    normalized_street = re.sub(r"N[º°]\.?\s*", "No. ", normalized_street)

    # Clean up double spaces
    normalized_street = re.sub(r"\s{2,}", " ", normalized_street).strip()

    # Normalize state abbreviations
    normalized_state = state.strip()
    if normalized_state.upper() in STATE_ABBREVIATIONS:
        old_state = normalized_state
        normalized_state = STATE_ABBREVIATIONS[normalized_state.upper()]
        actions.append(f"expanded_state_{old_state}")

    # Normalize postal code: strip spaces for consistency in lookups
    normalized_postal = postal.strip()

    # Normalize city: title case if ALL CAPS
    normalized_city = city.strip()
    if normalized_city.isupper() and len(normalized_city) > 2:
        normalized_city = normalized_city.title()
        actions.append("titlecased_city")

    return {
        "norm_street": normalized_street,
        "norm_city": normalized_city,
        "norm_state": normalized_state,
        "norm_country": country.strip().upper(),
        "norm_postal": normalized_postal,
        "normalization_actions": actions,
    }


def apply_address_normalization(df: pd.DataFrame) -> pd.DataFrame:
    """Apply address normalization to all records."""
    results = df.apply(
        lambda row: normalize_address(
            row["parsed_street"], row["parsed_city"],
            row["parsed_state"], row["parsed_country"], row["parsed_postal"]
        ),
        axis=1,
    )
    results_df = pd.DataFrame(results.tolist())
    for col in ["norm_street", "norm_city", "norm_state", "norm_country", "norm_postal"]:
        df[col] = results_df[col]
    df["normalization_actions"] = results_df["normalization_actions"].apply(json.dumps)

    modified = (results_df["normalization_actions"].apply(len) > 0).sum()
    print(f"[Address Norm] Normalized {modified}/{len(df)} addresses")
    return df


# ──────────────────────────────────────────────
# 5. LANGUAGE DETECTION
# ──────────────────────────────────────────────

# CJK Unicode ranges
CJK_PATTERN = re.compile(r"[\u4e00-\u9fff\u3400-\u4dbf]")
CYRILLIC_PATTERN = re.compile(r"[\u0400-\u04ff]")
ARABIC_PATTERN = re.compile(r"[\u0600-\u06ff]")

def detect_language(name: str, street: str, country: str) -> dict:
    """
    Detect the language of a record using script detection + langdetect.
    Returns detected language code and detection method.
    """
    combined = f"{name} {street}".strip()

    # Script-based detection first (more reliable for CJK/Cyrillic)
    if CJK_PATTERN.search(combined):
        return {"detected_lang": "zh", "lang_method": "script_cjk"}
    if CYRILLIC_PATTERN.search(combined):
        return {"detected_lang": "ru", "lang_method": "script_cyrillic"}
    if ARABIC_PATTERN.search(combined):
        return {"detected_lang": "ar", "lang_method": "script_arabic"}

    # Country-based heuristic for short text
    country_lang_map = {
        "US": "en", "GB": "en", "CA": "en", "AU": "en", "IE": "en", "GU": "en",
        "ES": "es", "MX": "es", "AR": "es", "CL": "es", "HN": "es",
        "DE": "de", "IT": "it", "PT": "pt", "BR": "pt",
        "FR": "fr", "BE": "fr", "NL": "nl",
        "CN": "zh", "HK": "zh", "RU": "ru", "KZ": "ru",
    }

    # Try langdetect on combined text
    try:
        if len(combined) > 10:
            detected = detect(combined)
            return {"detected_lang": detected, "lang_method": "langdetect"}
    except LangDetectException:
        pass

    # Fallback to country mapping
    lang = country_lang_map.get(country.upper(), "unknown")
    return {"detected_lang": lang, "lang_method": "country_fallback"}


def apply_language_detection(df: pd.DataFrame) -> pd.DataFrame:
    """Detect language for all records."""
    results = df.apply(
        lambda row: detect_language(
            row.get("cleaned_name", row["SOURCE_NAME"]),
            row["parsed_street"],
            row["parsed_country"]
        ),
        axis=1,
    )
    results_df = pd.DataFrame(results.tolist())
    df["detected_lang"] = results_df["detected_lang"]
    df["lang_method"] = results_df["lang_method"]

    lang_dist = df["detected_lang"].value_counts()
    print(f"[Language Detection] Distribution: {dict(lang_dist)}")
    return df


# ──────────────────────────────────────────────
# 6. QUALITY FLAGGING
# ──────────────────────────────────────────────

def flag_quality_issues(df: pd.DataFrame) -> pd.DataFrame:
    """Flag records with quality issues that need special handling."""
    flags = pd.DataFrame(index=df.index)

    # Flag: Missing street address
    flags["flag_no_street"] = df["parsed_street"].apply(
        lambda x: len(str(x).strip()) == 0
    )

    # Flag: Street looks like county/region name (no numbers)
    flags["flag_no_street_number"] = df["parsed_street"].apply(
        lambda x: bool(x.strip()) and not bool(re.search(r"\d", str(x)))
    )

    # Flag: Missing city
    flags["flag_no_city"] = df["parsed_city"].apply(
        lambda x: len(str(x).strip()) == 0
    )

    # Flag: Missing state/region
    flags["flag_no_state"] = df["parsed_state"].apply(
        lambda x: len(str(x).strip()) == 0
    )

    # Flag: Missing postal code
    flags["flag_no_postal"] = df["parsed_postal"].apply(
        lambda x: len(str(x).strip()) == 0
    )

    # Flag: Garbled/unreadable text (corrupted multibyte)
    def is_garbled(text):
        if pd.isna(text) or text == "":
            return False
        # Garbled UTF-8 as Latin-1 produces patterns like Ã followed by another byte
        garble_ratio = len(re.findall(r"[Ã¡-ÿ]{2,}", str(text))) / max(len(str(text)), 1)
        return garble_ratio > 0.1
    flags["flag_garbled_text"] = df.apply(
        lambda row: is_garbled(row["norm_street"]) or is_garbled(row["cleaned_name"]),
        axis=1,
    )

    # Flag: Encoding was repaired
    flags["flag_encoding_repaired"] = df.apply(
        lambda row: row["parsed_street"] != row["norm_street"]
        or row["SOURCE_NAME"] != row.get("cleaned_name", row["SOURCE_NAME"]),
        axis=1,
    )

    # Flag: Very long/complex street (might contain multiple addresses)
    flags["flag_complex_address"] = df["norm_street"].apply(
        lambda x: len(str(x)) > 80
    )

    # Flag: Name contains internal codes that may confuse matching
    flags["flag_name_has_codes"] = df["name_sub_category"].isin([
        "Company Location and Department Codes",
        "Company Names with Parenthetical Descriptors",
        "Company Names with Contact or Attention Descriptors",
    ])

    # Flag: Non-Latin script (needs translation for API calls)
    flags["flag_non_latin"] = df.apply(
        lambda row: bool(CJK_PATTERN.search(str(row["norm_street"]) + str(row["cleaned_name"])))
        or bool(CYRILLIC_PATTERN.search(str(row["norm_street"]) + str(row["cleaned_name"]))),
        axis=1,
    )

    # Composite: API-ready (no critical flags)
    flags["api_ready"] = ~(
        flags["flag_no_street"]
        | flags["flag_garbled_text"]
        | flags["flag_no_city"]
    )

    # Composite: Needs translation before API
    flags["needs_translation"] = flags["flag_non_latin"] & flags["api_ready"]

    # Composite: Quality tier
    def assign_tier(row):
        if row["flag_no_street"] or row["flag_garbled_text"]:
            return "CRITICAL"  # Cannot be sent to API without major intervention
        if row["flag_no_state"] or row["flag_no_postal"] or row["flag_no_street_number"]:
            return "INCOMPLETE"  # Can attempt API but may fail
        if row["flag_complex_address"] or row["flag_non_latin"]:
            return "NEEDS_PREP"  # Needs translation or simplification first
        return "READY"  # Can be sent directly to validation API

    flags["quality_tier"] = flags.apply(assign_tier, axis=1)

    for col in flags.columns:
        df[col] = flags[col]

    # Print summary
    tier_dist = flags["quality_tier"].value_counts()
    print(f"[Quality Flags] Tier distribution:")
    for tier, count in tier_dist.items():
        print(f"  {tier}: {count} ({100*count/len(df):.1f}%)")

    flag_cols = [c for c in flags.columns if c.startswith("flag_")]
    print(f"[Quality Flags] Flag summary:")
    for col in flag_cols:
        count = flags[col].sum()
        if count > 0:
            print(f"  {col}: {count} ({100*count/len(df):.1f}%)")

    return df


# ──────────────────────────────────────────────
# 7. EDA REPORT GENERATION
# ──────────────────────────────────────────────

def generate_eda_report(df: pd.DataFrame, output_dir: str) -> dict:
    """Generate comprehensive EDA report as a dict and save to JSON."""
    report = {
        "generated_at": datetime.now().isoformat(),
        "total_records": len(df),
        "columns": list(df.columns),
    }

    # --- Country distribution ---
    report["country_distribution"] = (
        df["parsed_country"].value_counts().to_dict()
    )

    # --- Source system distribution ---
    report["source_system_distribution"] = (
        df["ROWID_SYSTEM"].value_counts().to_dict()
    )

    # --- Name sub-category distribution ---
    report["name_subcategory_distribution"] = (
        df["name_sub_category"].value_counts().to_dict()
    )

    # --- Language distribution ---
    report["language_distribution"] = (
        df["detected_lang"].value_counts().to_dict()
    )
    report["language_method_distribution"] = (
        df["lang_method"].value_counts().to_dict()
    )

    # --- Quality tier distribution ---
    report["quality_tier_distribution"] = (
        df["quality_tier"].value_counts().to_dict()
    )

    # --- Flag counts ---
    flag_cols = [c for c in df.columns if c.startswith("flag_")]
    report["flag_counts"] = {col: int(df[col].sum()) for col in flag_cols}

    # --- Missing field analysis ---
    report["missing_fields"] = {
        "no_street": int(df["flag_no_street"].sum()),
        "no_city": int(df["flag_no_city"].sum()),
        "no_state": int(df["flag_no_state"].sum()),
        "no_postal": int(df["flag_no_postal"].sum()),
        "no_street_number": int(df["flag_no_street_number"].sum()),
    }

    # --- Encoding issues ---
    report["encoding_issues"] = {
        "records_repaired": int(df["flag_encoding_repaired"].sum()),
        "garbled_unrecoverable": int(df["flag_garbled_text"].sum()),
    }

    # --- Name cleaning impact ---
    name_modified = (df["SOURCE_NAME"].str.strip() != df["cleaned_name"]).sum()
    report["name_cleaning"] = {
        "records_modified": int(name_modified),
        "pct_modified": round(100 * name_modified / len(df), 1),
    }
    # Action breakdown
    all_actions = []
    for acts_json in df["name_cleaning_actions"]:
        all_actions.extend(json.loads(acts_json))
    report["name_cleaning"]["action_breakdown"] = dict(Counter(all_actions))

    # --- Normalization impact ---
    norm_modified = df["normalization_actions"].apply(
        lambda x: len(json.loads(x)) > 0
    ).sum()
    report["address_normalization"] = {
        "records_modified": int(norm_modified),
        "pct_modified": round(100 * norm_modified / len(df), 1),
    }
    all_norm_actions = []
    for acts_json in df["normalization_actions"]:
        all_norm_actions.extend(json.loads(acts_json))
    report["address_normalization"]["action_breakdown"] = dict(Counter(all_norm_actions))

    # --- Country x quality tier crosstab ---
    ct = pd.crosstab(df["parsed_country"], df["quality_tier"])
    report["country_quality_crosstab"] = ct.to_dict()

    # --- Sample problematic records ---
    problematic = df[df["quality_tier"].isin(["CRITICAL", "INCOMPLETE"])].head(10)
    report["sample_problematic_records"] = []
    for _, row in problematic.iterrows():
        report["sample_problematic_records"].append({
            "MDM_KEY": str(row["MDM_KEY"]),
            "SOURCE_NAME": row["SOURCE_NAME"],
            "cleaned_name": row["cleaned_name"],
            "street": row["parsed_street"],
            "norm_street": row["norm_street"],
            "city": row["parsed_city"],
            "country": row["parsed_country"],
            "quality_tier": row["quality_tier"],
            "flags": {c: bool(row[c]) for c in flag_cols if row[c]},
        })

    # Save report
    report_path = Path(output_dir) / "eda_report.json"
    with open(report_path, "w") as f:
        json.dump(report, f, indent=2, ensure_ascii=False, default=str)
    print(f"[EDA] Report saved to {report_path}")

    return report


# ──────────────────────────────────────────────
# 8. MAIN PIPELINE RUNNER
# ──────────────────────────────────────────────

def run_stage1(csv_path: str, output_dir: str = "output") -> pd.DataFrame:
    """
    Run the complete Stage 1 pipeline:
    Ingest → Encoding Repair → Name Cleaning → Address Normalization
    → Language Detection → Quality Flagging → EDA Report
    """
    Path(output_dir).mkdir(parents=True, exist_ok=True)

    print("=" * 60)
    print("MDM Pipeline — Stage 1: Preprocessing & EDA")
    print("=" * 60)

    # Step 1: Ingest
    print("\n--- Step 1: Data Ingestion & Parsing ---")
    df = load_and_parse(csv_path)

    # Step 2: Encoding repair
    print("\n--- Step 2: Encoding Repair ---")
    df = apply_encoding_repair(df)

    # Step 3: Name cleaning
    print("\n--- Step 3: Company Name Cleaning ---")
    df = apply_name_cleaning(df)

    # Step 4: Address normalization
    print("\n--- Step 4: Address Normalization ---")
    df = apply_address_normalization(df)

    # Step 5: Language detection
    print("\n--- Step 5: Language Detection ---")
    df = apply_language_detection(df)

    # Step 6: Quality flagging
    print("\n--- Step 6: Quality Flagging ---")
    df = flag_quality_issues(df)

    # Step 7: EDA report
    print("\n--- Step 7: EDA Report ---")
    report = generate_eda_report(df, output_dir)

    # Save processed output
    output_path = Path(output_dir) / "stage1_processed.csv"
    df.to_csv(output_path, index=False)
    print(f"\n[Output] Processed data saved to {output_path}")

    # Save API-ready subset
    api_ready = df[df["api_ready"] == True]
    api_path = Path(output_dir) / "stage1_api_ready.csv"
    api_ready.to_csv(api_path, index=False)
    print(f"[Output] API-ready records: {len(api_ready)}/{len(df)} saved to {api_path}")

    # Save flagged records needing manual review
    flagged = df[df["quality_tier"].isin(["CRITICAL"])]
    if len(flagged) > 0:
        flagged_path = Path(output_dir) / "stage1_manual_review.csv"
        flagged.to_csv(flagged_path, index=False)
        print(f"[Output] Manual review records: {len(flagged)} saved to {flagged_path}")

    print("\n" + "=" * 60)
    print("Stage 1 Complete!")
    print(f"  Total records:     {len(df)}")
    print(f"  API-ready:         {len(api_ready)} ({100*len(api_ready)/len(df):.1f}%)")
    print(f"  Needs prep:        {(df['quality_tier']=='NEEDS_PREP').sum()}")
    print(f"  Incomplete:        {(df['quality_tier']=='INCOMPLETE').sum()}")
    print(f"  Critical:          {(df['quality_tier']=='CRITICAL').sum()}")
    print("=" * 60)

    return df


if __name__ == "__main__":
    import sys
    csv_path = sys.argv[1] if len(sys.argv) > 1 else "/mnt/user-data/uploads/100_sample_MDM.csv"
    output_dir = sys.argv[2] if len(sys.argv) > 2 else "output"
    df = run_stage1(csv_path, output_dir)
