# MDM Address Validation & Enrichment POC

## Deliverable 2 and 3 Coverage Report

This report evaluates whether the current Honeywell MDM Address Validation and Enrichment pipeline covers the project deliverables, with emphasis on Deliverables 2 and 3 and the TA guidance from Medha.

The report reflects the latest full rerun completed on May 6, 2026:

- Final Week 2 output: `output_week2_final`
- Final Week 3 output: `output_week3_final`
- Week 2 input: `output_week2_final/stage1_processed.csv`
- Week 3 input: `output_week2_final/stage2b_validated_only.csv`

## Executive Summary

The pipeline covers the core requirements for the final POC. It implements the required end-to-end flow:

1. Clean and normalize failed D&B MDM records.
2. Validate and correct addresses using Azure Maps.
3. Verify whether the source company appears to be located at the validated address.
4. Use Azure OpenAI adjudication to improve borderline company-address decisions.
5. Enrich only trusted company-address pairs.
6. Route non-enriched records into operational buckets rather than one broad manual-review bucket.
7. Produce stage-level output files and metrics.

The latest caveat work improved the deliverable posture. Translation and web evidence are now real optional integrations rather than only documentation caveats:

- Azure Translator support is implemented in Stage 2A and can be enabled with keys.
- Bing Web Search support is implemented in Stage 3 and can be enabled with keys.
- Because no Translator or Bing keys were configured for this rerun, both optional services stayed disabled and the reports document that clearly.

## End-to-End Funnel

| Pipeline step | Count | % of original 36,230 |
|---|---:|---:|
| Original MDM records processed | 36,230 | 100.0% |
| Stage 1 processed records | 36,230 | 100.0% |
| Stage 2 exact address success | 16,264 | 44.9% |
| Stage 2 usable address success | 22,558 | 62.3% |
| Stage 2 manual review / unusable address | 13,672 | 37.7% |
| Stage 3 trusted auto-enriched | 1,935 | 5.3% |
| Stage 3 auto no-enrich due to mismatch | 9,677 | 26.7% |
| Stage 3 targeted manual review | 1,648 | 4.5% |
| Stage 3 deferred no evidence | 9,298 | 25.7% |

## Stage 1: Preprocessing / EDA

Stage 1 processed all `36,230` records and produced structured fields, cleaned names, normalized address components, language detection, and quality flags.

| Quality tier | Count | Rate |
|---|---:|---:|
| READY | 25,123 | 69.3% |
| INCOMPLETE | 9,619 | 26.5% |
| NEEDS_PREP | 1,485 | 4.1% |
| CRITICAL | 3 | ~0.0% |

Important Stage 1 quality findings:

| Finding | Count | Rate |
|---|---:|---:|
| Address normalized/modified | 12,787 | 35.3% |
| Name cleaned/modified | 2,392 | 6.6% |
| Encoding repaired | 2,961 | 8.2% |
| Missing street number | 6,624 | 18.3% |
| Missing state | 4,319 | 11.9% |
| Missing postal | 428 | 1.2% |
| Name has codes | 2,509 | 6.9% |
| Non-Latin text | 2,034 | 5.6% |
| Garbled unrecoverable | 3 | ~0.0% |

## Deliverable 2 Coverage

Deliverable 2 asks for preprocessing, category-specific address preparation, language handling, address validation/correction, retry/escalation, and diagnostic output.

| Requirement | Status | Evidence |
|---|---|---|
| Parse and preprocess source address data | Covered | Stage 1 parses `FULL_ADDRESS` into street, city, state, country, and postal fields. |
| Clean source company names | Covered | Stage 1 removes parentheticals, care-of text, department codes, and run-rate noise. |
| Repair encoding issues | Covered | Stage 1 repaired 2,961 records and identified 3 unrecoverable garbled records. |
| Normalize address components | Covered | Stage 1 and Stage 2A normalize street, city, state, country, and postal fields. |
| Normalize abbreviations | Covered | Stage 1/2A expand street and regional abbreviations such as `Rd`, `Ave`, `St`, `C/`, `Str.`, `V.le`, and related patterns. |
| Handle multilingual patterns | Covered | Stage 1 detects language/script; Stage 2A applies language-aware abbreviation expansion and API strategy selection. |
| Translate non-Latin fields | Supported, disabled in rerun | Azure Translator integration is implemented and controlled by `ENABLE_TRANSLATION`; no Translator key was configured, so 2,085 records were explicitly marked `TRANSLATION_NOT_CONFIGURED`. |
| Flag insufficient addresses | Covered | Missing street number, missing state/postal, complex address, non-Latin, and API-readiness flags are produced. |
| Validate addresses with API | Covered | Stage 2B ran live Azure Maps validation/correction on all 36,230 records. |
| Return validation/geocode output | Covered | Stage 2B outputs validation status, corrected/validated address, latitude/longitude, confidence, and diagnostics. |
| Correction and retry/escalation logic | Covered | API correction, partial validation, engine partial correction, component fixes, broader search, simplification, and manual-review routing are implemented. |
| Diagnostic reporting | Covered | `validation_report.json`, `stage2b_validated.csv`, `stage2b_validated_only.csv`, and `stage2b_manual_review.csv` are produced. |

## Stage 2 Rerun Results

Total records processed: `36,230`

| Stage 2 status | Count | Rate |
|---|---:|---:|
| VALIDATED | 5,196 | 14.3% |
| CORRECTED | 11,068 | 30.6% |
| PARTIAL_VALIDATED | 6,294 | 17.4% |
| FAILED | 13,670 | 37.7% |
| API_ERROR | 2 | ~0.0% |

Stage 2 success:

| Metric | Count | Rate |
|---|---:|---:|
| Exact success, VALIDATED + CORRECTED | 16,264 | 44.9% |
| Usable success, including PARTIAL_VALIDATED | 22,558 | 62.3% |
| Manual review / unusable address | 13,672 | 37.7% |

Translation handling from the rerun:

| Translation handling | Count |
|---|---:|
| NOT_NEEDED_LATIN_SCRIPT | 34,145 |
| TRANSLATION_NOT_CONFIGURED | 2,085 |

API strategy distribution:

| API strategy | Count |
|---|---:|
| direct | 34,145 |
| direct_cjk | 1,711 |
| needs_transliteration | 374 |

Interpretation: The pipeline now measures the translation gap directly. Since no Azure Translator key was configured, non-Latin records were not translated, but they were explicitly counted and routed with a translation status rather than hidden.

## Deliverable 3 Coverage

Deliverable 3 asks for company-address verification, lookup of actual address occupants, possible corrected-address candidates, AI-assisted enrichment, full pipeline integration, performance metrics, and a final summary report.

| Requirement | Status | Evidence |
|---|---|---|
| Verify whether source company is located at validated address | Covered | Stage 3 searches Azure Maps POI near the validated geocode and scores name, address, distance, and API confidence. |
| Search what entity occupies the validated address | Covered | Address-occupant search identifies mismatches and captures occupant evidence. |
| Report if another entity is at the address | Covered | `matched_company_name`, `occupant_at_address`, `matched_address`, `matched_category`, and `verification_evidence` are output. |
| Attempt to find possible correct company address | Covered | Fuzzy company lookup produces `CORRECT_ADDRESS_CANDIDATE` when a likely company exists elsewhere. |
| Use web search or deterministic APIs | Covered with optional live support | Azure Maps POI/fuzzy APIs are used by default. Optional Bing Web Search support is implemented but disabled in this rerun because no key was configured. |
| AI-assisted adjudication | Covered | Azure OpenAI adjudicates borderline candidates and produces status, confidence, method, promotion flag, and reason. |
| Overall confidence score | Covered | `overall_confidence_score` combines deterministic and AI confidence. |
| Enrich verified records only | Covered | Only `VERIFIED` and `LIKELY_MATCH` records route to `AUTO_ENRICH`. |
| Enrich legal name, address, hierarchy, website/domain, NAICS/SIC, employee/revenue/year | Covered | Stage 3 outputs all required firmographic fields. |
| Produce manual review and action routing files | Covered | Auto-enrich, auto-no-enrich, targeted-review, and deferred-no-evidence files are produced. |
| Generate stage metrics | Covered | `week3_summary_report.json` includes verification distribution, enrichment coverage, AI adjudication, evidence sources, routing, manual review, and API usage. |

## Stage 3 Rerun Results

Stage 3 input: `22,558` usable Stage 2 records.

| Verification status | Count | % of Stage 3 input | % of original |
|---|---:|---:|---:|
| VERIFIED | 568 | 2.5% | 1.6% |
| LIKELY_MATCH | 1,367 | 6.1% | 3.8% |
| ADDRESS_MISMATCH | 10,057 | 44.6% | 27.8% |
| CORRECT_ADDRESS_CANDIDATE | 1,268 | 5.6% | 3.5% |
| UNVERIFIED | 9,298 | 41.2% | 25.7% |

Trusted enrichment population:

| Metric | Count | Rate |
|---|---:|---:|
| VERIFIED + LIKELY_MATCH | 1,935 | 8.6% of Stage 3 input |
| AI-promoted matches | 79 | 0.4% of Stage 3 input |
| OpenAI-assisted enrichment records | 1,901 | 8.4% of Stage 3 input |

## Stage 3 Routing

| Routing bucket | Count | % of Stage 3 input | % of original |
|---|---:|---:|---:|
| AUTO_ENRICH | 1,935 | 8.6% | 5.3% |
| AUTO_NO_ENRICH_ADDRESS_MISMATCH | 9,677 | 42.9% | 26.7% |
| REVIEW_CORRECT_ADDRESS_CANDIDATE | 1,268 | 5.6% | 3.5% |
| REVIEW_LOW_CONFIDENCE_MISMATCH | 380 | 1.7% | 1.0% |
| DEFER_NO_EVIDENCE | 9,298 | 41.2% | 25.7% |

True targeted manual review:

```text
1,648 records = 7.3% of Stage 3 input
```

This is the strongest operational improvement. Instead of treating all non-enriched records as manual review, the pipeline separates:

- trusted records to enrich,
- high-confidence mismatches to block,
- targeted review records,
- no-evidence records to defer.

## Enrichment Coverage

The field coverage below is counted across all Stage 3 records. Some fields such as address and legal name can be populated by fallback logic, while richer firmographic fields are mainly meaningful for trusted enriched records.

| Field | Populated count |
|---|---:|
| legal_name | 22,534 |
| enriched_full_address | 22,558 |
| website | 1,533 |
| email_domain | 1,562 |
| naics_code | 1,647 |
| sic_code | 1,647 |
| parent_company | 918 |
| domestic_ultimate | 1,347 |
| global_ultimate | 1,346 |
| headquarters_address | 22,558 |
| employee_count | 1,472 |
| revenue_range | 1,377 |
| year_established | 1,477 |

Approximate enrichment coverage against the 1,935 `AUTO_ENRICH` records:

| Field | Count | Approx. rate vs auto-enriched |
|---|---:|---:|
| website | 1,533 | 79.2% |
| email_domain | 1,562 | 80.7% |
| NAICS/SIC | 1,647 | 85.1% |
| parent_company | 918 | 47.4% |
| domestic_ultimate | 1,347 | 69.6% |
| global_ultimate | 1,346 | 69.6% |
| employee_count | 1,472 | 76.1% |
| revenue_range | 1,377 | 71.2% |
| year_established | 1,477 | 76.3% |

## AI and Evidence Usage

| Metric | Count |
|---|---:|
| Azure Maps calls | 51,655 |
| Azure Maps cache hits | 955 |
| OpenAI adjudication calls | 10,972 |
| OpenAI adjudication cache hits | 432 |
| OpenAI enrichment calls | 1,827 |
| OpenAI enrichment cache hits | 5,738 |
| Bing web search calls | 0 |
| Bing web search cache hits | 0 |
| Bing web search errors | 0 |

Evidence-source reporting:

| Evidence source | Status |
|---|---|
| Azure Maps POI/fuzzy search | Enabled and used |
| Azure OpenAI adjudication | Enabled and used |
| External web evidence file | Supported but not configured |
| Bing Web Search runtime | Supported but disabled/no key |

## TA Guidance Alignment

TA guidance recommended option 2: improve recall/coverage with AI-assisted verification or web search while clearly documenting decision-making and confidence scores.

| TA guidance | Status | Evidence |
|---|---|---|
| Add AI-assisted verification | Covered | Azure OpenAI candidate adjudication is implemented. |
| Improve coverage while controlling risk | Covered | AI promoted 79 matches, but promotions require high confidence. |
| Document decision-making | Covered | Deterministic status, AI status, AI reason, final status, and routing reason are retained. |
| Add confidence score | Covered | `verification_confidence`, `ai_adjudication_confidence`, and `overall_confidence_score` are output. |
| Add web search where possible | Supported | Bing Web Search support exists, but was disabled because no key was available. |

## Remaining Production Notes

These are no longer deliverable blockers, but they should be stated clearly:

1. Azure Translator is implemented but was not used in the rerun because no Translator key was configured.
2. Bing Web Search is implemented but was not used in the rerun because no Bing Search key was configured.
3. Full open-web crawling is intentionally not implemented. The safer controlled design is API-based web evidence through Bing or a curated evidence file.
4. The pipeline is conservative. It prioritizes MDM precision over recall, which reduces the risk of attaching incorrect firmographic data to customer master records.
5. The rerun produced non-fatal warnings in stderr: pandas future warnings and Python asyncio cleanup warnings after completion. The output files and summary reports were still generated successfully.

## Final Verdict

| Deliverable | Verdict |
|---|---|
| Deliverable 1 | Covered |
| Deliverable 2 | Covered, including optional Azure Translator support and explicit translation-gap metrics |
| Deliverable 3 | Covered, including Azure Maps verification, Azure OpenAI adjudication/enrichment, optional Bing/web evidence support, confidence scoring, and operational routing |

Overall, the project satisfies the core deliverable requirements. The strongest final-project framing is:

The pipeline recovered usable validated addresses for 22,558 of 36,230 failed D&B records. It then verified company-address fit before enrichment, auto-enriched 1,935 trusted company-address pairs, blocked 9,677 high-confidence mismatches from bad enrichment, routed 1,648 records to targeted review, and deferred 9,298 records where evidence coverage was insufficient. This protects MDM quality by avoiding questionable enrichment while still recovering a meaningful set of trusted records.
