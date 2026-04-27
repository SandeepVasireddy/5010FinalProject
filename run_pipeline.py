"""
MDM Address Validation & Enrichment Pipeline
Master Runner — Executes all stages end-to-end

Usage:
    python3 run_pipeline.py <input_csv> [output_dir] [--api-key KEY]

Examples:
    python3 run_pipeline.py 100_sample_MDM.csv output
    python3 run_pipeline.py data/34k_records.csv output --api-key AIzaSy...
    GOOGLE_API_KEY=AIzaSy... python3 run_pipeline.py data/records.csv output
"""

import sys
import os
import asyncio
import pandas as pd
from pathlib import Path
from datetime import datetime

# Import pipeline modules
from stage1_preprocessing import run_stage1
from stage2a_enhanced_preprocessing import run_enhanced_preprocessing
from stage2b_address_validation import run_validation_pipeline


def run_full_pipeline(csv_path: str, output_dir: str = "output",
                      api_key: str = None):
    """Run the complete MDM pipeline: Stage 1 → Stage 2A → Stage 2B."""

    Path(output_dir).mkdir(parents=True, exist_ok=True)
    start_time = datetime.now()

    print("\n" + "=" * 70)
    print("  MDM Address Validation & Enrichment Pipeline")
    print(f"  Input:   {csv_path}")
    print(f"  Output:  {output_dir}")
    print(f"  API:     {'LIVE (Google Address Validation)' if api_key else 'SIMULATION'}")
    print(f"  Started: {start_time.isoformat()}")
    print("=" * 70)

    # ════════════════════════════════════════
    # STAGE 1: Ingestion, Parsing, Preprocessing, EDA
    # ════════════════════════════════════════
    print("\n\n" + "▓" * 70)
    print("  STAGE 1: Data Ingestion & Preprocessing")
    print("▓" * 70)
    df = run_stage1(csv_path, output_dir)

    # ════════════════════════════════════════
    # STAGE 2A: Enhanced Preprocessing
    # ════════════════════════════════════════
    print("\n\n" + "▓" * 70)
    print("  STAGE 2A: Enhanced Preprocessing")
    print("▓" * 70)
    df = run_enhanced_preprocessing(df)
    enhanced_path = Path(output_dir) / "stage2a_enhanced.csv"
    df.to_csv(enhanced_path, index=False)
    print(f"[Output] Saved to {enhanced_path}")

    # ════════════════════════════════════════
    # STAGE 2B: Address Validation & Correction
    # ════════════════════════════════════════
    print("\n\n" + "▓" * 70)
    print("  STAGE 2B: Address Validation & Correction")
    print("▓" * 70)
    df = asyncio.run(run_validation_pipeline(
        str(enhanced_path), output_dir, api_key
    ))

    # ════════════════════════════════════════
    # FINAL SUMMARY
    # ════════════════════════════════════════
    elapsed = (datetime.now() - start_time).total_seconds()
    total = len(df)
    validated = (df["validation_status"] == "VALIDATED").sum()
    corrected = (df["validation_status"] == "CORRECTED").sum()
    failed = (df["validation_status"] == "FAILED").sum()
    skipped = (df["validation_status"] == "SKIPPED").sum()
    success = validated + corrected

    print("\n\n" + "=" * 70)
    print("  PIPELINE COMPLETE")
    print("=" * 70)
    print(f"\n  Input:                    {csv_path}")
    print(f"  Records processed:        {total}")
    print(f"  Time elapsed:             {elapsed:.1f}s")
    print(f"\n  --- Stage 1 Results ---")
    print(f"  Encoding repairs:         {(df.get('flag_encoding_repaired', pd.Series([False])) == True).sum()}")
    print(f"  Names cleaned:            {(df['SOURCE_NAME'].str.strip() != df['cleaned_name']).sum()}")
    print(f"  Quality READY:            {(df['quality_tier'] == 'READY').sum()}")
    print(f"  Quality INCOMPLETE:       {(df['quality_tier'] == 'INCOMPLETE').sum()}")
    print(f"  Quality NEEDS_PREP:       {(df['quality_tier'] == 'NEEDS_PREP').sum()}")
    print(f"  Quality CRITICAL:         {(df['quality_tier'] == 'CRITICAL').sum()}")
    print(f"\n  --- Stage 2 Results ---")
    print(f"  Validated (clean pass):   {validated} ({100*validated/total:.1f}%)")
    print(f"  Corrected & validated:    {corrected} ({100*corrected/total:.1f}%)")
    print(f"  Failed:                   {failed} ({100*failed/total:.1f}%)")
    print(f"  Skipped:                  {skipped} ({100*skipped/total:.1f}%)")
    print(f"\n  Overall success rate:     {success}/{total} ({100*success/total:.1f}%)")
    print(f"  Manual review queue:      {(df['manual_review_flag'] == True).sum()} records")

    print(f"\n  --- Output Files ---")
    for f in sorted(Path(output_dir).glob("*")):
        if f.is_file():
            size_kb = f.stat().st_size / 1024
            print(f"    {f.name:45s} {size_kb:>7.1f} KB")

    print("=" * 70)
    return df


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print(__doc__)
        sys.exit(1)

    csv_path = sys.argv[1]
    output_dir = sys.argv[2] if len(sys.argv) > 2 and not sys.argv[2].startswith("--") else "output"

    # Check for --api-key flag
    api_key = None
    for i, arg in enumerate(sys.argv):
        if arg == "--api-key" and i + 1 < len(sys.argv):
            api_key = sys.argv[i + 1]
    if not api_key:
        api_key = os.environ.get("GOOGLE_API_KEY")

    if not Path(csv_path).exists():
        print(f"Error: File not found: {csv_path}")
        sys.exit(1)

    run_full_pipeline(csv_path, output_dir, api_key)
