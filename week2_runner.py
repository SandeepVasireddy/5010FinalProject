"""
MDM Pipeline — Week 2 Main Runner
Orchestrates: Stage 2A (Enhanced Preprocessing) → Stage 2B (Address Validation & Correction)

Usage:
    python3 week2_runner.py [input_csv] [output_dir] [--api-key KEY]

Without an API key, the pipeline runs in simulation mode with deterministic heuristics
that mimic real API behavior for development and testing.
"""

import sys
import os
import asyncio
import pandas as pd
from pathlib import Path
from datetime import datetime

from stage2a_enhanced_preprocessing import run_enhanced_preprocessing
from stage2b_address_validation import run_validation_pipeline


def run_week2(input_csv: str, output_dir: str = "output",
              api_key: str = None) -> pd.DataFrame:
    """Run the complete Week 2 pipeline."""
    Path(output_dir).mkdir(parents=True, exist_ok=True)

    print("\n" + "=" * 70)
    print("  MDM Pipeline — Week 2: Preprocessing & Address Validation")
    print(f"  Input: {input_csv}")
    print(f"  Output: {output_dir}")
    print(f"  API Mode: {'LIVE' if api_key else 'SIMULATION'}")
    print(f"  Started: {datetime.now().isoformat()}")
    print("=" * 70)

    # ── Stage 2A: Enhanced Preprocessing ──
    print("\n" + "─" * 70)
    df = pd.read_csv(input_csv)
    print(f"[Input] Loaded {len(df)} records from Stage 1 output")

    df = run_enhanced_preprocessing(df)

    enhanced_path = Path(output_dir) / "stage2a_enhanced.csv"
    df.to_csv(enhanced_path, index=False)
    print(f"[Stage 2A] Saved enhanced output to {enhanced_path}")

    # ── Stage 2B: Address Validation & Correction ──
    print("\n" + "─" * 70)
    df = asyncio.run(run_validation_pipeline(
        str(enhanced_path), output_dir, api_key
    ))

    # ── Final Summary ──
    print("\n" + "=" * 70)
    print("  Week 2 Pipeline Complete!")
    print("=" * 70)

    total = len(df)
    validated = (df["validation_status"] == "VALIDATED").sum()
    corrected = (df["validation_status"] == "CORRECTED").sum()
    failed = (df["validation_status"] == "FAILED").sum()
    skipped = (df["validation_status"] == "SKIPPED").sum()
    success = validated + corrected

    print(f"\n  Records processed:        {total}")
    print(f"  ✓ Validated (clean pass): {validated} ({100*validated/total:.1f}%)")
    print(f"  ✓ Corrected & validated:  {corrected} ({100*corrected/total:.1f}%)")
    print(f"  ✗ Failed validation:      {failed} ({100*failed/total:.1f}%)")
    print(f"  ○ Skipped (no address):   {skipped} ({100*skipped/total:.1f}%)")
    print(f"\n  Overall success rate:     {success}/{total} ({100*success/total:.1f}%)")
    print(f"  Manual review queue:      {(df['manual_review_flag']==True).sum()} records")

    # Output files summary
    print(f"\n  Output files:")
    for f in sorted(Path(output_dir).glob("stage2*")):
        size = f.stat().st_size
        print(f"    {f.name:40s} ({size:,} bytes)")
    report_path = Path(output_dir) / "validation_report.json"
    if report_path.exists():
        print(f"    {'validation_report.json':40s} ({report_path.stat().st_size:,} bytes)")

    print(f"\n  Finished: {datetime.now().isoformat()}")
    print("=" * 70)

    return df


if __name__ == "__main__":
    input_csv = sys.argv[1] if len(sys.argv) > 1 else "output/stage1_processed.csv"
    output_dir = sys.argv[2] if len(sys.argv) > 2 else "output"
    api_key = None

    # Check for --api-key flag
    for i, arg in enumerate(sys.argv):
        if arg == "--api-key" and i + 1 < len(sys.argv):
            api_key = sys.argv[i + 1]

    # Also check environment variable
    if not api_key:
        api_key = os.environ.get("GOOGLE_API_KEY")

    df = run_week2(input_csv, output_dir, api_key)
