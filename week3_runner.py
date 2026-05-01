"""
MDM Pipeline - Week 3 Main Runner

Usage:
    python week3_runner.py [input_csv] [output_dir] [--limit N]

Default input is the Week 2 usable output:
    output_week2_live_full/stage2b_validated_only.csv
"""

from stage3_company_verification_enrichment import main


if __name__ == "__main__":
    main()
