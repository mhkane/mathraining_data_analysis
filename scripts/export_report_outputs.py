"""
Export report outputs (CSVs and PNGs) to report/March_6_2026_Report.
Run from repo root. Requires report_data.pkl to exist (or pass --build to run full pipeline).
"""
import argparse
import os
import pickle
import sys

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
OUT_DIR = os.path.join(REPO_ROOT, "report", "March_6_2026_Report")
OUT_CSV_DIR = os.path.join(OUT_DIR, "csvs")
OUT_FIG_DIR = os.path.join(OUT_DIR, "figures")

CSV_KEYS = [
    "yearly_metrics_df",
    "yearly_metrics_by_gender_df",
    "most_active_top3_df",
    "most_active_top2_by_gender_df",
    "profiles_df",
    "events_df",
]

PNG_KEYS = [
    "fig_cumulative_accounts",
    "fig_cumulative_points",
    "fig_cumulative_exercises",
    "fig_cumulative_problems",
    "fig_per_year_signups",
    "fig_per_year_points",
    "fig_per_year_exercises",
    "fig_per_year_problems",
    "fig_cumulative_gender_signups",
    "fig_cumulative_gender_points",
    "fig_cumulative_gender_exercises",
    "fig_cumulative_gender_problems",
    "fig_pies_2025_gender",
    "fig_top2_active_by_gender",
    "fig_cumulative_points_per_user_violin",
]


def main():
    parser = argparse.ArgumentParser(description="Export report CSVs and PNGs")
    parser.add_argument("--build", action="store_true", help="Build report from pipeline (country_id=48) instead of loading pickle")
    parser.add_argument("--country", type=int, default=48, help="Country ID when using --build")
    args = parser.parse_args()

    sys.path.insert(0, REPO_ROOT)
    import mathraining_scraping_utils as msu
    import pandas as pd

    if args.build:
        report = msu.build_country_yearly_report(country_id=args.country, sleep_seconds=0.4)
        report_loaded = msu.build_report_from_data(report)
    else:
        pkl_path = os.path.join(REPO_ROOT, "report_data.pkl")
        if not os.path.isfile(pkl_path):
            print("report_data.pkl not found. Run with --build to generate from pipeline.")
            sys.exit(1)
        with open(pkl_path, "rb") as f:
            data = pickle.load(f)
        report_loaded = msu.build_report_from_data(data)

    os.makedirs(OUT_CSV_DIR, exist_ok=True)
    os.makedirs(OUT_FIG_DIR, exist_ok=True)

    for key in CSV_KEYS:
        df = report_loaded.get(key)
        if df is not None and hasattr(df, "to_csv"):
            path = os.path.join(OUT_CSV_DIR, f"{key}.csv")
            df.to_csv(path, index=False)
            print(f"Wrote {path}")

    for key in PNG_KEYS:
        fig = report_loaded.get(key)
        if fig is not None and hasattr(fig, "savefig"):
            path = os.path.join(OUT_FIG_DIR, f"{key}.png")
            fig.savefig(path, dpi=150, bbox_inches="tight")
            print(f"Wrote {path}")

    print(f"Done. CSVs in {OUT_CSV_DIR}, figures in {OUT_FIG_DIR}")


if __name__ == "__main__":
    main()
