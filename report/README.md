# Report outputs

CSVs and figures from the Mathraining country analytics pipeline.

## March 6th 2026 Report

**Path:** `March_6_2026_Report/`

- **`csvs/`** — 6 CSVs: yearly metrics, by-gender metrics, top-3 and top-2-by-gender tables, profiles, events.
- **`figures/`** — 14 PNGs: cumulative and per-year bar charts (overall + by gender), 2025 gender pies, top-2-by-gender.

**Regenerate:** from repo root, run `python scripts/export_report_outputs.py` (uses saved pickle) or `python scripts/export_report_outputs.py --build` (full scrape, Côte d'Ivoire).
