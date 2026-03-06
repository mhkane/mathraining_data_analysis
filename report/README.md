# Report outputs

Generated CSVs and figures from the Mathraining country yearly analytics pipeline.

## March 6th 2026 Report

**Location:** `March_6_2026_Report/`

- **4 CSVs:** `yearly_metrics_df.csv`, `yearly_metrics_by_gender_df.csv`, `most_active_top3_df.csv`, `most_active_top2_by_gender_df.csv`
- **14 PNGs:** cumulative and per-year bar charts (overall + by gender), 2025 gender pie charts, top-2-by-gender figure

Plot titles use Ivorian Mathraining wording. To regenerate from the saved pickle:

```bash
python scripts/export_report_outputs.py
```

To regenerate from a fresh scrape (Côte d'Ivoire, country_id=48):

```bash
python scripts/export_report_outputs.py --build
```
