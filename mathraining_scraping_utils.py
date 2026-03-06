"""
Mathraining scraping and yearly analytics utilities.
Country-level user lists, profile details, resolution history, and yearly/cumulative metrics with gender breakdown.
"""

from __future__ import annotations

import logging
import math
import pickle
import re
import time
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd
import requests
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)

# Optional progress bar: use tqdm if available, else plain iterator
try:
    from tqdm import tqdm
    def _progress(iterable, desc=None, total=None, **kwargs):
        return tqdm(iterable, desc=desc, total=total, **kwargs)
except ImportError:
    def _progress(iterable, desc=None, total=None, **kwargs):
        return iter(iterable)

BASE_URL = "https://www.mathraining.be"
USERS_PATH = "/users"
PAGE_SIZE = 50

# Max year to include in plots (exclude current/incomplete year, e.g. 2026)
PLOT_MAX_YEAR = 2025

# French month names -> month number
MONTH_MAP = {
    "janvier": 1, "février": 2, "mars": 3, "avril": 4, "mai": 5, "juin": 6,
    "juillet": 7, "août": 8, "septembre": 9, "octobre": 10, "novembre": 11, "décembre": 12,
}


def _get_session() -> requests.Session:
    s = requests.Session()
    s.headers.update({"User-Agent": "MathrainingScraper/1.0"})
    return s


def convert_french_date(french_date: str) -> str:
    """Convert French date like '24 juin 2023' to MM/DD/YYYY."""
    parts = french_date.split()
    if len(parts) == 3:
        day, month_name, year = parts
    elif len(parts) == 2:
        month_name, year = parts
        day = "1"
    else:
        raise ValueError(f"Unexpected date format: {french_date}")
    month = MONTH_MAP[month_name.lower()]
    date_obj = datetime(year=int(year), month=month, day=int(day))
    return date_obj.strftime("%m/%d/%Y")


def _parse_evolution_signup(evolution_text: str) -> tuple[str | None, str]:
    """
    Parse Évolution section for signup date and gender.
    Returns (signup_date_mm_dd_yyyy, gender_inferred) with gender in {'male','female','unknown'}.
    """
    if not evolution_text:
        return None, "unknown"
    # Prefer explicit 'inscrite' (female) over 'inscrit' (male)
    match_f = re.search(r"s'est inscrite sur Mathraining le\s*(\d{1,2} \w+ \d{4})", evolution_text)
    if match_f:
        try:
            return convert_french_date(match_f.group(1)), "female"
        except Exception:
            return None, "female"
    match_m = re.search(r"s'est inscrit(?!e) sur Mathraining le\s*(\d{1,2} \w+ \d{4})", evolution_text)
    if match_m:
        try:
            return convert_french_date(match_m.group(1)), "male"
        except Exception:
            return None, "male"
    # Fallback: any inscrit(e)
    match = re.search(r"s'est inscrit(e)? sur Mathraining le\s*(\d{1,2} \w+ \d{4})", evolution_text)
    if not match:
        return None, "unknown"
    try:
        date_str = convert_french_date(match.group(2))
        gender = "female" if match.group(1) == "e" else "male"
        return date_str, gender
    except Exception:
        return None, "unknown"


def _normalize_int_cell(value: str | None) -> int:
    """Strip spaces and non-digits, then parse as int. Returns 0 if empty or invalid."""
    if value is None or not str(value).strip():
        return 0
    s = re.sub(r"\s+", "", str(value))
    s = re.sub(r"[^\d]", "", s)
    return int(s) if s else 0


def get_country_user_count_and_pages(
    country_id: int,
    session: requests.Session | None = None,
) -> dict[str, int]:
    """
    Fetch country users page 1, parse user count from select#country option, compute num_pages.
    Returns {'country_count': int, 'num_pages': int}.
    Fallback: infer max page from pagination links if option not found.
    """
    logger.info("Fetching country users page 1 for country_id=%s ...", country_id)
    session = session or _get_session()
    url = f"{BASE_URL}{USERS_PATH}?country={country_id}&page=1&title=0"
    resp = session.get(url, timeout=30)
    resp.raise_for_status()
    soup = BeautifulSoup(resp.text, "html.parser")

    opt = soup.select_one(f'select#country option[value="{country_id}"]')
    if opt:
        txt = opt.get_text(" ", strip=True)
        m = re.search(r"\((\d+)\)", txt)
        if m:
            count = int(m.group(1))
            num_pages = max(1, math.ceil(count / PAGE_SIZE))
            logger.info("Country count=%s, num_pages=%s (from select option)", count, num_pages)
            return {"country_count": count, "num_pages": num_pages}

    # Fallback: max page from pagination
    pages = []
    for a in soup.select('ul.pagination a[href*="country="]'):
        if str(country_id) in (a.get("href") or ""):
            mm = re.search(r"[?&]page=(\d+)", a.get("href", ""))
            if mm:
                pages.append(int(mm.group(1)))
    num_pages = max(pages, default=1)
    logger.info("Inferred num_pages=%s from pagination (count ~%s)", num_pages, num_pages * PAGE_SIZE)
    return {"country_count": num_pages * PAGE_SIZE, "num_pages": num_pages}


def generate_country_user_urls(country_id: int, num_pages: int) -> list[str]:
    """Generate list of country user list URLs for pages 1..num_pages."""
    urls = []
    for page in range(1, num_pages + 1):
        urls.append(f"{BASE_URL}{USERS_PATH}?country={country_id}&page={page}&title=0")
    return urls


def extract_profiles_from_urls(
    urls: list[str],
    session: requests.Session | None = None,
) -> list[dict[str, str]]:
    """Scrape each URL and collect {name, link} for each user row in table#users_table."""
    session = session or _get_session()
    logger.info("Extracting profiles from %s page(s) ...", len(urls))
    profiles = []
    for url in _progress(urls, desc="Pages", total=len(urls)):
        resp = session.get(url, timeout=30)
        if resp.status_code != 200:
            logger.warning("Failed to fetch %s (status %s)", url, resp.status_code)
            continue
        soup = BeautifulSoup(resp.text, "html.parser")
        table = soup.find("table", {"id": "users_table"})
        if not table:
            continue
        for row in table.find_all("tr")[1:]:
            cells = row.find_all("td")
            if len(cells) > 1:
                name_cell = cells[1]
                name = name_cell.get_text(strip=True)
                a = name_cell.find("a", href=True)
                if a:
                    href = a["href"]
                    link = href if href.startswith("http") else f"{BASE_URL}{href}"
                    profiles.append({"name": name, "link": link})
    logger.info("Extracted %s profile(s)", len(profiles))
    return profiles


def scrape_profile_details(
    profile: dict[str, str],
    session: requests.Session | None = None,
) -> dict[str, Any] | None:
    """
    Scrape one profile page for Name, Link, Score, Exercises Completed, Problems Solved,
    Sign Up Date, and gender_inferred (male/female/unknown from inscrit/inscrite).
    Numeric fields are normalized to integers.
    """
    session = session or _get_session()
    url = profile["link"]
    resp = session.get(url, timeout=30)
    if resp.status_code != 200:
        return None
    soup = BeautifulSoup(resp.text, "html.parser")

    name_el = soup.find_all("span", class_="fw-bold")
    user_name = name_el[0].text.strip() if name_el else profile.get("name", "")

    score_el = soup.find(string="Score")
    score_raw = score_el.find_next("td", class_="myvalue").text.strip() if score_el else "0"
    ex_el = soup.find(string="Exercices")
    ex_raw = ex_el.find_next("div", class_="progress_nb").text.strip() if ex_el else "0"
    prob_el = soup.find(string="Problèmes")
    prob_raw = prob_el.find_next("div", class_="progress_nb").text.strip() if prob_el else "0"

    sign_up_date = None
    gender_inferred = "unknown"
    evolution = soup.find("div", class_="g-col-12 basic_container p-1")
    if evolution:
        full_text = evolution.get_text(strip=True)
        sign_up_date, gender_inferred = _parse_evolution_signup(full_text)

    return {
        "Name": user_name,
        "Link": url,
        "Score": _normalize_int_cell(score_raw),
        "Exercises Completed": _normalize_int_cell(ex_raw),
        "Problems Solved": _normalize_int_cell(prob_raw),
        "Sign Up Date": sign_up_date,
        "gender_inferred": gender_inferred,
    }


def _parse_resolution_date(s: str) -> tuple[datetime | None, int | None]:
    """Parse '9/09/25 17h06' -> (datetime, year). Year as 4-digit."""
    if not s or not s.strip():
        return None, None
    m = re.match(r"(\d{1,2})/(\d{1,2})/(\d{2})\s*\d{0,2}h?\d{0,2}", s.strip())
    if not m:
        return None, None
    try:
        day, month, yy = int(m.group(1)), int(m.group(2)), int(m.group(3))
        year = 2000 + yy if yy < 100 else yy
        dt = datetime(year, month, day)
        return dt, year
    except Exception:
        return None, None


def scrape_profile_resolutions(
    profile_url: str,
    profile_name: str,
    profile_link: str,
    session: requests.Session | None = None,
) -> list[dict[str, Any]]:
    """
    Scrape Résolutions table from profile page. Return list of events with
    Name, Link, event_dt, year, points, event_type (exercise|problem).
    """
    session = session or _get_session()
    resp = session.get(profile_url, timeout=30)
    if resp.status_code != 200:
        return []
    soup = BeautifulSoup(resp.text, "html.parser")
    rows = soup.select("div.resolution_container_table table.table.middle_aligned.my-0 tr")
    events = []
    seen = set()
    for row in rows:
        tds = row.find_all("td")
        if len(tds) < 4:
            continue
        points_raw = tds[0].get_text(strip=True)  # e.g. "+ 75" or ""
        date_raw = tds[1].get_text(strip=True)
        label = tds[3].get_text(" ", strip=True)

        points = 0
        if points_raw and "+" in points_raw:
            m = re.search(r"\+?\s*(\d+)", points_raw)
            if m:
                points = int(m.group(1))

        event_dt, year = _parse_resolution_date(date_raw)
        if event_dt is None and not date_raw:
            continue
        if event_dt is None:
            continue

        if "Problème #" in label or "Problème#" in label:
            event_type = "problem"
        elif "Exercice" in label:
            event_type = "exercise"
        else:
            event_type = "exercise"

        key = (profile_link, date_raw, label[:80], points)
        if key in seen:
            continue
        seen.add(key)

        events.append({
            "Name": profile_name,
            "Link": profile_link,
            "event_dt": event_dt,
            "year": year,
            "points": points,
            "event_type": event_type,
        })
    return events


def create_profiles_dataframe(
    profiles: list[dict[str, str]],
    session: requests.Session | None = None,
    sleep_seconds: float = 0.5,
) -> pd.DataFrame:
    """Build profiles DataFrame with normalized numerics and gender_inferred."""
    session = session or _get_session()
    logger.info("Scraping profile details for %s profile(s) ...", len(profiles))
    data = []
    for i, profile in _progress(enumerate(profiles), desc="Profiles", total=len(profiles)):
        row = scrape_profile_details(profile, session=session)
        if row:
            data.append(row)
        if sleep_seconds > 0 and i < len(profiles) - 1:
            time.sleep(sleep_seconds)
    logger.info("Scraped %s profile(s) successfully", len(data))
    return pd.DataFrame(data) if data else pd.DataFrame()


def build_events_df(
    profiles_df: pd.DataFrame,
    session: requests.Session | None = None,
    sleep_seconds: float = 0.3,
) -> pd.DataFrame:
    """Scrape resolution history for each profile and return long-format events DataFrame."""
    session = session or _get_session()
    n = len(profiles_df)
    logger.info("Scraping resolution history for %s profile(s) ...", n)
    rows = []
    for i, r in _progress(profiles_df.iterrows(), desc="Resolutions", total=n):
        evts = scrape_profile_resolutions(
            r["Link"], r["Name"], r["Link"], session=session
        )
        rows.extend(evts)
        if sleep_seconds > 0:
            time.sleep(sleep_seconds)
    logger.info("Collected %s resolution event(s)", len(rows))
    return pd.DataFrame(rows) if rows else pd.DataFrame()


def build_yearly_metrics(
    profiles_df: pd.DataFrame,
    events_df: pd.DataFrame,
) -> pd.DataFrame:
    """
    Build yearly metrics: signups_per_year, cumulative_accounts_eoy,
    points_gained_per_year, cumulative_points_eoy,
    exercises_completed_per_year, cumulative_exercises_eoy,
    problems_solved_per_year, cumulative_problems_eoy.
    """
    if profiles_df.empty:
        return pd.DataFrame()
    logger.info("Building yearly metrics ...")
    profiles_df = profiles_df.copy()

    # Signup year from Sign Up Date
    def signup_year(s: str):
        if pd.isna(s) or not s:
            return None
        try:
            # MM/DD/YYYY
            parts = str(s).split("/")
            if len(parts) == 3:
                return int(parts[2])
        except Exception:
            pass
        return None

    profiles_df = profiles_df.copy()
    profiles_df["signup_year"] = profiles_df["Sign Up Date"].map(signup_year)
    signups = profiles_df["signup_year"].dropna().astype(int)
    if len(signups) == 0:
        year_range = list(range(datetime.now().year, datetime.now().year + 1))
    else:
        year_range = range(int(signups.min()), datetime.now().year + 1)

    signups_per_year = signups.value_counts().sort_index()
    cumulative_accounts = signups_per_year.reindex(year_range, fill_value=0).cumsum()

    if events_df.empty:
        points_per_year = pd.Series(dtype=int)
        ex_per_year = pd.Series(dtype=int)
        prob_per_year = pd.Series(dtype=int)
    else:
        points_per_year = events_df.groupby("year")["points"].sum()
        ex_per_year = events_df[events_df["event_type"] == "exercise"].groupby("year").size()
        prob_per_year = events_df[events_df["event_type"] == "problem"].groupby("year").size()

    all_years = sorted(set(signups_per_year.index.tolist()) | set(points_per_year.index.tolist()) | set(year_range))
    if not all_years:
        return pd.DataFrame()
    all_years = list(range(min(all_years), max(all_years) + 1))

    out = pd.DataFrame({"year": all_years})
    out = out.set_index("year")

    out["signups_per_year"] = out.index.map(lambda y: signups_per_year.get(y, 0))
    out["cumulative_accounts_eoy"] = out["signups_per_year"].cumsum()

    out["points_gained_per_year"] = out.index.map(lambda y: int(points_per_year.get(y, 0)))
    out["cumulative_points_eoy"] = out["points_gained_per_year"].cumsum()

    out["exercises_completed_per_year"] = out.index.map(lambda y: int(ex_per_year.get(y, 0)))
    out["cumulative_exercises_eoy"] = out["exercises_completed_per_year"].cumsum()

    out["problems_solved_per_year"] = out.index.map(lambda y: int(prob_per_year.get(y, 0)))
    out["cumulative_problems_eoy"] = out["problems_solved_per_year"].cumsum()

    logger.info("Yearly metrics: %s year(s)", len(out))
    return out.reset_index()


def build_yearly_metrics_by_gender(
    profiles_df: pd.DataFrame,
    events_df: pd.DataFrame,
) -> pd.DataFrame:
    """
    Long-form table: year, gender, metric, value.
    Metrics: signups_per_year, cumulative_accounts_eoy, points_gained_per_year,
    cumulative_points_eoy, exercises_completed_per_year, cumulative_exercises_eoy,
    problems_solved_per_year, cumulative_problems_eoy.
    """
    if profiles_df.empty:
        return pd.DataFrame()
    logger.info("Building yearly metrics by gender ...")

    def signup_year(s):
        if pd.isna(s) or not s:
            return None
        try:
            parts = str(s).split("/")
            if len(parts) == 3:
                return int(parts[2])
        except Exception:
            pass
        return None

    prof = profiles_df.copy()
    prof["signup_year"] = prof["Sign Up Date"].map(signup_year)
    prof = prof.dropna(subset=["signup_year"])
    prof["signup_year"] = prof["signup_year"].astype(int)

    genders = prof["gender_inferred"].unique().tolist()
    years_set = set(prof["signup_year"].tolist())
    if not events_df.empty:
        years_set |= set(events_df["year"].dropna().astype(int).tolist())
    years = list(range(int(min(years_set)), int(max(years_set)) + 1)) if years_set else []

    rows = []
    for year in years:
        for gender in genders:
            prof_g = prof[prof["gender_inferred"] == gender]
            signups_y = (prof_g["signup_year"] == year).sum()
            rows.append({"year": year, "gender": gender, "metric": "signups_per_year", "value": signups_y})
            cum_acc = (prof_g["signup_year"] <= year).sum()
            rows.append({"year": year, "gender": gender, "metric": "cumulative_accounts_eoy", "value": cum_acc})

    df_per = pd.DataFrame(rows)

    if not events_df.empty and "Link" in events_df.columns:
        ev = events_df.merge(prof[["Link", "gender_inferred"]], on="Link", how="left")
        ev["gender_inferred"] = ev["gender_inferred"].fillna("unknown")
        for year in years:
            ev_y = ev[ev["year"] == year]
            for gender in genders:
                ev_g = ev_y[ev_y["gender_inferred"] == gender]
                pts = ev_g["points"].sum()
                df_per = pd.concat([df_per, pd.DataFrame([{"year": year, "gender": gender, "metric": "points_gained_per_year", "value": int(pts)}])], ignore_index=True)
                ex = (ev_g["event_type"] == "exercise").sum()
                df_per = pd.concat([df_per, pd.DataFrame([{"year": year, "gender": gender, "metric": "exercises_completed_per_year", "value": int(ex)}])], ignore_index=True)
                prob = (ev_g["event_type"] == "problem").sum()
                df_per = pd.concat([df_per, pd.DataFrame([{"year": year, "gender": gender, "metric": "problems_solved_per_year", "value": int(prob)}])], ignore_index=True)

    # Add cumulative points/exercises/problems by gender (cumsum of per-year)
    cum_rows = []
    for gender in df_per["gender"].unique():
        for m, cum_name in [
            ("points_gained_per_year", "cumulative_points_eoy"),
            ("exercises_completed_per_year", "cumulative_exercises_eoy"),
            ("problems_solved_per_year", "cumulative_problems_eoy"),
        ]:
            sub = df_per[(df_per["gender"] == gender) & (df_per["metric"] == m)].sort_values("year")
            if sub.empty:
                continue
            cum = sub["value"].cumsum()
            for (y, v) in zip(sub["year"], cum):
                cum_rows.append({"year": y, "gender": gender, "metric": cum_name, "value": int(v)})

    out = pd.concat([df_per, pd.DataFrame(cum_rows)], ignore_index=True)
    logger.info("Yearly by gender: %s row(s)", len(out))
    return out


def most_active_by_year(events_df: pd.DataFrame) -> pd.DataFrame:
    """Per year, student with most points gained. Columns: year, Name, points_gained."""
    if events_df.empty or "points" not in events_df.columns:
        return pd.DataFrame(columns=["year", "Name", "points_gained"])
    by_year_name = events_df.groupby(["year", "Name"])["points"].sum().reset_index()
    idx = by_year_name.groupby("year")["points"].idxmax()
    return by_year_name.loc[idx, ["year", "Name", "points"]].rename(columns={"points": "points_gained"})


def most_active_by_year_gender(
    events_df: pd.DataFrame,
    profiles_df: pd.DataFrame,
) -> pd.DataFrame:
    """Per year and gender, student with most points. Columns: year, gender, Name, points_gained."""
    if events_df.empty or profiles_df.empty:
        return pd.DataFrame(columns=["year", "gender", "Name", "points_gained"])
    ev = events_df.merge(profiles_df[["Link", "gender_inferred"]], on="Link", how="left")
    ev["gender_inferred"] = ev["gender_inferred"].fillna("unknown")
    by_yn = ev.groupby(["year", "gender_inferred", "Name"])["points"].sum().reset_index()
    idx = by_yn.groupby(["year", "gender_inferred"])["points"].idxmax()
    out = by_yn.loc[idx, ["year", "gender_inferred", "Name", "points"]].copy()
    out = out.rename(columns={"gender_inferred": "gender", "points": "points_gained"})
    return out


def get_report_data_description() -> str:
    """Return a short description of report data keys and main DataFrame columns (for notebook docs)."""
    return """
**Report dict keys (data):**
- `profiles_df` – One row per user: Name, Link, Score, Exercises Completed, Problems Solved, Sign Up Date, gender_inferred
- `events_df` – Resolution events: Name, Link, event_dt, year, points, event_type (exercise/problem)
- `yearly_metrics_df` – One row per year: year, signups_per_year, cumulative_accounts_eoy, points_gained_per_year, cumulative_points_eoy, exercises_completed_per_year, cumulative_exercises_eoy, problems_solved_per_year, cumulative_problems_eoy
- `yearly_metrics_by_gender_df` – Long form: year, gender, metric, value
- `most_active_by_year_df` – year, Name, points_gained
- `most_active_by_year_gender_df` – year, gender, Name, points_gained
- `most_active_top3_df` – year, Name, points_gained, rank (1–3)
- `most_active_top2_by_gender_df` – year, gender, Name, points_gained, rank (1–2)
- `first_year`, `country_count`, `num_pages` – scalars

**Custom plots:** Use `yearly_metrics_df` or `yearly_metrics_by_gender_df` with `msu.plot_cumulative_single_bar()`, `msu.plot_per_year_single()`, `msu.plot_cumulative_by_gender_bar()`.
"""


# Keys to save in pickle (data only, no figures)
REPORT_DATA_KEYS = [
    "profiles_df",
    "events_df",
    "yearly_metrics_df",
    "yearly_metrics_by_gender_df",
    "most_active_by_year_df",
    "most_active_by_year_gender_df",
    "first_year",
    "country_count",
    "num_pages",
]


def save_report_data(report: dict, path: str | Path) -> None:
    """Save data part of report to a pickle file (no figures)."""
    path = Path(path)
    data = {k: report[k] for k in REPORT_DATA_KEYS if k in report}
    with open(path, "wb") as f:
        pickle.dump(data, f, protocol=pickle.HIGHEST_PROTOCOL)
    logger.info("Saved report data to %s", path)


def load_report_data(path: str | Path) -> dict:
    """Load report data from a pickle file."""
    path = Path(path)
    with open(path, "rb") as f:
        data = pickle.load(f)
    logger.info("Loaded report data from %s", path)
    return data


def most_active_top_n(events_df: pd.DataFrame, n: int = 3) -> pd.DataFrame:
    """Per year, top n students by points gained. Columns: year, Name, points_gained, rank."""
    if events_df.empty or "points" not in events_df.columns:
        return pd.DataFrame(columns=["year", "Name", "points_gained", "rank"])
    by_year_name = events_df.groupby(["year", "Name"])["points"].sum().reset_index()
    by_year_name = by_year_name.rename(columns={"points": "points_gained"})
    top = (
        by_year_name.sort_values(["year", "points_gained"], ascending=[True, False])
        .groupby("year", group_keys=False)
        .apply(lambda g: g.head(n).assign(rank=range(1, min(n, len(g)) + 1)))
    )
    return top.reset_index(drop=True)


def most_active_top_n_by_gender(
    events_df: pd.DataFrame,
    profiles_df: pd.DataFrame,
    n: int = 2,
) -> pd.DataFrame:
    """Per year and gender, top n students by points. Columns: year, gender, Name, points_gained, rank."""
    if events_df.empty or profiles_df.empty:
        return pd.DataFrame(columns=["year", "gender", "Name", "points_gained", "rank"])
    ev = events_df.merge(profiles_df[["Link", "gender_inferred"]], on="Link", how="left")
    ev["gender_inferred"] = ev["gender_inferred"].fillna("unknown")
    by_yn = ev.groupby(["year", "gender_inferred", "Name"])["points"].sum().reset_index()
    by_yn = by_yn.rename(columns={"gender_inferred": "gender", "points": "points_gained"})
    top = (
        by_yn.sort_values(["year", "gender", "points_gained"], ascending=[True, True, False])
        .groupby(["year", "gender"], group_keys=False)
        .apply(lambda g: g.head(n).assign(rank=range(1, min(n, len(g)) + 1)))
    )
    return top.reset_index(drop=True)


def most_active_top3_names_table(top3_df: pd.DataFrame) -> pd.DataFrame:
    """From long-form top-3 table (year, Name, points_gained, rank), return a table with just names: columns year, 1st, 2nd, 3rd."""
    if top3_df.empty or "rank" not in top3_df.columns:
        return pd.DataFrame(columns=["year", "1st", "2nd", "3rd"])
    pivot = top3_df.pivot(index="year", columns="rank", values="Name")
    pivot = pivot.rename(columns={1: "1st", 2: "2nd", 3: "3rd"})
    for c in ["1st", "2nd", "3rd"]:
        if c not in pivot.columns:
            pivot[c] = None
    return pivot.reset_index()[["year", "1st", "2nd", "3rd"]]


def most_active_top2_by_gender_names_table(top2_df: pd.DataFrame) -> pd.DataFrame:
    """From long-form top-2-by-gender table (year, gender, Name, points_gained, rank), return a table with just names: columns year, gender, 1st, 2nd."""
    if top2_df.empty or "rank" not in top2_df.columns:
        return pd.DataFrame(columns=["year", "gender", "1st", "2nd"])
    pivot = top2_df.pivot_table(index=["year", "gender"], columns="rank", values="Name", aggfunc="first")
    pivot = pivot.rename(columns={1: "1st", 2: "2nd"})
    for c in ["1st", "2nd"]:
        if c not in pivot.columns:
            pivot[c] = None
    return pivot.reset_index()[["year", "gender", "1st", "2nd"]]


def plot_cumulative(
    yearly_df: pd.DataFrame,
    first_year: int | None = None,
    max_year: int | None = None,
    ax=None,
):
    """Plot cumulative: accounts, points, exercises, problems (4 lines). Excludes year > max_year. Shows every year on x-axis."""
    import matplotlib.pyplot as plt
    if yearly_df.empty:
        return
    df = yearly_df.copy()
    if first_year is not None:
        df = df[df["year"] >= first_year]
    max_y = max_year if max_year is not None else PLOT_MAX_YEAR
    df = df[df["year"] <= max_y]
    if df.empty:
        return
    ax = ax or plt.gca()
    ax.plot(df["year"], df["cumulative_accounts_eoy"], label="Accounts", marker="o", markersize=4)
    ax.plot(df["year"], df["cumulative_points_eoy"], label="Points", marker="s", markersize=4)
    ax.plot(df["year"], df["cumulative_exercises_eoy"], label="Exercises", marker="^", markersize=4)
    ax.plot(df["year"], df["cumulative_problems_eoy"], label="Problems", marker="d", markersize=4)
    ax.set_xlabel("Year")
    ax.set_ylabel("Cumulative (end of year)")
    ax.set_title("Cumulative metrics by year")
    ax.set_xticks(df["year"].tolist())
    ax.set_xticklabels(df["year"].tolist())
    ax.legend()
    ax.grid(True, alpha=0.3)


# Cumulative single-metric bar chart: (title, ylabel) for layout like reference images
CUMULATIVE_BAR_CONFIG = {
    "cumulative_accounts_eoy": (
        "Total Mathraining Accounts At End of Each Year",
        "Mathraining Accounts",
    ),
    "cumulative_points_eoy": (
        "Cumulative number of student points vs. Year",
        "Cumulative number of student points",
    ),
    "cumulative_exercises_eoy": (
        "Cumulative number of exercises completed vs. Year",
        "Cumulative number of exercises completed",
    ),
    "cumulative_problems_eoy": (
        "Cumulative number of problems solved vs. Year",
        "Cumulative number of problems solved",
    ),
}


def plot_cumulative_single_bar(
    yearly_df: pd.DataFrame,
    metric_col: str,
    title: str | None = None,
    ylabel: str | None = None,
    first_year: int | None = None,
    max_year: int | None = None,
    ax=None,
):
    """
    Bar chart for one cumulative metric with value labels on top of each bar.
    Uses teal (OVERALL_PLOT_COLOR) to distinguish from gender plots. Year on X-axis, value labels above bars.
    Excludes years > max_year (default PLOT_MAX_YEAR, e.g. 2026). Shows every year on x-axis.
    """
    import matplotlib.pyplot as plt
    if yearly_df.empty or metric_col not in yearly_df.columns:
        return
    df = yearly_df.copy()
    if first_year is not None:
        df = df[df["year"] >= first_year]
    max_y = max_year if max_year is not None else PLOT_MAX_YEAR
    df = df[df["year"] <= max_y]
    if df.empty:
        return
    ax = ax or plt.gca()
    title = title or CUMULATIVE_BAR_CONFIG.get(metric_col, (metric_col, ""))[0]
    ylabel = ylabel or CUMULATIVE_BAR_CONFIG.get(metric_col, ("", metric_col))[1] or metric_col
    bars = ax.bar(df["year"], df[metric_col], color=OVERALL_PLOT_COLOR, edgecolor=OVERALL_PLOT_EDGE, alpha=0.85)
    ax.set_xlabel("Year")
    ax.set_ylabel(ylabel)
    ax.set_title(title)
    ax.set_xticks(df["year"].tolist())
    ax.set_xticklabels(df["year"].tolist())
    ax.grid(True, alpha=0.3, axis="y")
    for bar, val in zip(bars, df[metric_col]):
        ax.text(
            bar.get_x() + bar.get_width() / 2,
            bar.get_height(),
            str(int(val)),
            ha="center",
            va="bottom",
            fontsize=9,
            color=OVERALL_PLOT_COLOR,
            fontweight="bold",
        )


def plot_per_year(
    yearly_df: pd.DataFrame,
    first_year: int | None = None,
    max_year: int | None = None,
    ax=None,
):
    """Plot per-year: signups, points gained, exercises completed, problems solved. Excludes year > max_year. Shows every year on x-axis."""
    import matplotlib.pyplot as plt
    if yearly_df.empty:
        return
    df = yearly_df.copy()
    if first_year is not None:
        df = df[df["year"] >= first_year]
    max_y = max_year if max_year is not None else PLOT_MAX_YEAR
    df = df[df["year"] <= max_y]
    if df.empty:
        return
    ax = ax or plt.gca()
    x = df["year"]
    w = 0.2
    ax.bar(x - 1.5 * w, df["signups_per_year"], width=w, label="Signups")
    ax.bar(x - 0.5 * w, df["points_gained_per_year"], width=w, label="Points")
    ax.bar(x + 0.5 * w, df["exercises_completed_per_year"], width=w, label="Exercises")
    ax.bar(x + 1.5 * w, df["problems_solved_per_year"], width=w, label="Problems")
    ax.set_xlabel("Year")
    ax.set_ylabel("Count (in year)")
    ax.set_title("Per-year metrics")
    ax.set_xticks(df["year"].tolist())
    ax.set_xticklabels(df["year"].tolist())
    ax.legend()
    ax.grid(True, alpha=0.3, axis="y")


# Per-year metric column -> display label
PER_YEAR_METRICS = {
    "signups_per_year": "Signups per year",
    "points_gained_per_year": "Points gained per year",
    "exercises_completed_per_year": "Exercises completed per year",
    "problems_solved_per_year": "Problems solved per year",
}


def plot_per_year_single(
    yearly_df: pd.DataFrame,
    metric: str,
    first_year: int | None = None,
    max_year: int | None = None,
    ax=None,
):
    """Plot a single per-year metric (bar chart) with value labels on top of bars. Excludes year > max_year. Shows every year on x-axis."""
    import matplotlib.pyplot as plt
    if yearly_df.empty or metric not in yearly_df.columns:
        return
    df = yearly_df.copy()
    if first_year is not None:
        df = df[df["year"] >= first_year]
    max_y = max_year if max_year is not None else PLOT_MAX_YEAR
    df = df[df["year"] <= max_y]
    if df.empty:
        return
    ax = ax or plt.gca()
    title = PER_YEAR_METRICS.get(metric, metric)
    bars = ax.bar(df["year"], df[metric], color=OVERALL_PLOT_COLOR, edgecolor=OVERALL_PLOT_EDGE, alpha=0.85)
    ax.set_xlabel("Year")
    ax.set_ylabel(metric)
    ax.set_title(title)
    ax.set_xticks(df["year"].tolist())
    ax.set_xticklabels(df["year"].tolist())
    ax.grid(True, alpha=0.3, axis="y")
    for bar, val in zip(bars, df[metric]):
        ax.text(
            bar.get_x() + bar.get_width() / 2,
            bar.get_height(),
            str(int(val)),
            ha="center",
            va="bottom",
            fontsize=9,
            color=OVERALL_PLOT_COLOR,
            fontweight="bold",
        )


# Cumulative by gender metric -> display label
CUM_GENDER_METRICS = {
    "cumulative_accounts_eoy": "Cumulative accounts (end of year)",
    "cumulative_points_eoy": "Cumulative points (end of year)",
    "cumulative_exercises_eoy": "Cumulative exercises (end of year)",
    "cumulative_problems_eoy": "Cumulative problems (end of year)",
}

# Colors for non-gendered (overall) bar charts: teal to distinguish from gender (male=steelblue, female=pink)
OVERALL_PLOT_COLOR = "#0D9488"   # teal-600
OVERALL_PLOT_EDGE = "#115E59"    # teal-800

# Colors for gender in plots: male=steelblue, female=pink
GENDER_COLORS = {"male": "steelblue", "female": "pink", "unknown": "gray"}


def plot_cumulative_by_gender_bar(
    yearly_by_gender_df: pd.DataFrame,
    metric: str,
    first_year: int | None = None,
    max_year: int | None = None,
    ax=None,
):
    """Plot one cumulative metric by gender as grouped bar chart with value labels on bars. Excludes year > max_year. Shows every year on x-axis."""
    import matplotlib.pyplot as plt
    import numpy as np
    if yearly_by_gender_df.empty:
        return
    df = yearly_by_gender_df[yearly_by_gender_df["metric"] == metric].copy()
    if first_year is not None:
        df = df[df["year"] >= first_year]
    max_y = max_year if max_year is not None else PLOT_MAX_YEAR
    df = df[df["year"] <= max_y]
    if df.empty:
        return
    ax = ax or plt.gca()
    genders = sorted(df["gender"].unique().tolist())
    years = sorted(df["year"].unique())
    x = np.arange(len(years))
    w = 0.8 / max(len(genders), 1)
    for i, g in enumerate(genders):
        sub = df[df["gender"] == g].set_index("year").reindex(years, fill_value=0)
        vals = sub["value"].tolist()
        off = (i - len(genders) / 2 + 0.5) * w
        color = GENDER_COLORS.get(g, "gray")
        bars = ax.bar(x + off, vals, width=w, label=g, color=color)
        for bar, val in zip(bars, vals):
            if val > 0:
                ax.text(
                    bar.get_x() + bar.get_width() / 2,
                    bar.get_height(),
                    str(int(val)),
                    ha="center",
                    va="bottom",
                    fontsize=8,
                    color="black",
                )
    ax.set_xticks(x)
    ax.set_xticklabels(years)
    ax.set_xlabel("Year")
    ax.set_ylabel("Count")
    ax.set_title(CUM_GENDER_METRICS.get(metric, metric) + " by gender")
    ax.legend()
    ax.grid(True, alpha=0.3, axis="y")


def plot_gender_pies_2025(
    yearly_by_gender_df: pd.DataFrame,
    year: int = 2025,
    metrics: list[str] | None = None,
    figsize: tuple[int, int] = (10, 10),
):
    """
    Plot 4 pie charts for the given year: % male vs female for accounts, points, exercises, problems.
    Uses GENDER_COLORS: male=blue, female=pink. Returns a single figure with 2x2 subplots.
    """
    import matplotlib.pyplot as plt
    if yearly_by_gender_df.empty:
        fig, axes = plt.subplots(2, 2, figsize=figsize)
        for ax in axes.flat:
            ax.text(0.5, 0.5, "No data", ha="center", va="center")
        fig.suptitle(f"Gender breakdown ({year})", fontsize=12, y=1.02)
        return fig
    if metrics is None:
        metrics = [
            "cumulative_accounts_eoy",
            "cumulative_points_eoy",
            "cumulative_exercises_eoy",
            "cumulative_problems_eoy",
        ]
    df = yearly_by_gender_df[
        (yearly_by_gender_df["year"] == year) & (yearly_by_gender_df["gender"].isin(["male", "female"]))
    ]
    if df.empty:
        fig, axes = plt.subplots(2, 2, figsize=figsize)
        for ax in axes.flat:
            ax.text(0.5, 0.5, "No data", ha="center", va="center")
        fig.suptitle(f"Gender breakdown ({year})", fontsize=12, y=1.02)
        return fig
    fig, axes = plt.subplots(2, 2, figsize=figsize)
    axes = axes.flatten()
    for idx, metric in enumerate(metrics):
        ax = axes[idx]
        sub = df[df["metric"] == metric]
        if sub.empty:
            ax.text(0.5, 0.5, "No data", ha="center", va="center")
            ax.set_title(CUM_GENDER_METRICS.get(metric, metric))
            continue
        # Order: male first, then female (for consistent blue then pink)
        sub = sub.set_index("gender").reindex(["male", "female"]).fillna(0).reset_index()
        sub = sub[sub["value"] > 0]
        if sub.empty:
            ax.text(0.5, 0.5, "No data", ha="center", va="center")
            ax.set_title(CUM_GENDER_METRICS.get(metric, metric))
            continue
        labels = sub["gender"].str.capitalize()
        sizes = sub["value"].astype(int)
        colors = [GENDER_COLORS.get(g, "gray") for g in sub["gender"]]
        total = sizes.sum()
        if total == 0:
            ax.text(0.5, 0.5, "No data", ha="center", va="center")
        else:
            wedges, texts, autotexts = ax.pie(
                sizes, labels=labels, colors=colors, autopct="%1.1f%%", startangle=90
            )
            for t in autotexts:
                t.set_fontsize(14)
                t.set_fontweight("bold")
        ax.set_title(CUM_GENDER_METRICS.get(metric, metric))
    fig.suptitle(f"Gender breakdown ({year}) – male vs female %", fontsize=12, y=1.02)
    fig.tight_layout()
    return fig


def plot_top2_active_by_gender(
    top2_df: pd.DataFrame,
    first_year: int | None = None,
    max_year: int | None = None,
    figsize: tuple[int, int] = (12, 6),
):
    """
    Plot top 2 most active students per gender per year: one subplot per gender,
    grouped bars per year (rank 1 and rank 2 points). Returns a figure.
    Excludes year > max_year. Shows every year on x-axis.
    """
    import matplotlib.pyplot as plt
    import numpy as np
    if top2_df.empty or "rank" not in top2_df.columns:
        fig, ax = plt.subplots(figsize=figsize)
        ax.text(0.5, 0.5, "No data", ha="center", va="center")
        return fig
    df = top2_df.copy()
    if first_year is not None:
        df = df[df["year"] >= first_year]
    max_y = max_year if max_year is not None else PLOT_MAX_YEAR
    df = df[df["year"] <= max_y]
    if df.empty:
        fig, ax = plt.subplots(figsize=figsize)
        ax.text(0.5, 0.5, "No data", ha="center", va="center")
        return fig
    years = sorted(df["year"].unique())
    genders = sorted(df["gender"].unique().tolist())
    n_g = len(genders)
    fig, axes = plt.subplots(1, n_g, figsize=(figsize[0], figsize[1]), sharey=True)
    if n_g == 1:
        axes = [axes]
    for ax, gender in zip(axes, genders):
        sub = df[df["gender"] == gender]
        pivot = sub.pivot_table(index="year", columns="rank", values="points_gained", aggfunc="first").reindex(years)
        pivot = pivot.fillna(0)
        # Ensure column order 1, 2
        cols = [c for c in [1, 2] if c in pivot.columns] or sorted(pivot.columns)
        pivot = pivot[cols]
        x = np.arange(len(years))
        w = 0.35
        bar_color = GENDER_COLORS.get(gender, "gray")
        for i, rank in enumerate(cols):
            off = (i - 0.5) * w
            ax.bar(x + off, pivot[rank].values, width=w, label=f"Rank {rank}", color=bar_color, alpha=0.7 + 0.15 * (2 - i))
        ax.set_xticks(x)
        ax.set_xticklabels(years, rotation=45, ha="right")
        ax.set_ylabel("Points gained")
        ax.set_title(f"Top 2 – {gender}")
        ax.legend()
        ax.grid(True, alpha=0.3, axis="y")
    fig.suptitle("Most active students per year by gender (top 2)", fontsize=12, y=1.02)
    fig.tight_layout()
    return fig


def plot_cumulative_by_gender(
    yearly_by_gender_df: pd.DataFrame,
    metrics: list[str],
    first_year: int | None = None,
    max_year: int | None = None,
    ax=None,
):
    """Plot cumulative metrics by gender (one line per gender per metric). Excludes year > max_year. Shows every year on x-axis."""
    import matplotlib.pyplot as plt
    if yearly_by_gender_df.empty or not metrics:
        return
    df = yearly_by_gender_df.copy()
    df = df[df["metric"].isin(metrics)]
    if first_year is not None:
        df = df[df["year"] >= first_year]
    max_y = max_year if max_year is not None else PLOT_MAX_YEAR
    df = df[df["year"] <= max_y]
    if df.empty:
        return
    ax = ax or plt.gca()
    years = sorted(df["year"].unique())
    for gender in df["gender"].unique():
        for m in metrics:
            sub = df[(df["gender"] == gender) & (df["metric"] == m)]
            if sub.empty:
                continue
            ax.plot(sub["year"], sub["value"], label=f"{gender} ({m})", marker="o", markersize=3)
    ax.set_xlabel("Year")
    ax.set_ylabel("Cumulative")
    ax.set_title("Cumulative by gender")
    ax.set_xticks(years)
    ax.set_xticklabels(years)
    ax.legend(bbox_to_anchor=(1.02, 1), loc="upper left", fontsize=8)
    ax.grid(True, alpha=0.3)


def plot_per_year_by_gender(
    yearly_by_gender_df: pd.DataFrame,
    metric: str,
    first_year: int | None = None,
    max_year: int | None = None,
    ax=None,
):
    """Plot one per-year metric grouped by gender (grouped bars). Excludes year > max_year. Shows every year on x-axis."""
    import matplotlib.pyplot as plt
    if yearly_by_gender_df.empty:
        return
    df = yearly_by_gender_df[yearly_by_gender_df["metric"] == metric].copy()
    if first_year is not None:
        df = df[df["year"] >= first_year]
    max_y = max_year if max_year is not None else PLOT_MAX_YEAR
    df = df[df["year"] <= max_y]
    if df.empty:
        return
    ax = ax or plt.gca()
    genders = df["gender"].unique().tolist()
    years = sorted(df["year"].unique())
    x = range(len(years))
    w = 0.8 / max(len(genders), 1)
    for i, g in enumerate(genders):
        sub = df[df["gender"] == g].set_index("year").reindex(years, fill_value=0)
        off = (i - len(genders) / 2 + 0.5) * w
        ax.bar([xi + off for xi in x], sub["value"].tolist(), width=w, label=g)
    ax.set_xticks(x)
    ax.set_xticklabels(years)
    ax.set_xlabel("Year")
    ax.set_ylabel(metric)
    ax.set_title(f"Per-year: {metric} by gender")
    ax.legend()
    ax.grid(True, alpha=0.3, axis="y")


def build_country_yearly_report(
    country_id: int,
    sleep_seconds: float = 0.4,
    session: requests.Session | None = None,
):
    """
    Full pipeline: get count/pages -> urls -> profiles -> details -> resolutions ->
    yearly metrics, by-gender metrics, most active tables, and create plot figures.
    Returns dict with:
      profiles_df, events_df, yearly_metrics_df, yearly_metrics_by_gender_df,
      most_active_by_year_df, most_active_by_year_gender_df,
      first_year, country_count, num_pages,
      fig_cumulative, fig_per_year, fig_cumulative_gender, fig_per_year_gender
    """
    import matplotlib.pyplot as plt
    session = session or _get_session()

    logger.info("=== Starting country yearly report for country_id=%s ===", country_id)

    info = get_country_user_count_and_pages(country_id, session=session)
    country_count = info["country_count"]
    num_pages = info["num_pages"]

    logger.info("Step 1/6: Generating %s user list URL(s) ...", num_pages)
    urls = generate_country_user_urls(country_id, num_pages)
    profiles = extract_profiles_from_urls(urls, session=session)
    logger.info("Step 2/6: Scraping profile details ...")
    profiles_df = create_profiles_dataframe(profiles, session=session, sleep_seconds=sleep_seconds)
    if profiles_df.empty:
        logger.warning("No profiles scraped; returning empty report.")
        return {
            "profiles_df": profiles_df,
            "events_df": pd.DataFrame(),
            "yearly_metrics_df": pd.DataFrame(),
            "yearly_metrics_by_gender_df": pd.DataFrame(),
            "most_active_by_year_df": pd.DataFrame(),
            "most_active_by_year_gender_df": pd.DataFrame(),
            "first_year": None,
            "country_count": country_count,
            "num_pages": num_pages,
            "fig_cumulative": None,
            "fig_per_year": None,
            "fig_cumulative_gender": None,
            "fig_per_year_gender": None,
        }

    logger.info("Step 3/6: Scraping resolution history ...")
    events_df = build_events_df(profiles_df, session=session, sleep_seconds=sleep_seconds)
    logger.info("Step 4/6: Computing yearly metrics and by-gender ...")
    yearly_metrics_df = build_yearly_metrics(profiles_df, events_df)
    yearly_metrics_by_gender_df = build_yearly_metrics_by_gender(profiles_df, events_df)
    most_active_by_year_df = most_active_by_year(events_df)
    most_active_by_year_gender_df = most_active_by_year_gender(events_df, profiles_df)

    first_year = int(yearly_metrics_df["year"].min()) if not yearly_metrics_df.empty else None
    logger.info("Step 5/6: Building plots ...")
    fig_cumulative, ax1 = plt.subplots(figsize=(10, 5))
    plot_cumulative(yearly_metrics_df, first_year=first_year, max_year=PLOT_MAX_YEAR, ax=ax1)
    fig_cumulative.tight_layout()

    fig_per_year, ax2 = plt.subplots(figsize=(10, 5))
    plot_per_year(yearly_metrics_df, first_year=first_year, max_year=PLOT_MAX_YEAR, ax=ax2)
    fig_per_year.tight_layout()

    cum_metrics = ["cumulative_accounts_eoy", "cumulative_points_eoy", "cumulative_exercises_eoy", "cumulative_problems_eoy"]
    fig_cumulative_gender, ax3 = plt.subplots(figsize=(10, 5))
    plot_cumulative_by_gender(yearly_metrics_by_gender_df, cum_metrics, first_year=first_year, max_year=PLOT_MAX_YEAR, ax=ax3)
    fig_cumulative_gender.tight_layout()

    fig_per_year_gender, ax4 = plt.subplots(figsize=(10, 5))
    plot_per_year_by_gender(yearly_metrics_by_gender_df, "points_gained_per_year", first_year=first_year, max_year=PLOT_MAX_YEAR, ax=ax4)
    fig_per_year_gender.tight_layout()

    logger.info("Step 6/6: Done. Report ready (first_year=%s, %s profiles, %s events).", first_year, len(profiles_df), len(events_df))
    out = {
        "profiles_df": profiles_df,
        "events_df": events_df,
        "yearly_metrics_df": yearly_metrics_df,
        "yearly_metrics_by_gender_df": yearly_metrics_by_gender_df,
        "most_active_by_year_df": most_active_by_year_df,
        "most_active_by_year_gender_df": most_active_by_year_gender_df,
        "first_year": first_year,
        "country_count": country_count,
        "num_pages": num_pages,
        "fig_cumulative": fig_cumulative,
        "fig_per_year": fig_per_year,
        "fig_cumulative_gender": fig_cumulative_gender,
        "fig_per_year_gender": fig_per_year_gender,
    }
    # Top-3 and top-2-by-gender: name-only tables for display
    top3_detail = most_active_top_n(events_df, n=3)
    top2_detail = most_active_top_n_by_gender(events_df, profiles_df, n=2)
    out["most_active_top3_df"] = most_active_top3_names_table(top3_detail)
    out["most_active_top2_by_gender_df"] = most_active_top2_by_gender_names_table(top2_detail)
    return out


def build_report_from_data(data: dict):
    """
    Build all report figures and extra tables from loaded report data (no scraping).
    Use after load_report_data(). Returns a report dict with same data plus:
    - fig_per_year_signups, fig_per_year_points, fig_per_year_exercises, fig_per_year_problems
    - fig_cumulative_gender_signups, fig_cumulative_gender_points, fig_cumulative_gender_exercises, fig_cumulative_gender_problems (bar; male=blue, female=pink)
    - fig_pies_2025_gender (4 pies: accounts, points, exercises, problems for 2025)
    - fig_top2_active_by_gender
    - most_active_top3_df, most_active_top2_by_gender_df
    """
    import matplotlib.pyplot as plt
    profiles_df = data.get("profiles_df", pd.DataFrame())
    events_df = data.get("events_df", pd.DataFrame())
    yearly_metrics_df = data.get("yearly_metrics_df", pd.DataFrame())
    yearly_metrics_by_gender_df = data.get("yearly_metrics_by_gender_df", pd.DataFrame())
    first_year = data.get("first_year")

    out = dict(data)
    if profiles_df.empty and events_df.empty:
        logger.warning("No data to build report from.")
        return out

    # Top-3 and top-2-by-gender: name-only tables for display (detail kept for plot)
    top3_detail = most_active_top_n(events_df, n=3)
    top2_detail = most_active_top_n_by_gender(events_df, profiles_df, n=2)
    out["most_active_top3_df"] = most_active_top3_names_table(top3_detail)
    out["most_active_top2_by_gender_df"] = most_active_top2_by_gender_names_table(top2_detail)

    max_year = PLOT_MAX_YEAR
    # Cumulative (overall): 4 separate bar charts with value labels (reference layout)
    for metric, key in [
        ("cumulative_accounts_eoy", "fig_cumulative_accounts"),
        ("cumulative_points_eoy", "fig_cumulative_points"),
        ("cumulative_exercises_eoy", "fig_cumulative_exercises"),
        ("cumulative_problems_eoy", "fig_cumulative_problems"),
    ]:
        fig, ax = plt.subplots(figsize=(8, 4))
        plot_cumulative_single_bar(yearly_metrics_df, metric, first_year=first_year, max_year=max_year, ax=ax)
        fig.tight_layout()
        out[key] = fig

    # Per-year: 4 separate plots
    for metric, key in [
        ("signups_per_year", "fig_per_year_signups"),
        ("points_gained_per_year", "fig_per_year_points"),
        ("exercises_completed_per_year", "fig_per_year_exercises"),
        ("problems_solved_per_year", "fig_per_year_problems"),
    ]:
        fig, ax = plt.subplots(figsize=(8, 4))
        plot_per_year_single(yearly_metrics_df, metric, first_year=first_year, max_year=max_year, ax=ax)
        fig.tight_layout()
        out[key] = fig

    # Cumulative by gender: 4 bar plots
    for metric, key in [
        ("cumulative_accounts_eoy", "fig_cumulative_gender_signups"),
        ("cumulative_points_eoy", "fig_cumulative_gender_points"),
        ("cumulative_exercises_eoy", "fig_cumulative_gender_exercises"),
        ("cumulative_problems_eoy", "fig_cumulative_gender_problems"),
    ]:
        fig, ax = plt.subplots(figsize=(8, 4))
        plot_cumulative_by_gender_bar(yearly_metrics_by_gender_df, metric, first_year=first_year, max_year=max_year, ax=ax)
        fig.tight_layout()
        out[key] = fig

    # 2025 gender pie charts: % male vs female (accounts, points, exercises, problems)
    out["fig_pies_2025_gender"] = plot_gender_pies_2025(yearly_metrics_by_gender_df, year=2025)

    # Top 2 most active per gender (plot uses detail table with points/rank)
    out["fig_top2_active_by_gender"] = plot_top2_active_by_gender(
        top2_detail, first_year=first_year, max_year=max_year
    )

    return out
