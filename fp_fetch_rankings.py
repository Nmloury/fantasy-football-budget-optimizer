"""
Download FantasyPros season-long projections and convert to per-game for use as rankings_csv.

Outputs: fp_rankings_MMDDYYYY.csv   (e.g., fp_rankings_08142025.csv)

Requirements:
  pip install pandas requests lxml

Notes:
- For RB/WR/TE we use HALF-PPR URLs you provided.
- If FantasyPros changes HTML/CSV schema, you may need to update the column mapping below.
"""

import argparse
import io
import os
import sys
import time
from datetime import datetime
from urllib.parse import urlparse, parse_qsl, urlencode, urlunparse

import pandas as pd
import requests


URLS = [
    # (url, pos)
    ("https://www.fantasypros.com/nfl/projections/qb.php?week=draft", "QB"),
    ("https://www.fantasypros.com/nfl/projections/rb.php?week=draft&scoring=HALF&week=draft", "RB"),
    ("https://www.fantasypros.com/nfl/projections/wr.php?week=draft&scoring=HALF&week=draft", "WR"),
    ("https://www.fantasypros.com/nfl/projections/te.php?week=draft&scoring=HALF&week=draft", "TE"),
]


HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; projections-scraper/1.0; +https://example.com)",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}


def add_or_update_query(url: str, **params):
    """Return url with added/updated query parameters."""
    parts = urlparse(url)
    q = dict(parse_qsl(parts.query, keep_blank_values=True))
    q.update(params)
    new_query = urlencode(q, doseq=True)
    return urlunparse((parts.scheme, parts.netloc, parts.path, parts.params, new_query, parts.fragment))


def try_fetch_csv(url: str, timeout: int = 30):
    """
    Try FantasyPros CSV export by appending csv=1.
    Return DataFrame if successful; else None.
    """
    csv_url = add_or_update_query(url, csv="1")
    resp = requests.get(csv_url, headers=HEADERS, timeout=timeout)
    if resp.status_code != 200:
        return None

    text = resp.text.strip()
    # quick heuristic: must look like CSV with a header that includes "Player"
    if "Player" not in text.splitlines()[0]:
        return None

    try:
        df = pd.read_csv(io.StringIO(text))
        return df
    except Exception:
        return None


def clean_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Lower-case and normalize column names; flatten any multiindex."""
    if isinstance(df.columns, pd.MultiIndex):
        # Keep only the lowest level (last level) of the MultiIndex
        df.columns = [str(col[-1]).strip() for col in df.columns.values]
    df.columns = [c.strip().lower() for c in df.columns]
    return df


def locate_projection_table(tables: list[pd.DataFrame]):
    """
    Among dataframes read from HTML, pick the one that has a Player column and FPTS (seasonal total).
    Accept common synonyms for fpts.
    """
    fpts_aliases = {"fpts", "fantasy pts", "fantasypts", "points", "misc fpts"}
    for t in tables:
        df = clean_columns(t.copy())
        cols = set(df.columns)
        if "player" in cols and any(alias in cols for alias in fpts_aliases):
            return df
    return None


def fetch_html_table(url: str, timeout: int = 30):
    """
    Fallback: pull the HTML and parse tables with pandas.read_html.
    Return DataFrame if we find a matching table; else None.
    """
    resp = requests.get(url, headers=HEADERS, timeout=timeout)
    resp.raise_for_status()
    try:
        tables = pd.read_html(io.StringIO(resp.text), flavor="lxml")
    except ValueError:
        # no tables found
        return None
    return locate_projection_table(tables)


def extract_player_team_fpts(df: pd.DataFrame):
    """
    Keep only 'player','team','fpts' (season totals). Handle common header variants.
    If team is embedded or missing, try to split/clean gracefully.
    """
    df = clean_columns(df)

    # Player
    if "player" not in df.columns:
        # Some FP exports may use 'name'
        if "name" in df.columns:
            df["player"] = df["name"]
        else:
            raise ValueError("Could not find 'player' column in downloaded table.")

    # Team - check if there's a separate team column first
    team_col = None
    for c in ("team", "tm"):
        if c in df.columns:
            team_col = c
            break
    
    if team_col is None:
        # Extract team from 'player' column if embedded like "Lamar Jackson BAL"
        def extract_player_team(player_str):
            parts = str(player_str).strip().split()
            if len(parts) >= 2:
                # Last part is likely the team abbreviation (usually 2-3 chars)
                potential_team = parts[-1]
                if len(potential_team) <= 4 and potential_team.isupper():
                    return " ".join(parts[:-1]), potential_team
            return player_str, pd.NA
        
        # Apply extraction
        extracted = df["player"].apply(extract_player_team)
        df["player"] = [x[0] for x in extracted]
        df["team"] = [x[1] for x in extracted]
    else:
        df["team"] = df[team_col]

    # FPTS (season total)
    fpts_col = None
    for c in ("fpts", "fantasy pts", "fantasypts", "points", "total fpts"):
        if c in df.columns:
            fpts_col = c
            break
    if fpts_col is None:
        raise ValueError("Could not find 'FPTS' (season total) in downloaded table.")

    out = df[["player", "team", fpts_col]].copy()
    out.rename(columns={fpts_col: "fpts"}, inplace=True)

    # Coerce types
    out["player"] = out["player"].astype(str).str.strip()
    out["team"] = out["team"].astype(str).str.strip()
    out["fpts"] = pd.to_numeric(out["fpts"], errors="coerce")

    # Drop rows without players or points
    out = out[(out["player"] != "") & out["fpts"].notna()].reset_index(drop=True)
    return out


def fetch_position(url: str, pos_label: str, weeks: int = 17, retries: int = 2, backoff: float = 1.5):
    """
    Pull one position, prefer CSV endpoint, else HTML table.
    Returns columns: player, team, proj_pts, pos
    """
    last_err = None
    for attempt in range(retries + 1):
        try:
            # Try CSV export first
            df = try_fetch_csv(url)
            if df is None:
                # Fallback to HTML table read
                df = fetch_html_table(url)
            if df is None or df.empty:
                raise ValueError("No data table found.")

            base = extract_player_team_fpts(df)
            base["proj_pts"] = (base["fpts"] / float(weeks)).round(2)
            base["pos"] = pos_label
            return base[["player", "team", "proj_pts", "pos"]]
        except Exception as e:
            last_err = e
            if attempt < retries:
                time.sleep(backoff ** attempt)
            else:
                raise RuntimeError(f"Failed to fetch {pos_label} from {url}: {e}") from e
    # Should not reach here
    raise last_err


def main():
    parser = argparse.ArgumentParser(description="Download FantasyPros season projections and make per-game rankings CSV.")
    parser.add_argument("--weeks", type=int, default=17, help="Games to divide season FPTS by (default: 17).")
    parser.add_argument("--out", type=str, default=None, help="Output CSV filename (default: fp_rankings_MMDDYYYY.csv).")
    args = parser.parse_args()

    out_name = args.out
    if not out_name:
        out_name = f"fp_rankings_{datetime.now().strftime('%m%d%Y')}.csv"
    
    # Ensure output goes to data/ folder
    if not os.path.isabs(out_name):
        data_dir = "data"
        os.makedirs(data_dir, exist_ok=True)
        out_name = os.path.join(data_dir, out_name)

    frames = []
    for url, pos in URLS:
        print(f"[INFO] Fetching {pos} from {url} ...")
        df_pos = fetch_position(url, pos_label=pos, weeks=args.weeks)
        print(f"[INFO]  -> {len(df_pos)} rows")
        frames.append(df_pos)

    df_all = pd.concat(frames, ignore_index=True)

    # Basic sanity filter: keep only expected positions
    df_all = df_all[df_all["pos"].isin(["QB", "RB", "WR", "TE"])].copy()

    # Final ordering and dtypes
    df_all = df_all[["player", "team", "proj_pts", "pos"]]
    df_all["proj_pts"] = pd.to_numeric(df_all["proj_pts"], errors="coerce")

    df_all.to_csv(out_name, index=False)
    print(f"[DONE] Wrote {len(df_all)} rows to {out_name}")


if __name__ == "__main__":
    sys.exit(main())
