#!/usr/bin/env python3
"""
Cannes main competition Letterboxd ratings, 2016-2025.

Usage:
    pip install requests beautifulsoup4 pandas matplotlib
    python cannes_letterboxd.py

Output:
    cannes_ratings.json    — raw data (resume cache)
    cannes_summary.csv     — per-year stats
    cannes_boxplot.png     — boxplot figure
"""

import json, re, time, sys, os
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple
import requests
from bs4 import BeautifulSoup

UA = {"User-Agent": "Mozilla/5.0 (compatible; cannes-research/1.0)"}
CACHE = Path("cannes_ratings.json")
YEARS = range(2016, 2026)
SLEEP = 0.4  # be polite to Letterboxd
DEBUG = True
TARGET_YEARS = {2016, 2017, 2018, 2019}
SKIP_YEARS = {2020}
PROCESS_YEARS = sorted(TARGET_YEARS)
MAX_LIST_PAGES = 40
LETTERBOXD_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/123.0.0.0 Safari/537.36"
    ),
    "Accept": (
        "text/html,application/xhtml+xml,application/xml;q=0.9,"
        "image/avif,image/webp,*/*;q=0.8"
    ),
    "Accept-Language": "en-US,en;q=0.9",
    "Cache-Control": "no-cache",
    "Pragma": "no-cache",
    "Referer": "https://letterboxd.com/",
}
MAIN_COMP_LIST_URLS = [
    "https://letterboxd.com/corebasis/list/all-cannes-film-festival-main-competition/detail/",
    "https://letterboxd.com/corebasis/list/all-cannes-film-festival-main-competition/",
]


# ---------- 1. Wikipedia: pull each year's main competition list ----------

def fetch_letterboxd_html(url: str) -> str:
    session = requests.Session()
    r = session.get(url, headers=LETTERBOXD_HEADERS, timeout=30)
    if DEBUG:
        print(f"[debug] GET {url} -> {r.status_code} ({len(r.text)} chars)")
    r.raise_for_status()
    return r.text


def fetch_letterboxd_soup(url: str) -> BeautifulSoup:
    return BeautifulSoup(fetch_letterboxd_html(url), "html.parser")


def collect_list_page_urls() -> List[str]:
    """Collect paginated URLs for the curated Cannes list in order."""
    base_url = None
    for candidate in MAIN_COMP_LIST_URLS:
        if "/detail/" in candidate:
            continue
        base_url = candidate
        break
    if base_url is None:
        base_url = MAIN_COMP_LIST_URLS[-1]

    base_url = re.sub(r"page/\d+/?$", "", base_url)
    if not base_url.endswith("/"):
        base_url += "/"

    urls = [base_url]
    for page_num in range(2, MAX_LIST_PAGES + 1):
        urls.append(f"{base_url}page/{page_num}/")

    if DEBUG:
        print("[debug] Candidate list pages:")
        for url in urls:
            print(f"  - {url}")
    return urls


def parse_slugs_from_list_html(soup: BeautifulSoup, html: str) -> List[str]:
    slugs = []
    seen = set()

    for elem in soup.select("[data-film-slug]"):
        slug = (elem.get("data-film-slug") or "").strip()
        if slug and slug not in seen:
            slugs.append(slug)
            seen.add(slug)

    for a in soup.select('a[href^="/film/"]'):
        href = (a.get("href") or "").strip()
        m = re.match(r"^/film/([^/]+)/?$", href)
        if not m:
            continue
        slug = m.group(1)
        if slug not in seen:
            slugs.append(slug)
            seen.add(slug)

    for elem in soup.select("[data-target-link]"):
        href = (elem.get("data-target-link") or "").strip()
        m = re.match(r"^/film/([^/]+)/?$", href)
        if not m:
            continue
        slug = m.group(1)
        if slug not in seen:
            slugs.append(slug)
            seen.add(slug)

    for slug in re.findall(r'/film/([^/"?#]+)/', html):
        if slug not in seen:
            slugs.append(slug)
            seen.add(slug)

    if DEBUG:
        print(f"[debug] Parsed {len(slugs)} unique slugs from current list page")
        for slug in slugs[:10]:
            print(f"    slug: {slug}")

    return slugs


def get_competition_films_from_letterboxd_list() -> Dict[int, List[dict]]:
    """Return {year: [{'title': str, 'slug': str}, ...]} from a curated Letterboxd list."""
    last_error = None
    by_year = {year: [] for year in PROCESS_YEARS}
    seen_slugs = set()

    found_2019 = False
    found_target_years = set()
    passed_before_2016 = False

    for url in collect_list_page_urls():
        try:
            html = fetch_letterboxd_html(url)
            soup = BeautifulSoup(html, "html.parser")
        except Exception as e:
            last_error = e
            continue

        page_slugs = parse_slugs_from_list_html(soup, html)
        if DEBUG and not page_slugs:
            text_sample = soup.get_text(" ", strip=True)[:400]
            print(f"[debug] No slugs found on {url}")
            print(f"[debug] Page text sample: {text_sample}")

        page_years = set()
        parsed_on_page = 0
        for slug in page_slugs:
            if slug in seen_slugs:
                continue
            seen_slugs.add(slug)
            year, title = lb_film_year_and_title(slug)
            if DEBUG:
                print(f"[debug] Film page -> slug={slug}, year={year}, title={title}")
            if year is not None:
                page_years.add(year)
            if year in by_year and title:
                by_year[year].append({"title": title, "slug": slug})
                parsed_on_page += 1
                found_target_years.add(year)

        if DEBUG:
            page_years_display = sorted(y for y in page_years if 1900 <= y <= 2100)
            print(f"[debug] Accepted {parsed_on_page} films from {url}")
            print(f"[debug] Years seen on page: {page_years_display[:20]}")

        if 2019 in page_years:
            found_2019 = True

        if found_2019 and page_years and max(page_years) < 2016:
            passed_before_2016 = True

        if found_2019 and found_target_years == TARGET_YEARS:
            if DEBUG:
                print("[debug] Found all target years 2016-2019; stopping pagination")
            break

        if found_2019 and passed_before_2016:
            if DEBUG:
                print("[debug] Pagination has moved earlier than 2016; stopping")
            break

    total = sum(len(v) for v in by_year.values())
    if DEBUG:
        print("[debug] Parsed films by year:")
        for year in PROCESS_YEARS:
            print(f"  {year}: {len(by_year[year])}")
    if total == 0:
        raise RuntimeError(f"No films parsed from Letterboxd main competition list ({last_error})")

    return by_year

def get_competition_films(year: int) -> List[dict]:
    """Return [{'title': str, 'director': str}, ...] for main competition."""
    url = f"https://en.wikipedia.org/wiki/{year}_Cannes_Film_Festival"
    r = requests.get(url, headers=UA, timeout=30)
    r.raise_for_status()
    soup = BeautifulSoup(r.text, "html.parser")

    # First locate the "Official Selection" section, then find its
    # "Main Competition" subsection. Without this, the script can accidentally
    # lock onto the earlier "Juries > Main competition" heading.
    official_selection = None
    for h in soup.find_all("h2"):
        span = h.find("span", class_="mw-headline") or h
        txt = span.get_text(" ", strip=True).lower()
        if "official selection" in txt:
            official_selection = h
            break

    if official_selection is None:
        raise RuntimeError(f"{year}: no 'Official Selection' section")

    anchor = None
    for node in official_selection.find_next_siblings():
        if node.name == "h2":
            break
        if node.name not in ("h3", "h4"):
            continue
        span = node.find("span", class_="mw-headline") or node
        txt = span.get_text(" ", strip=True).lower()
        if txt in ("main competition", "in competition", "competition"):
            anchor = node
            break

    if anchor is None:
        raise RuntimeError(f"{year}: no main competition subsection under 'Official Selection'")

    # Find the first plausible table under that subsection.
    table = None
    for node in anchor.find_next_siblings():
        if node.name in ("h2", "h3"):
            break
        if node.name != "table":
            continue
        header_cells = [
            cell.get_text(" ", strip=True).lower()
            for cell in node.find_all("th")
        ]
        header_text = " | ".join(header_cells)
        if "director" in header_text and ("title" in header_text or "film" in header_text):
            table = node
            break

    if table is None:
        raise RuntimeError(f"{year}: no competition table found under main competition subsection")

    films = []
    for tr in table.find_all("tr")[1:]:
        cells = tr.find_all(["td", "th"])
        if len(cells) < 3:
            continue
        # Columns are typically: English title | Original title | Director(s) | Country
        title = re.sub(r"\[.*?\]|\(.*?\)", "",
                       cells[0].get_text(" ", strip=True)).strip()
        director = re.sub(r"\[.*?\]", "",
                          cells[2].get_text(" ", strip=True)).strip()
        if title:
            films.append({"title": title, "director": director})
    if not films:
        raise RuntimeError(f"{year}: competition table parsed but produced no films")
    return films


# ---------- 2. Letterboxd: search → film slug → rating ----------

def lb_search_slug(title: str, director: str, year: int) -> Optional[str]:
    """Search Letterboxd and return the best-matching film slug."""
    q = requests.utils.quote(title)
    r = requests.get(f"https://letterboxd.com/search/films/{q}/",
                     headers=UA, timeout=30)
    if r.status_code != 200:
        return None
    soup = BeautifulSoup(r.text, "html.parser")

    # Search results expose data-film-slug on each poster container
    candidates = []
    for div in soup.select("[data-film-slug]"):
        slug = div.get("data-film-slug")
        if slug and slug not in candidates:
            candidates.append(slug)
    # Fallback: anchor hrefs
    if not candidates:
        for a in soup.select('a[href^="/film/"]'):
            m = re.match(r"^/film/([^/]+)/?$", a["href"])
            if m and m.group(1) not in candidates:
                candidates.append(m.group(1))

    last_name = director.split(",")[0].split(" and ")[0].strip().split()[-1].lower() \
        if director else ""

    for slug in candidates[:6]:
        film_url = f"https://letterboxd.com/film/{slug}/"
        rr = requests.get(film_url, headers=UA, timeout=30)
        if rr.status_code != 200:
            continue
        text = rr.text.lower()
        # Verify director (if we have one) and year (±1 for festival vs release)
        dir_ok = (not last_name) or (last_name in text)
        year_ok = any(str(y) in text for y in (year, year - 1, year + 1))
        if dir_ok and year_ok:
            return slug
    return candidates[0] if candidates else None


def lb_rating(slug: str) -> Tuple[Optional[float], Optional[int]]:
    """Fetch a Letterboxd film page and extract rating + count from JSON-LD."""
    r = requests.get(f"https://letterboxd.com/film/{slug}/",
                     headers=LETTERBOXD_HEADERS, timeout=30)
    if r.status_code != 200:
        return None, None
    # ratingValue and ratingCount live in a JSON-LD <script> block
    rv = re.search(r'"ratingValue":\s*([0-9.]+)', r.text)
    rc = re.search(r'"ratingCount":\s*([0-9]+)', r.text)
    return (float(rv.group(1)) if rv else None,
            int(rc.group(1)) if rc else None)


def lb_film_year_and_title(slug: str) -> Tuple[Optional[int], Optional[str]]:
    """Fetch a Letterboxd film page and extract release year + title."""
    r = requests.get(
        f"https://letterboxd.com/film/{slug}/",
        headers=LETTERBOXD_HEADERS,
        timeout=30,
    )
    if r.status_code != 200:
        if DEBUG:
            print(f"[debug] Film page request failed for {slug}: {r.status_code}")
        return None, None

    html = r.text
    title = None
    year = None

    og_title = re.search(r'<meta property="og:title" content="([^"]+)"', html)
    if og_title:
        title_text = og_title.group(1).strip()
        title_text = re.sub(r"\s*\(\d{4}\)\s*$", "", title_text).strip()
        if title_text:
            title = title_text

    year_match = re.search(r'"datePublished"\s*:\s*"(\d{4})', html)
    if not year_match:
        year_match = re.search(r'/films/year/(\d{4})/', html)
    if year_match:
        year = int(year_match.group(1))
    elif DEBUG:
        print(f"[debug] No year match for slug={slug}")
        print(f"[debug] Film HTML sample: {html[:400]}")

    return year, title


# ---------- 3. Orchestrate with on-disk cache ----------

def load_cache() -> dict:
    if CACHE.exists():
        return json.loads(CACHE.read_text())
    return {}

def save_cache(data: dict) -> None:
    CACHE.write_text(json.dumps(data, ensure_ascii=False, indent=2))

def main() -> None:
    data = load_cache()

    print("Fetching Cannes main competition titles from Letterboxd list...")
    list_films_by_year = get_competition_films_from_letterboxd_list()

    for year in PROCESS_YEARS:
        key = str(year)
        existing_films = data.get(key, {}).get("films", [])
        source_films = list_films_by_year.get(year, [])
        existing_by_slug = {
            f.get("slug"): f for f in existing_films if f.get("slug")
        }

        merged_films = []
        for film in source_films:
            old = existing_by_slug.get(film["slug"], {})
            merged = dict(film)
            for cache_field in ("rating", "count"):
                if cache_field in old:
                    merged[cache_field] = old[cache_field]
            merged_films.append(merged)

        if merged_films:
            data[key] = {"films": merged_films}
        else:
            data.setdefault(key, {"films": existing_films})
        save_cache(data)

        films = data[key]["films"]
        print(f"[{year}] {len(films)} films from Letterboxd list")
        for f in films:
            if "rating" in f:  # already done
                continue
            try:
                slug = f.get("slug") or lb_search_slug(f["title"], "", year)
                if slug:
                    rating, count = lb_rating(slug)
                else:
                    rating = count = None
                f["slug"] = slug
                f["rating"] = rating
                f["count"] = count
                print(f"  {f['title'][:38]:38} -> {slug or '-':35} {rating} ({count})")
            except Exception as e:
                print(f"  ! {f['title']}: {e}")
                f["rating"] = None
            save_cache(data)
            time.sleep(SLEEP)

    # ---------- 4. Analysis ----------
    import pandas as pd
    import matplotlib.pyplot as plt

    rows = []
    for year, payload in sorted(data.items(), key=lambda x: int(x[0])):
        for f in payload["films"]:
            if f.get("rating") is not None:
                rows.append({"year": int(year), "title": f["title"],
                             "director": f.get("director"),
                             "rating": f["rating"],
                             "count": f.get("count")})
    df = pd.DataFrame(rows)
    print("\nMissing ratings:",
          sum(1 for y in data.values() for f in y["films"]
              if f.get("rating") is None))

    if df.empty:
        print("\nNo ratings were collected, so summary/boxplot were skipped.")
        return

    summary = df.groupby("year")["rating"].agg(
        ["count", "mean", "median", "std", "min", "max"]).round(3)
    print("\n", summary)
    summary.to_csv("cannes_summary.csv")

    # Boxplot
    fig, ax = plt.subplots(figsize=(11, 6))
    years_sorted = sorted(df["year"].unique())
    ax.boxplot([df[df.year == y]["rating"].values for y in years_sorted],
               labels=years_sorted, showmeans=True,
               meanprops={"marker": "D", "markerfacecolor": "red",
                          "markeredgecolor": "red", "markersize": 6})
    ax.set_xlabel("Year")
    ax.set_ylabel("Letterboxd average rating")
    ax.set_title("Cannes Main Competition — Letterboxd ratings, 2016–2025")
    ax.grid(axis="y", alpha=0.3)
    plt.tight_layout()
    plt.savefig("cannes_boxplot.png", dpi=150)
    print("\nSaved cannes_boxplot.png, cannes_summary.csv, cannes_ratings.json")


if __name__ == "__main__":
    main()
