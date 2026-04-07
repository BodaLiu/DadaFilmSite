import json
import os
from pathlib import Path

os.environ.setdefault("MPLCONFIGDIR", str(Path(".matplotlib-cache").resolve()))
os.environ.setdefault("XDG_CACHE_HOME", str(Path(".cache").resolve()))

import matplotlib.pyplot as plt
import pandas as pd


INPUT_JSON = Path("cannes_ratings.json")
OUTPUT_SUMMARY_CSV = Path("cannes_yearly_summary.csv")
OUTPUT_RANKING_CSV = Path("cannes_yearly_ranking.csv")
OUTPUT_TOP_FILMS_CSV = Path("cannes_top_films_by_year.csv")
OUTPUT_BOXPLOT_PNG = Path("cannes_yearly_ratings_boxplot.png")


def load_rows():
    data = json.loads(INPUT_JSON.read_text(encoding="utf-8"))
    rows = []

    for year_str, payload in sorted(data.items(), key=lambda item: int(item[0])):
        year = int(year_str)
        if year == 2020:
            continue

        for film in payload.get("films", []):
            rating = film.get("rating")
            if rating is None:
                continue

            rows.append(
                {
                    "year": year,
                    "title": film.get("title"),
                    "slug": film.get("slug"),
                    "rating": float(rating),
                    "rating_count": film.get("count"),
                }
            )

    return pd.DataFrame(rows)


def main():
    df = load_rows()
    if df.empty:
        raise RuntimeError("No rated films found in cannes_ratings.json")

    yearly_summary = (
        df.groupby("year")
        .agg(
            film_count=("rating", "size"),
            mean_rating=("rating", "mean"),
            median_rating=("rating", "median"),
            std_rating=("rating", "std"),
            min_rating=("rating", "min"),
            max_rating=("rating", "max"),
        )
        .round(3)
        .reset_index()
        .sort_values("year")
    )

    yearly_summary["rank_by_mean"] = (
        yearly_summary["mean_rating"].rank(ascending=False, method="min").astype(int)
    )
    yearly_summary["rank_by_median"] = (
        yearly_summary["median_rating"].rank(ascending=False, method="min").astype(int)
    )
    yearly_summary["rank_by_min"] = (
        yearly_summary["min_rating"].rank(ascending=False, method="min").astype(int)
    )

    best_films = (
        df.sort_values(["year", "rating", "rating_count"], ascending=[True, False, False])
        .groupby("year")
        .first()
        .reset_index()[["year", "title", "rating"]]
        .rename(columns={"title": "best_film", "rating": "best_film_rating"})
    )

    worst_films = (
        df.sort_values(["year", "rating", "rating_count"], ascending=[True, True, False])
        .groupby("year")
        .first()
        .reset_index()[["year", "title", "rating"]]
        .rename(columns={"title": "worst_film", "rating": "worst_film_rating"})
    )

    yearly_summary = yearly_summary.merge(best_films, on="year", how="left")
    yearly_summary = yearly_summary.merge(worst_films, on="year", how="left")
    yearly_summary.to_csv(OUTPUT_SUMMARY_CSV, index=False, encoding="utf-8-sig")

    ranking = yearly_summary.sort_values(
        ["rank_by_mean", "median_rating", "min_rating"],
        ascending=[True, False, False],
    ).reset_index(drop=True)
    ranking.to_csv(OUTPUT_RANKING_CSV, index=False, encoding="utf-8-sig")

    top_films = (
        df.sort_values(["year", "rating", "rating_count"], ascending=[True, False, False])
        .groupby("year")
        .head(3)
        .copy()
    )
    top_films["year_rank"] = top_films.groupby("year").cumcount() + 1
    top_films = top_films[
        ["year", "year_rank", "title", "rating", "rating_count", "slug"]
    ].sort_values(["year", "year_rank"])
    top_films.to_csv(OUTPUT_TOP_FILMS_CSV, index=False, encoding="utf-8-sig")

    years = sorted(df["year"].unique())
    boxplot_data = [df.loc[df["year"] == year, "rating"].values for year in years]

    fig, ax = plt.subplots(figsize=(14, 7), facecolor="#f7f1e3")
    bp = ax.boxplot(
        boxplot_data,
        tick_labels=years,
        patch_artist=True,
        showmeans=True,
        meanprops={
            "marker": "D",
            "markerfacecolor": "#c0392b",
            "markeredgecolor": "#c0392b",
            "markersize": 6,
        },
        medianprops={"color": "#1f2937", "linewidth": 2},
        whiskerprops={"color": "#7f8c8d"},
        capprops={"color": "#7f8c8d"},
        flierprops={
            "marker": "o",
            "markerfacecolor": "#d97706",
            "markeredgecolor": "#d97706",
            "markersize": 4,
            "alpha": 0.55,
        },
    )

    palette = [
        "#4c78a8",
        "#f58518",
        "#54a24b",
        "#e45756",
        "#72b7b2",
        "#b279a2",
        "#ff9da6",
        "#9d755d",
    ]
    for patch, color in zip(bp["boxes"], palette * 2):
        patch.set_facecolor(color)
        patch.set_alpha(0.75)
        patch.set_edgecolor("#ffffff")
        patch.set_linewidth(1.5)

    means = yearly_summary.set_index("year")["mean_rating"]
    ax.plot(
        range(1, len(years) + 1),
        [means[year] for year in years],
        color="#2c3e50",
        linewidth=1.5,
        marker="o",
        markersize=4,
        alpha=0.8,
        label="Mean rating",
    )

    ax.set_title("Cannes Main Competition Ratings by Year", fontsize=18, weight="bold")
    ax.set_xlabel("Year")
    ax.set_ylabel("Letterboxd Rating")
    ax.set_facecolor("#fffaf2")
    ax.grid(axis="y", linestyle="--", alpha=0.25)
    ax.legend(frameon=False, loc="upper right")

    fig.text(
        0.125,
        0.92,
        "2020 excluded because Cannes main competition was not held normally.",
        fontsize=10.5,
        color="#5c6773",
    )

    plt.tight_layout()
    plt.savefig(OUTPUT_BOXPLOT_PNG, dpi=300, bbox_inches="tight")
    plt.close()

    print("Yearly Cannes reputation ranking by mean rating:")
    print(
        ranking[
            ["rank_by_mean", "year", "mean_rating", "median_rating", "best_film", "best_film_rating"]
        ].to_string(index=False)
    )
    print(f"\nSaved summary: {OUTPUT_SUMMARY_CSV.resolve()}")
    print(f"Saved ranking: {OUTPUT_RANKING_CSV.resolve()}")
    print(f"Saved top films: {OUTPUT_TOP_FILMS_CSV.resolve()}")
    print(f"Saved boxplot: {OUTPUT_BOXPLOT_PNG.resolve()}")


if __name__ == "__main__":
    main()
