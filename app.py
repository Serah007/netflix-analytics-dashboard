# ============================================================
# BIG DATA ANALYTICS OF NETFLIX CONTENT AND USER RATINGS
# Streamlit dashboard  |  run with:  streamlit run app.py
# ============================================================
#
# Project:
# Big Data Analytics of Netflix Movies and TV Shows
# for Content Trend Analysis
#
# TECHNOLOGIES USED:
#
# 1. Python
# 2. PySpark
# 3. Apache Spark
# 4. Spark SQL
#
# Datasets:
# 1. Netflix Movies and TV Shows   (netflix_titles.csv)
# 2. IMDb Top 1000 Movies          (imdb_top_1000.csv)
# 3. Netflix Prize Ratings         (combined_data_1.txt)
#
# How it works:
#   - The PySpark pipeline (load -> dedupe -> missing values -> clean ->
#     normalize titles -> Netflix + IMDb merge -> Netflix Prize parsing)
#     runs ONCE and is cached for the whole Streamlit session.
#   - Every chart is a Spark SQL query. The "Content type" toggle and the
#     "Release year" slider are injected into the WHERE clause, so the
#     whole dashboard re-queries Spark whenever they change.
# ============================================================

import os

import pandas as pd
import plotly.express as px
import streamlit as st

from pyspark import StorageLevel
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    trim,
    lower,
    regexp_replace,
    regexp_extract,
    count,
    when,
    to_date,
    last,
    monotonically_increasing_id,
)
from pyspark.sql.window import Window


# ============================================================
# PAGE SETUP + THEME
# ============================================================

st.set_page_config(
    page_title="Netflix Dashboard",
    page_icon="🍿",
    layout="wide",
)

NETFLIX_RED = "#E50914"
SOFT_RED = "#E5495A"
OFF_WHITE = "#F5F5F1"
GREY = "#8C8C8C"
TYPE_COLORS = {"Movie": NETFLIX_RED, "TV Show": OFF_WHITE}
GENRE_COLORS = [
    "#E5495A", "#E0B51B", "#2F8CFF", "#1FA2FF",
    "#F06BC1", "#8C8C8C", "#4CD07D", "#B04FE0", "#5A5A5A",
]

CSS = """
<style>
@import url('https://fonts.googleapis.com/css2?family=Bebas+Neue&family=Inter:wght@400;500;600&display=swap');

html, body, [class*="css"], .stApp {
    font-family: 'Inter', sans-serif;
}
.stApp { background-color: #0b0b0b; color: #F5F5F1; }
.block-container { padding-top: 1.5rem; padding-bottom: 2rem; max-width: 1500px; }
footer, #MainMenu { visibility: hidden; }

/* ---------- header ---------- */
.nf-title {
    font-family: 'Bebas Neue', sans-serif;
    font-size: 4.6rem; line-height: 1; letter-spacing: 3px;
    color: #E50914; text-align: center; margin: 0;
}
.nf-sub {
    font-family: 'Bebas Neue', sans-serif;
    font-size: 1.5rem; letter-spacing: 12px;
    color: #F5F5F1; text-align: center; margin: 0 0 .6rem 0;
}
.pill-row { display: flex; justify-content: center; gap: .5rem; flex-wrap: wrap; margin-bottom: 1rem; }
.pill {
    border: 1px solid #E5495A; color: #F5F5F1; border-radius: 999px;
    padding: .15rem .8rem; font-size: .8rem; background: #160a0c;
}

/* ---------- cards ---------- */
div[data-testid="stVerticalBlockBorderWrapper"] {
    border: 1.5px solid #E5495A !important;
    border-radius: 16px !important;
    background: #0e0e0e;
}
.card-title {
    font-family: 'Bebas Neue', sans-serif;
    font-size: 1.35rem; letter-spacing: 1px; text-align: center;
    color: #F5F5F1; margin-bottom: .1rem;
}
.card-note { color: #9a9a9a; font-size: .78rem; text-align: center; margin-bottom: .2rem; }

.kpi {
    border: 1.5px solid #E5495A; border-radius: 16px; background: #0e0e0e;
    padding: .9rem .5rem; margin-bottom: .8rem; text-align: center;
}
.kpi-val { font-family: 'Bebas Neue', sans-serif; font-size: 2.4rem; line-height: 1.05; color: #F5F5F1; }
.kpi-lbl { font-size: .9rem; color: #cfcfcf; font-weight: 500; }

.tech {
    border: 1.5px solid #E5495A; border-radius: 16px; background: #0e0e0e;
    padding: .8rem 1rem; text-align: center;
}
.tech b { font-family: 'Bebas Neue', sans-serif; font-size: 1.5rem; color: #E50914; margin-right: .5rem; }
.tech span { font-size: 1.05rem; }

/* ---------- widgets ---------- */
label, .stRadio label, .stSlider label, p { color: #F5F5F1; }
div[data-testid="stRadio"] > label p, div[data-testid="stSlider"] > label p {
    font-family: 'Bebas Neue', sans-serif; font-size: 1.15rem; letter-spacing: 1px;
}
button[data-baseweb="tab"] p { font-family: 'Bebas Neue', sans-serif; font-size: 1.2rem; letter-spacing: 1px; }
</style>
"""
st.markdown(CSS, unsafe_allow_html=True)


# ============================================================
# 1-3. CREATE SPARK SESSION
# ============================================================

@st.cache_resource(show_spinner=False)
def get_spark():
    spark = (
        SparkSession.builder
        .appName("Netflix Big Data Analytics")
        .master("local[*]")
        .config("spark.driver.memory", "4g")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.sql.legacy.timeParserPolicy", "LEGACY")
        .config("spark.sql.ansi.enabled", "false")
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    return spark


# ============================================================
# THE PYSPARK PIPELINE (all of your original steps, run once)
# ============================================================

def missing_counts(df):
    """One-row table: number of null / empty values per column."""
    return df.select([
        count(
            when(
                col(c).isNull() | (trim(col(c).cast("string")) == ""),
                c,
            )
        ).alias(c)
        for c in df.columns
    ]).toPandas()


@st.cache_resource(show_spinner="Running the Spark pipeline (first load only)...")
def run_pipeline(netflix_path, imdb_path, prize_path, include_prize):
    spark = get_spark()
    log = {}

    # --------------------------------------------------------
    # 6. LOAD NETFLIX DATASET
    # --------------------------------------------------------
    netflix_raw = (
        spark.read
        .option("header", True)
        .option("inferSchema", True)
        .option("quote", '"')
        .option("escape", '"')
        .option("multiLine", True)
        .csv(netflix_path)
    )

    # --------------------------------------------------------
    # 7. LOAD IMDb DATASET
    # Load IMDb as STRING first. Numeric conversion is done later
    # after cleaning, so malformed values such as "PG" appearing in
    # a numeric column don't crash Spark.
    # --------------------------------------------------------
    imdb_raw = (
        spark.read
        .option("header", True)
        .option("inferSchema", False)
        .option("quote", '"')
        .option("escape", '"')
        .option("multiLine", True)
        .csv(imdb_path)
    )

    # --------------------------------------------------------
    # 9. DATASET SIZES
    # --------------------------------------------------------
    log["netflix_raw_rows"] = netflix_raw.count()
    log["imdb_raw_rows"] = imdb_raw.count()

    # --------------------------------------------------------
    # 12. DUPLICATE CHECKING + REMOVAL
    # --------------------------------------------------------
    log["netflix_duplicates"] = (
        log["netflix_raw_rows"] - netflix_raw.dropDuplicates().count()
    )
    log["imdb_duplicates"] = (
        log["imdb_raw_rows"] - imdb_raw.dropDuplicates().count()
    )

    netflix = netflix_raw.dropDuplicates()
    imdb = imdb_raw.dropDuplicates()

    # --------------------------------------------------------
    # 13-14. MISSING VALUE ANALYSIS
    # --------------------------------------------------------
    log["netflix_missing"] = missing_counts(netflix)
    log["imdb_missing"] = missing_counts(imdb)

    # --------------------------------------------------------
    # 15. CLEAN NETFLIX DATA
    # --------------------------------------------------------
    for c in netflix.columns:
        netflix = netflix.withColumn(c, trim(col(c)))

    netflix = netflix.fillna({
        "director": "Unknown",
        "cast": "Unknown",
        "country": "Unknown",
        "rating": "Unknown",
        "duration": "Unknown",
        "listed_in": "Unknown",
        "description": "Unknown",
        "date_added": "Unknown",
    })

    netflix = netflix.withColumn("release_year", col("release_year").cast("int"))

    netflix = netflix.filter(
        (col("release_year") >= 1900) & (col("release_year") <= 2026)
    )

    # NEW (dashboard only): parse date_added so we can chart
    # "content added over time". Original columns are untouched.
    netflix = netflix.withColumn(
        "date_added_parsed",
        when(
            col("date_added") != "Unknown",
            to_date(col("date_added"), "MMMM d, yyyy"),
        ),
    )

    netflix = netflix.cache()
    log["netflix_clean"] = netflix.count()

    # --------------------------------------------------------
    # 16. CLEAN IMDb DATA
    # --------------------------------------------------------
    for c in imdb.columns:
        imdb = imdb.withColumn(c, trim(col(c)))

    for c in imdb.columns:
        imdb = imdb.withColumn(
            c, when(trim(col(c)) == "", None).otherwise(col(c))
        )

    # 17. CLEAN IMDb RELEASE YEAR  ("2001" -> 2001, "PG" -> NULL)
    imdb = imdb.withColumn(
        "clean_release_year",
        regexp_extract(col("Released_Year"), r"(\d{4})", 1),
    )
    imdb = imdb.withColumn(
        "clean_release_year",
        when(col("clean_release_year") == "", None).otherwise(
            col("clean_release_year").cast("int")
        ),
    )

    # 18-20. NUMERIC COLUMNS
    imdb = imdb.withColumn("IMDB_Rating", col("IMDB_Rating").cast("double"))
    imdb = imdb.withColumn("Meta_score", col("Meta_score").cast("double"))
    imdb = imdb.withColumn("No_of_Votes", col("No_of_Votes").cast("long"))

    # 21. MISSING CATEGORICAL VALUES
    imdb = imdb.fillna({
        "Certificate": "Unknown",
        "Director": "Unknown",
        "Genre": "Unknown",
        "Overview": "Unknown",
        "Star1": "Unknown",
        "Star2": "Unknown",
        "Star3": "Unknown",
        "Star4": "Unknown",
        "Gross": "Unknown",
    })

    # 22-23. MEDIAN META SCORE + FILL
    meta_values = (
        imdb.filter(col("Meta_score").isNotNull())
        .approxQuantile("Meta_score", [0.5], 0.01)
    )
    meta_median = meta_values[0] if len(meta_values) > 0 else 0.0
    log["meta_median"] = meta_median
    imdb = imdb.fillna({"Meta_score": meta_median})

    # 24. VALIDATE IMDb RATINGS
    imdb = imdb.filter((col("IMDB_Rating") >= 0) & (col("IMDB_Rating") <= 10))

    # 25. VALIDATE IMDb RELEASE YEARS
    imdb = imdb.filter(
        col("clean_release_year").isNull()
        | ((col("clean_release_year") >= 1900) & (col("clean_release_year") <= 2026))
    )

    imdb = imdb.cache()
    log["imdb_clean"] = imdb.count()

    # --------------------------------------------------------
    # 26-27. STANDARDIZE TITLES
    # --------------------------------------------------------
    netflix = netflix.withColumn(
        "normalized_title",
        lower(regexp_replace(trim(col("title")), r"[^a-zA-Z0-9 ]", "")),
    )
    imdb = imdb.withColumn(
        "normalized_title",
        lower(regexp_replace(trim(col("Series_Title")), r"[^a-zA-Z0-9 ]", "")),
    )

    # --------------------------------------------------------
    # 28-29. PREPARE FOR JOIN
    # --------------------------------------------------------
    netflix_for_join = netflix.select(
        "show_id", "title", "type", "release_year",
        "country", "listed_in", "normalized_title",
    )
    imdb_for_join = imdb.select(
        "Series_Title", "clean_release_year", "IMDB_Rating", "Meta_score",
        "No_of_Votes", "Genre", "Runtime", "Director", "normalized_title",
    )

    # --------------------------------------------------------
    # 30. JOIN NETFLIX + IMDb
    #   1. normalized title must match
    #   2. release year must match
    # --------------------------------------------------------
    combined = netflix_for_join.join(
        imdb_for_join,
        (netflix_for_join.normalized_title == imdb_for_join.normalized_title)
        & (netflix_for_join.release_year == imdb_for_join.clean_release_year),
        "inner",
    ).drop(imdb_for_join.normalized_title)  # avoid a duplicate column name in the SQL view

    combined = combined.cache()
    log["combined_count"] = combined.count()

    # --------------------------------------------------------
    # 32-41. CLEAN NETFLIX PRIZE DATA
    # --------------------------------------------------------
    log["prize_loaded"] = bool(include_prize)

    if include_prize:
        prize_raw = spark.read.text(prize_path)

        prize = prize_raw.withColumn("line_id", monotonically_increasing_id())

        # 33. identify movie id lines ("1:")
        prize = prize.withColumn(
            "movie_id_temp", regexp_extract(col("value"), r"^(\d+):$", 1)
        )

        # 34. carry movie id downward to the rating rows
        window_spec = (
            Window.orderBy("line_id")
            .rowsBetween(Window.unboundedPreceding, Window.currentRow)
        )
        prize = prize.withColumn(
            "movie_id",
            last(
                when(col("movie_id_temp") != "", col("movie_id_temp")),
                ignorenulls=True,
            ).over(window_spec),
        )

        # 35-37. extract user id, rating, date
        prize = prize.withColumn("user_id", regexp_extract(col("value"), r"^(\d+),", 1))
        prize = prize.withColumn("rating", regexp_extract(col("value"), r"^\d+,(\d),", 1))
        prize = prize.withColumn(
            "rating_date",
            regexp_extract(col("value"), r"^\d+,\d,(\d{4}-\d{2}-\d{2})", 1),
        )

        # 38. keep only actual rating records
        prize = prize.filter((col("user_id") != "") & (col("rating") != ""))

        # 39. convert data types
        prize = prize.withColumn("movie_id", col("movie_id").cast("int"))
        prize = prize.withColumn("user_id", col("user_id").cast("int"))
        prize = prize.withColumn("rating", col("rating").cast("int"))
        prize = prize.withColumn("rating_date", to_date(col("rating_date")))

        # 40. remove duplicate ratings
        prize = prize.dropDuplicates(["movie_id", "user_id", "rating_date"])

        # 41. validate ratings
        prize = prize.filter((col("rating") >= 1) & (col("rating") <= 5))

        # This table is large, so keep it on disk when it doesn't fit in memory
        prize = prize.persist(StorageLevel.MEMORY_AND_DISK)
        prize.createOrReplaceTempView("prize")

    # --------------------------------------------------------
    # 42. CREATE SPARK SQL VIEWS
    # --------------------------------------------------------
    netflix.createOrReplaceTempView("netflix")
    imdb.createOrReplaceTempView("imdb")
    combined.createOrReplaceTempView("combined")

    bounds = spark.sql(
        "SELECT MIN(release_year) AS lo, MAX(release_year) AS hi FROM netflix"
    ).collect()[0]
    log["year_min"] = int(bounds["lo"])
    log["year_max"] = int(bounds["hi"])

    return log


def sql_pd(query):
    """Run a Spark SQL query and return a pandas DataFrame."""
    return get_spark().sql(query).toPandas()


# ============================================================
# SPARK SQL ANALYSES (filtered by the toggle + slider)
# ============================================================

def build_where(ctype, y0, y1, year_col="release_year", use_type=True):
    where = f"WHERE {year_col} BETWEEN {int(y0)} AND {int(y1)}"
    if use_type and ctype in ("Movie", "TV Show"):  # whitelisted values only
        where += f" AND type = '{ctype}'"
    return where


@st.cache_data(show_spinner="Querying Spark SQL...")
def netflix_queries(ctype, y0, y1):
    where = build_where(ctype, y0, y1)
    cte = f"WITH f AS (SELECT * FROM netflix {where}) "

    out = {}

    # KPI cards
    out["kpi"] = sql_pd(cte + """
        SELECT
            COUNT(*) AS total,
            COALESCE(SUM(CASE WHEN type = 'Movie'   THEN 1 ELSE 0 END), 0) AS movies,
            COALESCE(SUM(CASE WHEN type = 'TV Show' THEN 1 ELSE 0 END), 0) AS tv_shows
        FROM f
    """)

    # ANALYSIS 1: Movies vs TV Shows
    out["types"] = sql_pd(cte + """
        SELECT type, COUNT(*) AS total_titles
        FROM f GROUP BY type ORDER BY total_titles DESC
    """)

    # ANALYSIS 4: Top countries
    out["countries"] = sql_pd(cte + """
        SELECT TRIM(c) AS country, COUNT(*) AS total_titles
        FROM f
        LATERAL VIEW explode(split(country, ', ')) t AS c
        WHERE TRIM(c) <> 'Unknown' AND TRIM(c) <> ''
        GROUP BY TRIM(c)
        ORDER BY total_titles DESC
    """)

    out["directors"] = sql_pd(cte + """
        SELECT COUNT(DISTINCT TRIM(d)) AS n
        FROM f
        LATERAL VIEW explode(split(director, ', ')) t AS d
        WHERE TRIM(d) <> 'Unknown' AND TRIM(d) <> ''
    """)

    # Maturity rating distribution
    out["ratings"] = sql_pd(cte + """
        SELECT rating, COUNT(*) AS total_titles
        FROM f GROUP BY rating ORDER BY total_titles DESC
    """)

    # ANALYSIS 3: Genres
    out["genres"] = sql_pd(cte + """
        SELECT TRIM(g) AS genre, COUNT(*) AS total_titles
        FROM f
        LATERAL VIEW explode(split(listed_in, ', ')) t AS g
        WHERE TRIM(g) <> 'Unknown' AND TRIM(g) <> ''
        GROUP BY TRIM(g)
        ORDER BY total_titles DESC
    """)

    # Content added over time
    out["added"] = sql_pd(cte + """
        SELECT YEAR(date_added_parsed) AS year_added, type, COUNT(*) AS total_titles
        FROM f
        WHERE date_added_parsed IS NOT NULL
        GROUP BY YEAR(date_added_parsed), type
        ORDER BY year_added
    """)

    # ANALYSIS 2: Content by release year
    out["by_release"] = sql_pd(cte + """
        SELECT release_year, type, COUNT(*) AS total_titles
        FROM f GROUP BY release_year, type ORDER BY release_year
    """)
    return out


@st.cache_data(show_spinner="Querying Spark SQL...")
def match_queries(ctype, y0, y1):
    where = build_where(ctype, y0, y1)  # `combined` has type + release_year
    out = {}

    out["kpi"] = sql_pd(f"""
        SELECT
            COUNT(*) AS matched,
            ROUND(AVG(IMDB_Rating), 2) AS avg_imdb,
            ROUND(AVG(Meta_score), 1) AS avg_meta
        FROM combined {where}
    """)

    # ANALYSIS 6: Netflix type vs IMDb rating
    out["by_type"] = sql_pd(f"""
        SELECT type, ROUND(AVG(IMDB_Rating), 2) AS average_imdb_rating,
               COUNT(*) AS matched_titles
        FROM combined {where}
        GROUP BY type ORDER BY average_imdb_rating DESC
    """)

    out["dist"] = sql_pd(f"""
        SELECT IMDB_Rating, COUNT(*) AS movie_count
        FROM combined {where}
        GROUP BY IMDB_Rating ORDER BY IMDB_Rating
    """)

    # ANALYSIS 10: Top rated matched titles
    out["top"] = sql_pd(f"""
        SELECT title AS Title, type AS Type, release_year AS Year,
               IMDB_Rating AS `IMDb rating`, No_of_Votes AS Votes, Genre
        FROM combined {where} AND IMDB_Rating IS NOT NULL
        ORDER BY IMDB_Rating DESC, No_of_Votes DESC
        LIMIT 10
    """)
    return out


@st.cache_data(show_spinner="Querying Spark SQL...")
def imdb_queries(y0, y1):
    where = build_where(None, y0, y1, year_col="clean_release_year", use_type=False)
    out = {}

    # ANALYSIS 5: IMDb rating distribution
    out["dist"] = sql_pd(f"""
        SELECT IMDB_Rating, COUNT(*) AS movie_count
        FROM imdb {where}
        GROUP BY IMDB_Rating ORDER BY IMDB_Rating
    """)

    # ANALYSIS 11: Average rating by genre
    out["genre_rating"] = sql_pd(f"""
        SELECT Genre, ROUND(AVG(IMDB_Rating), 2) AS average_rating,
               COUNT(*) AS movie_count
        FROM imdb {where} AND Genre IS NOT NULL
        GROUP BY Genre
        HAVING COUNT(*) >= 5
        ORDER BY average_rating DESC
        LIMIT 10
    """)
    return out


@st.cache_data(show_spinner="Querying Netflix Prize ratings...")
def prize_queries():
    out = {}

    # ANALYSIS 7: rating distribution
    out["dist"] = sql_pd("""
        SELECT rating, COUNT(*) AS total_ratings
        FROM prize GROUP BY rating ORDER BY rating
    """)

    # ANALYSIS 8: ratings over time
    out["over_time"] = sql_pd("""
        SELECT YEAR(rating_date) AS rating_year, COUNT(*) AS total_ratings
        FROM prize WHERE rating_date IS NOT NULL
        GROUP BY YEAR(rating_date) ORDER BY rating_year
    """)

    # ANALYSIS 9: average rating over time
    out["avg_over_time"] = sql_pd("""
        SELECT YEAR(rating_date) AS rating_year, ROUND(AVG(rating), 2) AS average_rating
        FROM prize WHERE rating_date IS NOT NULL
        GROUP BY YEAR(rating_date) ORDER BY rating_year
    """)
    return out


# ============================================================
# UI HELPERS
# ============================================================

def style_fig(fig, height=300, legend=True):
    fig.update_layout(
        height=height,
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        font=dict(color=OFF_WHITE, family="Inter"),
        margin=dict(l=10, r=10, t=10, b=10),
        showlegend=legend,
        legend=dict(font=dict(size=11)),
    )
    fig.update_xaxes(gridcolor="#262626", zeroline=False)
    fig.update_yaxes(gridcolor="#262626", zeroline=False)
    return fig


def show(fig):
    st.plotly_chart(fig, use_container_width=True, config={"displayModeBar": False})


def card(title, note=None):
    """Open a bordered card with a title; use as a context manager."""
    box = st.container(border=True)
    box.markdown(f'<div class="card-title">{title}</div>', unsafe_allow_html=True)
    if note:
        box.markdown(f'<div class="card-note">{note}</div>', unsafe_allow_html=True)
    return box


def kpi(value, label):
    st.markdown(
        f'<div class="kpi"><div class="kpi-val">{value}</div>'
        f'<div class="kpi-lbl">{label}</div></div>',
        unsafe_allow_html=True,
    )


# ============================================================
# SIDEBAR: DATA FILES
# ============================================================

with st.sidebar:
    st.header("Data files")
    netflix_path = st.text_input(
        "Netflix titles (CSV)", os.getenv("NETFLIX_PATH", "data/netflix_titles.csv")
    )
    imdb_path = st.text_input(
        "IMDb Top 1000 (CSV)", os.getenv("IMDB_PATH", "data/imdb_top_1000.csv")
    )
    include_prize = st.checkbox(
        "Include Netflix Prize ratings", value=True,
        help="The Netflix Prize file is very large. Untick this to start faster.",
    )
    prize_path = st.text_input(
        "Netflix Prize ratings (TXT)",
        os.getenv("PRIZE_PATH", "data/combined_data_1.txt"),
        disabled=not include_prize,
    )

required = {"Netflix titles": netflix_path, "IMDb Top 1000": imdb_path}
if include_prize:
    required["Netflix Prize ratings"] = prize_path

missing_files = [f"{name}: `{p}`" for name, p in required.items() if not os.path.exists(p)]
if missing_files:
    st.error(
        "These files were not found. Update the paths in the sidebar:\n\n- "
        + "\n- ".join(missing_files)
    )
    st.stop()

log = run_pipeline(netflix_path, imdb_path, prize_path, include_prize)


# ============================================================
# HEADER + FILTERS
# ============================================================

st.markdown(
    '<p class="nf-title">NETFLIX</p><p class="nf-sub">NETFLIX ANALYTICS DASHBOARD</p>',
    unsafe_allow_html=True,
)

with st.container(border=True):
    f1, f2 = st.columns([1, 2], gap="large")
    with f1:
        ctype = st.radio(
            "Content type", ["All", "Movie", "TV Show"], horizontal=True
        )
    with f2:
        year_range = st.slider(
            "Release year",
            min_value=log["year_min"],
            max_value=log["year_max"],
            value=(log["year_min"], log["year_max"]),
        )
y0, y1 = year_range


# ============================================================
# TABS
# ============================================================

tab_overview, tab_match, tab_ratings = st.tabs(
    ["Overview", "Netflix + IMDb", "User ratings"]
)

# ------------------------------------------------------------
# TAB 1: OVERVIEW
# ------------------------------------------------------------
with tab_overview:
    nq = netflix_queries(ctype, y0, y1)
    k = nq["kpi"].iloc[0]

    if int(k["total"]) == 0:
        st.warning("No titles match these filters. Try widening the release year range.")
    else:
        left, right = st.columns([1, 5], gap="medium")

        with left:
            kpi(f"{int(k['total']):,}", "Total Titles")
            kpi(f"{int(k['movies']):,}", "Movies")
            kpi(f"{int(k['tv_shows']):,}", "TV Shows")
            kpi(f"{len(nq['countries']):,}", "Total Countries")
            kpi(f"{int(nq['directors'].iloc[0]['n']):,}", "Total Directors")

        with right:
            c1, c2, c3 = st.columns([1.25, 1, 1.25], gap="medium")

            # ---- Top countries
            with c1:
                with card("TOP COUNTRIES"):
                    d = nq["countries"].head(8).iloc[::-1]
                    fig = px.bar(
                        d, x="total_titles", y="country", orientation="h",
                        color_discrete_sequence=[SOFT_RED],
                    )
                    fig.update_layout(xaxis_title="Number of titles", yaxis_title=None)
                    show(style_fig(fig, 300, legend=False))

            # ---- Movies vs TV shows
            with c2:
                with card("MOVIES VS TV SHOWS"):
                    fig = px.pie(
                        nq["types"], names="type", values="total_titles",
                        color="type", color_discrete_map=TYPE_COLORS,
                    )
                    fig.update_traces(
                        textinfo="percent", textfont_size=14,
                        marker=dict(line=dict(color="#0e0e0e", width=2)),
                    )
                    fig.update_traces(textfont_color="#0b0b0b", selector=dict(name="TV Show"))
                    show(style_fig(fig, 300))

            # ---- Maturity rating distribution
            with c3:
                with card("RATING DISTRIBUTION"):
                    d = nq["ratings"].head(12)
                    fig = px.bar(
                        d, x="rating", y="total_titles",
                        color_discrete_sequence=[SOFT_RED],
                    )
                    fig.update_layout(xaxis_title=None, yaxis_title="Number of titles")
                    show(style_fig(fig, 300, legend=False))

            c4, c5 = st.columns([1.1, 1.3], gap="medium")

            # ---- Genre donut
            with c4:
                with card("GENRE DISTRIBUTION"):
                    g = nq["genres"]
                    top = g.head(8)
                    other = int(g["total_titles"].iloc[8:].sum())
                    if other > 0:
                        top = pd.concat(
                            [top, pd.DataFrame([{"genre": "Other", "total_titles": other}])],
                            ignore_index=True,
                        )
                    fig = px.pie(
                        top, names="genre", values="total_titles", hole=0.55,
                        color_discrete_sequence=GENRE_COLORS,
                    )
                    fig.update_traces(
                        textinfo="percent",
                        marker=dict(line=dict(color="#0e0e0e", width=2)),
                    )
                    fig.update_layout(legend=dict(orientation="v", x=1.02, y=0.5))
                    show(style_fig(fig, 320))

            # ---- Content added over time
            with c5:
                with card(
                    "CONTENT ADDED OVER TIME",
                    "Year each title was added to Netflix, for titles released in the selected years",
                ):
                    a = nq["added"]
                    if a.empty:
                        st.info("No 'date added' information for these filters.")
                    else:
                        fig = px.line(
                            a, x="year_added", y="total_titles", color="type",
                            color_discrete_map=TYPE_COLORS, markers=True,
                        )
                        fig.update_layout(
                            xaxis_title="Year added", yaxis_title="Titles", legend_title=None
                        )
                        show(style_fig(fig, 300))

            # ---- Content by release year
            with card("CONTENT BY RELEASE YEAR"):
                r = nq["by_release"]
                fig = px.bar(
                    r, x="release_year", y="total_titles", color="type",
                    color_discrete_map=TYPE_COLORS, barmode="stack",
                )
                fig.update_layout(
                    xaxis_title="Release year", yaxis_title="Titles", legend_title=None
                )
                show(style_fig(fig, 260))

# ------------------------------------------------------------
# TAB 2: NETFLIX + IMDb (the merged dataset)
# ------------------------------------------------------------
with tab_match:
    st.caption(
        "Titles are matched on normalized title + release year. "
        "IMDb Top 1000 only contains movies, so TV Shows rarely match."
    )
    mq = match_queries(ctype, y0, y1)
    mk = mq["kpi"].iloc[0]

    if int(mk["matched"]) == 0:
        st.warning("No Netflix titles matched an IMDb Top 1000 movie for these filters.")
    else:
        m1, m2, m3, m4 = st.columns(4)
        with m1:
            kpi(f"{int(mk['matched']):,}", "Matched Titles")
        with m2:
            kpi(f"{mk['avg_imdb']}", "Avg IMDb Rating")
        with m3:
            kpi(f"{mk['avg_meta']}", "Avg Meta Score")
        with m4:
            kpi(f"{log['combined_count']:,}", "Matched (all years)")

        a1, a2 = st.columns(2, gap="medium")
        with a1:
            with card("AVERAGE IMDb RATING BY TYPE"):
                fig = px.bar(
                    mq["by_type"], x="type", y="average_imdb_rating",
                    color="type", color_discrete_map=TYPE_COLORS, text="average_imdb_rating",
                )
                fig.update_layout(xaxis_title=None, yaxis_title="Average IMDb rating")
                show(style_fig(fig, 300, legend=False))
        with a2:
            with card("IMDb RATING OF MATCHED TITLES"):
                fig = px.bar(
                    mq["dist"], x="IMDB_Rating", y="movie_count",
                    color_discrete_sequence=[SOFT_RED],
                )
                fig.update_layout(xaxis_title="IMDb rating", yaxis_title="Titles")
                show(style_fig(fig, 300, legend=False))

        with card("TOP RATED MATCHED TITLES"):
            st.dataframe(mq["top"], use_container_width=True, hide_index=True)

    # IMDb-only views (filtered by release year only)
    iq = imdb_queries(y0, y1)
    st.markdown("#### IMDb Top 1000 in the selected years")
    i1, i2 = st.columns(2, gap="medium")
    with i1:
        with card("IMDb RATING DISTRIBUTION"):
            if iq["dist"].empty:
                st.info("No IMDb movies in this range.")
            else:
                fig = px.bar(
                    iq["dist"], x="IMDB_Rating", y="movie_count",
                    color_discrete_sequence=[SOFT_RED],
                )
                fig.update_layout(xaxis_title="IMDb rating", yaxis_title="Movies")
                show(style_fig(fig, 300, legend=False))
    with i2:
        with card("AVERAGE RATING BY GENRE", "Genre combinations with at least 5 movies"):
            if iq["genre_rating"].empty:
                st.info("Not enough movies per genre in this range. Try widening the years.")
            else:
                st.dataframe(iq["genre_rating"], use_container_width=True, hide_index=True)

# ------------------------------------------------------------
# TAB 3: NETFLIX PRIZE USER RATINGS
# ------------------------------------------------------------
with tab_ratings:
    if not log["prize_loaded"]:
        st.info("Netflix Prize ratings are switched off. Tick the box in the sidebar to load them.")
    else:
        st.caption(
            "The Netflix Prize file only has movie IDs, so these charts cover all users "
            "and don't change with the content type or release year filters."
        )
        pq = prize_queries()
        total_ratings = int(pq["dist"]["total_ratings"].sum())
        avg_all = (
            (pq["dist"]["rating"] * pq["dist"]["total_ratings"]).sum() / max(total_ratings, 1)
        )

        p1, p2, p3 = st.columns(3)
        with p1:
            kpi(f"{total_ratings:,}", "Total Ratings")
        with p2:
            kpi(f"{avg_all:.2f}", "Average Rating")
        with p3:
            kpi(
                f"{int(pq['over_time']['rating_year'].min())} - "
                f"{int(pq['over_time']['rating_year'].max())}",
                "Rating Years",
            )

        r1, r2, r3 = st.columns(3, gap="medium")
        with r1:
            with card("USER RATING DISTRIBUTION"):
                fig = px.bar(
                    pq["dist"], x="rating", y="total_ratings",
                    color_discrete_sequence=[SOFT_RED],
                )
                fig.update_layout(xaxis=dict(tickmode="array", tickvals=[1, 2, 3, 4, 5]),
                                  xaxis_title="Rating", yaxis_title="Ratings")
                show(style_fig(fig, 300, legend=False))
        with r2:
            with card("RATINGS OVER TIME"):
                fig = px.line(
                    pq["over_time"], x="rating_year", y="total_ratings", markers=True,
                    color_discrete_sequence=[SOFT_RED],
                )
                fig.update_layout(xaxis_title="Year", yaxis_title="Ratings")
                show(style_fig(fig, 300, legend=False))
        with r3:
            with card("AVERAGE RATING OVER TIME"):
                fig = px.line(
                    pq["avg_over_time"], x="rating_year", y="average_rating", markers=True,
                    color_discrete_sequence=[OFF_WHITE],
                )
                fig.update_layout(xaxis_title="Year", yaxis_title="Average rating")
                show(style_fig(fig, 300, legend=False))

