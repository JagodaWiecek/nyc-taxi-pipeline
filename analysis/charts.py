"""Build the charts in docs/images from the rpt_ tables in Databricks."""

import os
from pathlib import Path
import matplotlib.pyplot as plt
from databricks import sql
import calendar

from src.config import ChartConfig, Config


CONFIG = Config()
CHARTS = ChartConfig()


def fetch(cursor, query: str) -> list[dict]:
    """Run a query and return rows as dictionaries (column name -> value)."""
    cursor.execute(query)
    columns = [column[0] for column in cursor.description]
    return [dict(zip(columns, row)) for row in cursor.fetchall()]


def style_axes(ax, title: str, ylabel: str, xlabel: str) -> None:
    """Common look: title on the left, light grid, no top and right frame."""
    ax.set_title(title, loc="left", color=CHARTS.text_color, fontsize=12)
    ax.set_ylabel(ylabel, color=CHARTS.muted_color)
    ax.set_xlabel(xlabel, color=CHARTS.muted_color)
    ax.grid(axis="y", color=CHARTS.grid_color, linewidth=0.8)
    ax.set_axisbelow(True)
    for side in ("top", "right"):
        ax.spines[side].set_visible(False)
    for side in ("left", "bottom"):
        ax.spines[side].set_color(CHARTS.grid_color)
    ax.tick_params(colors=CHARTS.muted_color)


def save(fig, name: str) -> None:
    """Save the figure as a PNG file and close it."""
    path = Path(CHARTS.save_path(name))
    path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(path, dpi=CHARTS.dpi, bbox_inches="tight", facecolor="white")
    plt.close(fig)
    print(f"Saved {path}")


def chart_trips_by_hour(cursor) -> None:
    rows = fetch(cursor, f"SELECT pickup_hour, trips FROM {CONFIG.gold_table('rpt_demand_by_hour')} ORDER BY pickup_hour")
    fig, ax = plt.subplots(figsize=(8, 4))
    ax.bar(
        [row["pickup_hour"] for row in rows],
        [row["trips"] / 1_000_000 for row in rows],
        color=CHARTS.series_color,
        width=0.7,
    )
    ax.set_xticks(range(0, 24, 2))
    style_axes(ax, "Trips by pickup hour, 2024", "Trips (millions)", "Hour of day")
    save(fig, "q1_trips_by_hour")


def chart_speed_by_hour(cursor) -> None:
    rows = fetch(cursor, f"SELECT pickup_hour, avg_speed_kmh FROM {CONFIG.gold_table('rpt_efficiency_by_hour')} ORDER BY pickup_hour")
    fig, ax = plt.subplots(figsize=(8, 4))
    ax.plot(
        [row["pickup_hour"] for row in rows],
        [row["avg_speed_kmh"] for row in rows],
        color=CHARTS.series_color,
        linewidth=2,
    )
    ax.set_xticks(range(0, 24, 2))
    ax.set_ylim(bottom=0)
    style_axes(ax, "Average taxi speed by pickup hour, 2024", "Speed (km/h)", "Hour of day")
    save(fig, "q4_speed_by_hour")


def main() -> None:
    with sql.connect(
        server_hostname=os.environ["DATABRICKS_HOST"],
        http_path=os.environ["DATABRICKS_HTTP_PATH"],
        access_token=os.environ["DATABRICKS_TOKEN"],
    ) as connection, connection.cursor() as cursor:
        chart_trips_by_hour(cursor)
        chart_speed_by_hour(cursor)
        chart_trips_by_weekday(cursor)
        chart_trips_by_month(cursor)
        chart_top_pickup_zones(cursor)
        chart_tip_pct_by_distance(cursor)
        chart_revenue_by_payment_type(cursor)
        chart_fare_per_km_by_hour(cursor)

def chart_trips_by_weekday(cursor) -> None:
    rows = fetch(
        cursor,
        f"SELECT weekday_name, avg_trips_per_day FROM {CONFIG.gold_table('rpt_demand_by_weekday')} ORDER BY weekday_number",
    )
    fig, ax = plt.subplots(figsize=(8, 4))
    ax.bar(
        [row["weekday_name"][:3] for row in rows],
        [row["avg_trips_per_day"] / 1000 for row in rows],
        color=CHARTS.series_color,
        width=0.6,
    )
    style_axes(ax, "Average trips per day by weekday, 2024", "Trips per day (thousands)", "")
    save(fig, "q1_trips_by_weekday")


def chart_trips_by_month(cursor) -> None:
    rows = fetch(
        cursor,
        f"SELECT pickup_month, avg_trips_per_day FROM {CONFIG.gold_table('rpt_demand_by_month')} ORDER BY pickup_month",
    )
    fig, ax = plt.subplots(figsize=(8, 4))
    ax.bar(
        [calendar.month_abbr[int(row["pickup_month"][5:])] for row in rows],
        [row["avg_trips_per_day"] / 1000 for row in rows],
        color=CHARTS.series_color,
        width=0.6,
    )
    style_axes(ax, "Average trips per day by month, 2024", "Trips per day (thousands)", "")
    save(fig, "q1_trips_by_month")


def chart_top_pickup_zones(cursor) -> None:
    rows = fetch(
        cursor,
        f"SELECT zone, borough, trips FROM {CONFIG.gold_table('rpt_pickup_zones')} ORDER BY trips DESC LIMIT 10",
    )
    rows.reverse()
    fig, ax = plt.subplots(figsize=(8, 5))
    ax.barh(
        [f"{row['zone']} ({row['borough']})" for row in rows],
        [row["trips"] / 1_000_000 for row in rows],
        color=CHARTS.series_color,
        height=0.6,
    )
    style_axes(ax, "Top 10 pickup zones by trips, 2024", "", "Trips (millions)")
    ax.grid(axis="y", visible=False)
    ax.grid(axis="x", color=CHARTS.grid_color, linewidth=0.8)
    save(fig, "q2_top_pickup_zones")


def chart_tip_pct_by_distance(cursor) -> None:
    rows = fetch(
        cursor,
        f"SELECT distance_bucket, tip_pct_of_fare FROM {CONFIG.gold_table('rpt_tip_pct_by_distance')} ORDER BY distance_bucket",
    )
    fig, ax = plt.subplots(figsize=(8, 4))
    ax.bar(
        [row["distance_bucket"].split(": ", 1)[1] for row in rows],
        [row["tip_pct_of_fare"] for row in rows],
        color=CHARTS.series_color,
        width=0.6,
    )
    style_axes(
        ax,
        "Card tips as % of fare by trip distance, 2024",
        "Tip (% of fare)",
        "Trip distance (1 mi = 1.6 km)",
    )
    save(fig, "q3_tip_pct_by_distance")


def chart_revenue_by_payment_type(cursor) -> None:
    rows = fetch(
        cursor,
        f"SELECT payment_type_name, fare_per_trip, revenue_per_trip FROM {CONFIG.gold_table('rpt_revenue_by_payment_type')} ORDER BY payment_type",
    )
    positions = range(len(rows))
    width = 0.38
    fig, ax = plt.subplots(figsize=(8, 4))
    ax.bar(
        [p - width / 2 for p in positions],
        [row["fare_per_trip"] for row in rows],
        width=width,
        color=CHARTS.series_color,
        label="Metered fare",
    )
    ax.bar(
        [p + width / 2 for p in positions],
        [row["revenue_per_trip"] for row in rows],
        width=width,
        color=CHARTS.secondary_color,
        label="Total charged",
    )
    ax.set_xticks(list(positions), [row["payment_type_name"] for row in rows])
    style_axes(ax, "Fare and total amount per trip by payment type, 2024", "USD per trip", "")
    ax.legend(frameon=False, labelcolor=CHARTS.text_color)
    save(fig, "q3_revenue_by_payment_type")


def chart_fare_per_km_by_hour(cursor) -> None:
    rows = fetch(
        cursor,
        f"SELECT pickup_hour, fare_per_km FROM {CONFIG.gold_table('rpt_efficiency_by_hour')} ORDER BY pickup_hour",
    )
    fig, ax = plt.subplots(figsize=(8, 4))
    ax.plot(
        [row["pickup_hour"] for row in rows],
        [row["fare_per_km"] for row in rows],
        color=CHARTS.series_color,
        linewidth=2,
    )
    ax.set_xticks(range(0, 24, 2))
    ax.set_ylim(bottom=0)
    style_axes(ax, "Metered fare per km by pickup hour, 2024", "USD per km", "Hour of day")
    save(fig, "q4_fare_per_km_by_hour")


if __name__ == "__main__":
    main()