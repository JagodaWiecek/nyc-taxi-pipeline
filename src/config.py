from dataclasses import dataclass

"""
The configuration class for all impotant parameters within the project. 
The variables inside cannot be changed.
"""
@dataclass(frozen=True)
class Config:
    catalog: str = "workspace"
    bronze_schema: str = "bronze"
    silver_schema: str = "silver"
    gold_schema: str = "gold"
    raw_volume: str = "raw"

    base_url: str = "https://d37ci6vzurychx.cloudfront.net/trip-data"
    dataset: str = "yellow_tripdata"
    start_month: str = "2024-01"
    end_month: str = "2024-12"

    zone_lookup_url: str = "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv"

    max_trip_distance_miles: float = 100.0

    @property
    def volume_path(self) -> str:
        return f"/Volumes/{self.catalog}/{self.bronze_schema}/{self.raw_volume}"

    @property
    def bronze_table(self) -> str:
        return f"{self.catalog}.{self.bronze_schema}.yellow_trips"

    @property
    def silver_table(self) -> str:
        return f"{self.catalog}.{self.silver_schema}.yellow_trips"

    def source_url(self, year_month: str) -> str:
        return f"{self.base_url}/{self.dataset}_{year_month}.parquet"

    def raw_file_path(self, year_month: str) -> str:
        return f"{self.volume_path}/{self.dataset}_{year_month}.parquet"
    
    @property
    def zone_lookup_path(self) -> str:
        return f"{self.volume_path}/taxi_zone_lookup.csv"

    @property
    def zone_lookup_table(self) -> str:
        return f"{self.catalog}.{self.bronze_schema}.taxi_zones"
    
    @property
    def silver_zone_table(self) -> str:
        return f"{self.catalog}.{self.silver_schema}.taxi_zones"

    def gold_table(self, name: str) -> str:
        return f"{self.catalog}.{self.gold_schema}.{name}"

@dataclass(frozen=True)
class ChartConfig:
    """Look and output location of the charts built by analysis/charts.py."""

    images_dir: str = "docs/images"
    dpi: int = 150
    series_color: str = "#257de0"
    text_color: str = "#191919"
    muted_color: str = "#666666"
    grid_color: str = "#e5e5e5"
    secondary_color: str = "#f47230"

    def save_path(self, name: str) -> str:
        return f"{self.images_dir}/{name}.png"