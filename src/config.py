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