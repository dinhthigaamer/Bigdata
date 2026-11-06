from pydantic import BaseModel
from typing import List


class Station(BaseModel):
    station_id: str
    name: str
    short_name: str
    lon: float
    lat: float
    region_id: str
    capacity: int
    rental_uris: dict
    is_charging: bool = False


class Data(BaseModel):
    stations: List[Station]


class StationInformation(BaseModel):
    data: Data
    last_updated: int
    ttl: int
    version: str
