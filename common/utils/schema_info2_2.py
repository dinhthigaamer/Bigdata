from pydantic import BaseModel
from typing import List


class Station(BaseModel):
    station_id: str
    external_id: str
    name: str
    lon: float
    lat: float
    rental_methods: List[str]
    capacity: int
    electric_bike_surcharge_waiver: bool = False
    is_charging: bool = False
    eightd_has_key_dispenser: bool = False
    has_kiosk: bool = False


class Data(BaseModel):
    stations: List[Station]


class StationInformation(BaseModel):
    data: Data
    last_updated: int
    ttl: int
    version: str
