from pydantic import BaseModel
from typing import List


# Station_status v 2.2.json
class VehicleType(BaseModel):
    vehicle_type_id: str
    count: int


class Station(BaseModel):
    station_id: str
    num_bikes_available: int
    num_bikes_disabled: int
    num_docks_available: int
    num_docks_disabled: int
    is_installed: int
    is_renting: int
    is_returning: int
    last_reported: int
    vehicle_types_available: List[VehicleType] = []
    num_ebikes_available: int = 0
    eightd_has_available_keys: bool = False
    is_charging: bool = False


class Data(BaseModel):
    stations: List[Station]


class StationStatus(BaseModel):
    data: Data
    last_updated: int
    ttl: int
    version: str
