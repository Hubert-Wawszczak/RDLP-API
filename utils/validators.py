from pydantic import BaseModel
from pydantic_geojson import MultiPolygonModel
from typing import Optional
from datetime import datetime


class RDLPData(BaseModel):
    id: int
    area_type: str
    a_i_num: int
    silvicult: Optional[str]
    stand_stru: Optional[str]
    sub_area: float
    species_cd: Optional[str]
    spec_age: int
    nazwa: str
    adr_for: str
    site_type: Optional[str]
    forest_fun: Optional[str]
    rotat_age: int
    prot_categ: Optional[str]
    part_cd: Optional[str]
    a_year: int
    geometry: MultiPolygonModel
    load_time: datetime # timestamp datetime.now()
    forest_range_name: str
    rdlp_name: str

