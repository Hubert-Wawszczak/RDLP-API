from pydantic import BaseModel
from typing import Optional, Any


class RDLPData(BaseModel):
    """
    Data model for RDLP wydzielenia, aligned with WydzielenieResponse schema from backend.
    """
    id: int
    area_type: Optional[str] = None
    a_i_num: Optional[int] = None
    silvicult: Optional[str] = None
    stand_stru: Optional[str] = None
    sub_area: Optional[float] = None
    species_cd: Optional[str] = None
    spec_age: Optional[int] = None
    nazwa: Optional[str] = None
    adr_for: str
    site_type: Optional[str] = None
    forest_fun: Optional[str] = None
    rotat_age: Optional[int] = None
    prot_categ: Optional[str] = None
    part_cd: Optional[str] = None
    a_year: Optional[int] = None
    geometry: Optional[Any] = None  # GeoJSON format (MultiPolygon)
    forest_range_name: str
    rdlp_name: str

