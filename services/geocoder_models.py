"""Pydantic request/response models for the geocoding service.

Extracted from ``geocoder.py`` to keep the route module focused on handlers.
These models depend only on pydantic, so they import in isolation.
"""
from pydantic import BaseModel, Field


class PlaceCreate(BaseModel):
    """Model for creating a new place."""
    name: str = Field(..., min_length=1, max_length=255)
    name_en: str | None = Field(None, max_length=255)
    name_fr: str | None = Field(None, max_length=255)
    lat: float = Field(..., ge=-90, le=90)
    lon: float = Field(..., ge=-180, le=180)
    tags: dict[str, str] | None = Field(default_factory=dict)
    osm_type: str = Field(default="node", description="OSM type: node, way, or relation")
    admin_level: int = Field(default=0, ge=0, le=10)
    # Optional structured address fields
    addr_housenumber: str | None = Field(None, max_length=50,  description="House/building number")
    addr_street:      str | None = Field(None, max_length=255, description="Street name")
    addr_city:        str | None = Field(None, max_length=255, description="City or town")
    addr_postcode:    str | None = Field(None, max_length=20,  description="Postal / ZIP code")
    addr_country:     str | None = Field(None, max_length=10,  description="ISO 3166-1 country code")
    addr_suburb:      str | None = Field(None, max_length=255, description="Suburb / neighbourhood")
    addr_state:       str | None = Field(None, max_length=255, description="State / governorate")


class InsertMessage(BaseModel):
    """Model for insert endpoint - matches watcher.py message format exactly."""
    osm_id: str = Field(..., description="OSM ID (e.g., 'n123', 'w456', 'r789')")
    osm_type: str = Field(..., description="OSM type: node, way, or relation")
    tags: dict[str, str] = Field(..., description="OSM tags")
    geom: dict = Field(..., description="GeoJSON geometry")
    admin_level: int = Field(default=0, ge=0, le=10, description="Administrative level")
    area_km2: float = Field(default=0.0, ge=0, description="Area in square kilometers")


class PlaceResponse(BaseModel):
    """Model for place response."""
    osm_id: str
    osm_type: str
    name: str
    name_en: str | None
    name_fr: str | None
    tags: dict[str, str]
    lat: float
    lon: float
    admin_level: int
    created_at: str


class ProbePing(BaseModel):
    """A single GPS observation from a moving device."""
    lat: float = Field(..., ge=-90, le=90)
    lon: float = Field(..., ge=-180, le=180)
    speed: float | None = Field(None, ge=0, description="Instantaneous speed in m/s, if known")
    heading: float | None = Field(None, ge=0, le=360, description="Bearing in degrees, if known")
    ts: float | None = Field(None, description="Unix epoch seconds for this fix")


class ProbeBatch(BaseModel):
    """An ordered GPS trace from one device/session (best for map-matching)."""
    device_id: str = Field(..., min_length=1, max_length=128, description="Opaque device/session id")
    points: list[ProbePing] = Field(..., min_length=1, max_length=500)
