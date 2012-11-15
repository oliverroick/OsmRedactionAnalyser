CREATE TABLE osm_redaction_before
(
  cell_id bigint NOT NULL,
  the_geom geometry,
  length_major_highways double precision,
  length_minor_highways double precision,
  length_waterways double precision,
  length_railways double precision,
  length_boundary double precision,
  area_amenity double precision,
  area_buildings double precision,
  area_landuse double precision,
  area_leisure double precision,
  area_natural double precision,
  CONSTRAINT pk_osm_red_before PRIMARY KEY (cell_id)
)
WITH (
  OIDS=FALSE
);