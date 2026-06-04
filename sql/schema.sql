-- ==========================================================================
-- PostgreSQL schema for geocoder — replaces Elasticsearch
--
-- Required extensions: PostGIS, pg_trgm, unaccent
-- ==========================================================================

CREATE EXTENSION IF NOT EXISTS postgis;
CREATE EXTENSION IF NOT EXISTS pg_trgm;
CREATE EXTENSION IF NOT EXISTS unaccent;

-- ── Street-type synonym table (EN / AR / FR) ────────────────────────────
-- Replaces the ES synonym token filters.  The geocoder expands abbreviations
-- at query time using the expand_synonyms() function.

CREATE TABLE IF NOT EXISTS street_synonyms (
    abbrev   TEXT PRIMARY KEY,
    expanded TEXT NOT NULL,
    lang     TEXT NOT NULL DEFAULT 'en'   -- 'en', 'ar', 'fr'
);

INSERT INTO street_synonyms (abbrev, expanded, lang) VALUES
    -- English
    ('st',   'street',    'en'),
    ('rd',   'road',      'en'),
    ('ave',  'avenue',    'en'),
    ('av',   'avenue',    'en'),
    ('blvd', 'boulevard', 'en'),
    ('bvd',  'boulevard', 'en'),
    ('ln',   'lane',      'en'),
    ('dr',   'drive',     'en'),
    ('pl',   'place',     'en'),
    ('ct',   'court',     'en'),
    ('sq',   'square',    'en'),
    ('hwy',  'highway',   'en'),
    ('cres', 'crescent',  'en'),
    ('terr', 'terrace',   'en'),
    ('pkwy', 'parkway',   'en'),
    -- Arabic
    ('ش',   'شارع',  'ar'),
    ('ط',   'طريق',  'ar'),
    ('م',   'ميدان', 'ar'),
    -- French
    ('r',    'rue',       'fr'),
    ('bd',   'boulevard', 'fr'),
    ('ch',   'chemin',    'fr'),
    ('imp',  'impasse',   'fr'),
    ('all',  'allée',     'fr'),
    ('crs',  'cours',     'fr'),
    ('rte',  'route',     'fr'),
    ('pass', 'passage',   'fr')
ON CONFLICT (abbrev) DO NOTHING;


-- ── Arabic normalisation function ────────────────────────────────────────
-- Replaces the ES arabic_normalization filter + arabic_normalize_char filter.
-- Strips tatweel, normalises hamza variants, removes diacritics.

CREATE OR REPLACE FUNCTION arabic_normalize(t TEXT) RETURNS TEXT
LANGUAGE sql IMMUTABLE STRICT PARALLEL SAFE AS $$
    SELECT regexp_replace(
        regexp_replace(
            regexp_replace(
                regexp_replace(t,
                    E'[\u0640]', '', 'g'),           -- strip tatweel (kashida)
                E'[\u064B-\u065F\u0670]', '', 'g'),  -- strip diacritics (tashkeel)
            E'[\u0622\u0623\u0625]', E'\u0627', 'g'),-- normalise hamza-on-alef → bare alef
        E'[\u0629]', E'\u0647', 'g')                  -- taa marbuta → haa
$$;


-- ── Synonym-expansion function ───────────────────────────────────────────
-- Expands each token in a query string using the street_synonyms table.
-- E.g.  "9 pentland cres"  →  "9 pentland crescent"
-- Also adds the original abbreviation so both forms match.

CREATE OR REPLACE FUNCTION expand_synonyms(q TEXT) RETURNS TEXT
LANGUAGE sql STABLE PARALLEL SAFE AS $$
    SELECT string_agg(
        CASE
            WHEN s.expanded IS NOT NULL
            THEN s.expanded          -- replace abbrev with expanded form
            ELSE w.token
        END,
        ' ' ORDER BY w.ord
    )
    FROM unnest(string_to_array(lower(q), ' '))
         WITH ORDINALITY AS w(token, ord)
    LEFT JOIN street_synonyms s ON s.abbrev = w.token
$$;


-- ── Main places table ────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS osm_places (
    osm_id           TEXT PRIMARY KEY,
    osm_type         TEXT NOT NULL DEFAULT '',

    -- Names (multilingual)
    name             TEXT NOT NULL DEFAULT '',
    name_en          TEXT NOT NULL DEFAULT '',
    name_fr          TEXT NOT NULL DEFAULT '',

    -- All tag values concatenated — replaces ES tags_text field.
    -- Built by the inserter using shared/embeddings.build_text().
    tags_text        TEXT NOT NULL DEFAULT '',

    -- Raw tags stored as JSONB for retrieval / enrichment
    tags             JSONB NOT NULL DEFAULT '{}',

    -- Geometry (full shape for geo_shape queries + centroid for proximity)
    geom             geometry(Geometry, 4326),
    centroid         geometry(Point, 4326),

    -- Classification / ranking
    admin_level      INT NOT NULL DEFAULT 0,
    area_km2         FLOAT NOT NULL DEFAULT 0,
    offline_rank     FLOAT NOT NULL DEFAULT 0,
    popularity       FLOAT NOT NULL DEFAULT 0,

    -- Structured address fields
    addr_housenumber TEXT NOT NULL DEFAULT '',
    addr_street      TEXT NOT NULL DEFAULT '',
    addr_city        TEXT NOT NULL DEFAULT '',
    addr_postcode    TEXT NOT NULL DEFAULT '',
    addr_country     TEXT NOT NULL DEFAULT '',
    addr_suburb      TEXT NOT NULL DEFAULT '',
    addr_state       TEXT NOT NULL DEFAULT '',
    full_address     TEXT NOT NULL DEFAULT '',
    has_address      BOOLEAN NOT NULL DEFAULT FALSE,

    -- Full-text search vector (auto-maintained via trigger)
    -- Weights: A = names, B = street/address, C = tags_text
    search_vector    tsvector,

    -- AI-generated description cache (same as ES ai_description)
    ai_description   JSONB,

    -- Reverse-geocoded address enrichment cache
    address          JSONB
);


-- ── Search-vector trigger ────────────────────────────────────────────────
-- Mirrors the ES analysers: combines name, street, address, and tags_text
-- into a single weighted tsvector.  Uses 'simple' config so proper nouns
-- are not stemmed away.  Arabic normalisation applied before indexing.

CREATE OR REPLACE FUNCTION osm_places_search_trigger() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    NEW.search_vector :=
        setweight(to_tsvector('simple',
            arabic_normalize(coalesce(NEW.name, ''))      || ' ' ||
            arabic_normalize(coalesce(NEW.name_en, ''))   || ' ' ||
            arabic_normalize(coalesce(NEW.name_fr, ''))
        ), 'A')
        ||
        setweight(to_tsvector('simple',
            arabic_normalize(coalesce(NEW.addr_street, ''))  || ' ' ||
            arabic_normalize(coalesce(NEW.full_address, '')) || ' ' ||
            arabic_normalize(coalesce(NEW.addr_city, ''))    || ' ' ||
            arabic_normalize(coalesce(NEW.addr_suburb, ''))
        ), 'B')
        ||
        setweight(to_tsvector('simple',
            arabic_normalize(coalesce(NEW.tags_text, ''))
        ), 'C');
    RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS trig_osm_places_search ON osm_places;
CREATE TRIGGER trig_osm_places_search
    BEFORE INSERT OR UPDATE ON osm_places
    FOR EACH ROW EXECUTE FUNCTION osm_places_search_trigger();


-- ── Indexes ──────────────────────────────────────────────────────────────

-- Trigram indexes — fast fuzzy & prefix matching (replaces ES fuzziness + edge_ngram)
CREATE INDEX IF NOT EXISTS idx_places_name_trgm
    ON osm_places USING GIN (name gin_trgm_ops);
CREATE INDEX IF NOT EXISTS idx_places_name_en_trgm
    ON osm_places USING GIN (name_en gin_trgm_ops);
CREATE INDEX IF NOT EXISTS idx_places_name_fr_trgm
    ON osm_places USING GIN (name_fr gin_trgm_ops);
CREATE INDEX IF NOT EXISTS idx_places_street_trgm
    ON osm_places USING GIN (addr_street gin_trgm_ops);
CREATE INDEX IF NOT EXISTS idx_places_fulladdr_trgm
    ON osm_places USING GIN (full_address gin_trgm_ops);
CREATE INDEX IF NOT EXISTS idx_places_city_trgm
    ON osm_places USING GIN (addr_city gin_trgm_ops);
CREATE INDEX IF NOT EXISTS idx_places_tags_trgm
    ON osm_places USING GIN (tags_text gin_trgm_ops);

-- Full-text search (replaces ES match queries on name / tags_text)
CREATE INDEX IF NOT EXISTS idx_places_search_vec
    ON osm_places USING GIN (search_vector);

-- Spatial indexes (replaces ES geo_shape + geo_point)
CREATE INDEX IF NOT EXISTS idx_places_geom
    ON osm_places USING GIST (geom);
CREATE INDEX IF NOT EXISTS idx_places_centroid
    ON osm_places USING GIST (centroid);

-- Exact-match B-tree indexes (replaces ES keyword fields)
CREATE INDEX IF NOT EXISTS idx_places_postcode
    ON osm_places (addr_postcode) WHERE addr_postcode != '';
CREATE INDEX IF NOT EXISTS idx_places_country
    ON osm_places (addr_country) WHERE addr_country != '';
CREATE INDEX IF NOT EXISTS idx_places_housenumber
    ON osm_places (addr_housenumber) WHERE addr_housenumber != '';
CREATE INDEX IF NOT EXISTS idx_places_osm_type
    ON osm_places (osm_type);
CREATE INDEX IF NOT EXISTS idx_places_admin
    ON osm_places (admin_level) WHERE admin_level > 0;

-- Ranking index — for ORDER BY offline_rank DESC fast paths
CREATE INDEX IF NOT EXISTS idx_places_rank
    ON osm_places (offline_rank DESC);

-- Lower-cased name index for exact prefix (LIKE 'x%')
CREATE INDEX IF NOT EXISTS idx_places_name_lower
    ON osm_places (lower(name) text_pattern_ops);
CREATE INDEX IF NOT EXISTS idx_places_name_en_lower
    ON osm_places (lower(name_en) text_pattern_ops) WHERE name_en != '';
CREATE INDEX IF NOT EXISTS idx_places_name_fr_lower
    ON osm_places (lower(name_fr) text_pattern_ops) WHERE name_fr != '';
