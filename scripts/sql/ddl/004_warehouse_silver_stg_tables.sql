-- silver_stg: entity columns match bronze + lineage for WAP / dedupe (phase 08).
-- No FOREIGN KEY / REFERENCES — parallel silver_stg loads; RI on source (public.*) only.

CREATE TABLE IF NOT EXISTS silver_stg.competition (
    competition_id BIGINT NOT NULL,
    name TEXT NOT NULL,
    country_code CHAR(2),
    _ingested_at TIMESTAMPTZ NOT NULL,
    _batch_id TEXT NOT NULL,
    _source_system TEXT NOT NULL DEFAULT 'postgres_source'
);

CREATE TABLE IF NOT EXISTS silver_stg.venue (
    venue_id BIGINT NOT NULL,
    name TEXT NOT NULL,
    city TEXT,
    capacity INTEGER,
    _ingested_at TIMESTAMPTZ NOT NULL,
    _batch_id TEXT NOT NULL,
    _source_system TEXT NOT NULL DEFAULT 'postgres_source'
);

CREATE TABLE IF NOT EXISTS silver_stg.season (
    season_id BIGINT NOT NULL,
    competition_id BIGINT NOT NULL,
    label TEXT NOT NULL,
    start_date DATE NOT NULL,
    end_date DATE NOT NULL,
    _ingested_at TIMESTAMPTZ NOT NULL,
    _batch_id TEXT NOT NULL,
    _source_system TEXT NOT NULL DEFAULT 'postgres_source'
);

CREATE TABLE IF NOT EXISTS silver_stg.team (
    team_id BIGINT NOT NULL,
    name TEXT NOT NULL,
    short_name TEXT NOT NULL,
    founded_year SMALLINT,
    venue_id BIGINT,
    _ingested_at TIMESTAMPTZ NOT NULL,
    _batch_id TEXT NOT NULL,
    _source_system TEXT NOT NULL DEFAULT 'postgres_source'
);

CREATE TABLE IF NOT EXISTS silver_stg.player (
    player_id BIGINT NOT NULL,
    given_name TEXT NOT NULL,
    family_name TEXT NOT NULL,
    date_of_birth DATE,
    nationality TEXT,
    preferred_foot TEXT,
    _ingested_at TIMESTAMPTZ NOT NULL,
    _batch_id TEXT NOT NULL,
    _source_system TEXT NOT NULL DEFAULT 'postgres_source',
    CONSTRAINT silver_stg_player_preferred_foot_chk CHECK (
        preferred_foot IS NULL OR preferred_foot IN ('left', 'right', 'both')
    )
);

CREATE TABLE IF NOT EXISTS silver_stg.player_contract (
    player_contract_id BIGINT NOT NULL,
    player_id BIGINT NOT NULL,
    team_id BIGINT NOT NULL,
    valid_from DATE NOT NULL,
    valid_to DATE,
    shirt_number SMALLINT,
    is_loan BOOLEAN NOT NULL DEFAULT FALSE,
    _ingested_at TIMESTAMPTZ NOT NULL,
    _batch_id TEXT NOT NULL,
    _source_system TEXT NOT NULL DEFAULT 'postgres_source'
);

CREATE TABLE IF NOT EXISTS silver_stg.matchday_round (
    round_id BIGINT NOT NULL,
    season_id BIGINT NOT NULL,
    round_number SMALLINT NOT NULL,
    label TEXT NOT NULL,
    _ingested_at TIMESTAMPTZ NOT NULL,
    _batch_id TEXT NOT NULL,
    _source_system TEXT NOT NULL DEFAULT 'postgres_source'
);

CREATE TABLE IF NOT EXISTS silver_stg.fixture (
    fixture_id BIGINT NOT NULL,
    season_id BIGINT NOT NULL,
    round_id BIGINT NOT NULL,
    home_team_id BIGINT NOT NULL,
    away_team_id BIGINT NOT NULL,
    venue_id BIGINT NOT NULL,
    kickoff_utc TIMESTAMPTZ NOT NULL,
    status TEXT NOT NULL,
    _ingested_at TIMESTAMPTZ NOT NULL,
    _batch_id TEXT NOT NULL,
    _source_system TEXT NOT NULL DEFAULT 'postgres_source',
    CONSTRAINT silver_stg_fixture_status_chk CHECK (
        status IN ('scheduled', 'live', 'finished', 'postponed', 'cancelled')
    )
);

CREATE TABLE IF NOT EXISTS silver_stg.match_result (
    fixture_id BIGINT NOT NULL,
    home_goals SMALLINT NOT NULL,
    away_goals SMALLINT NOT NULL,
    home_xg NUMERIC(5, 3),
    away_xg NUMERIC(5, 3),
    _ingested_at TIMESTAMPTZ NOT NULL,
    _batch_id TEXT NOT NULL,
    _source_system TEXT NOT NULL DEFAULT 'postgres_source'
);
