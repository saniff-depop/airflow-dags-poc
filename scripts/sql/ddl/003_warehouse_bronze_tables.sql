-- Bronze layer: mirror public.* table names and column types (1:1 for Soda row-count checks).
-- No FOREIGN KEY / REFERENCES on warehouse bronze — RI on source (public.*) only; enables
-- parallel loads. Legacy FKs are stripped by ensure_warehouse_schemas.sql / 007_drop_warehouse_foreign_keys.sql.

CREATE TABLE IF NOT EXISTS bronze.competition (
    competition_id BIGINT PRIMARY KEY,
    name TEXT NOT NULL,
    country_code CHAR(2)
);

CREATE TABLE IF NOT EXISTS bronze.venue (
    venue_id BIGINT PRIMARY KEY,
    name TEXT NOT NULL,
    city TEXT,
    capacity INTEGER
);

CREATE TABLE IF NOT EXISTS bronze.season (
    season_id BIGINT PRIMARY KEY,
    competition_id BIGINT NOT NULL,
    label TEXT NOT NULL,
    start_date DATE NOT NULL,
    end_date DATE NOT NULL
);

CREATE TABLE IF NOT EXISTS bronze.team (
    team_id BIGINT PRIMARY KEY,
    name TEXT NOT NULL,
    short_name TEXT NOT NULL,
    founded_year SMALLINT,
    venue_id BIGINT
);

CREATE TABLE IF NOT EXISTS bronze.player (
    player_id BIGINT PRIMARY KEY,
    given_name TEXT NOT NULL,
    family_name TEXT NOT NULL,
    date_of_birth DATE,
    nationality TEXT,
    preferred_foot TEXT,
    CONSTRAINT bronze_player_preferred_foot_chk CHECK (
        preferred_foot IS NULL OR preferred_foot IN ('left', 'right', 'both')
    )
);

CREATE TABLE IF NOT EXISTS bronze.player_contract (
    player_contract_id BIGINT PRIMARY KEY,
    player_id BIGINT NOT NULL,
    team_id BIGINT NOT NULL,
    valid_from DATE NOT NULL,
    valid_to DATE,
    shirt_number SMALLINT,
    is_loan BOOLEAN NOT NULL DEFAULT FALSE
);

CREATE TABLE IF NOT EXISTS bronze.matchday_round (
    round_id BIGINT PRIMARY KEY,
    season_id BIGINT NOT NULL,
    round_number SMALLINT NOT NULL,
    label TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS bronze.fixture (
    fixture_id BIGINT PRIMARY KEY,
    season_id BIGINT NOT NULL,
    round_id BIGINT NOT NULL,
    home_team_id BIGINT NOT NULL,
    away_team_id BIGINT NOT NULL,
    venue_id BIGINT NOT NULL,
    kickoff_utc TIMESTAMPTZ NOT NULL,
    status TEXT NOT NULL,
    CONSTRAINT bronze_fixture_status_chk CHECK (
        status IN ('scheduled', 'live', 'finished', 'postponed', 'cancelled')
    )
);

CREATE TABLE IF NOT EXISTS bronze.match_result (
    fixture_id BIGINT PRIMARY KEY,
    home_goals SMALLINT NOT NULL,
    away_goals SMALLINT NOT NULL,
    home_xg NUMERIC(5, 3),
    away_xg NUMERIC(5, 3)
);
