-- Source OLTP schema (postgres_source, database from POSTGRES_SOURCE_DB).
-- Idempotent: CREATE TABLE IF NOT EXISTS. Apply before warehouse DDL.
-- v1 scope: through match_result (no fixture_event, standing_snapshot).

CREATE TABLE IF NOT EXISTS public.competition (
    competition_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    name TEXT NOT NULL,
    country_code CHAR(2)
);

CREATE TABLE IF NOT EXISTS public.venue (
    venue_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    name TEXT NOT NULL,
    city TEXT,
    capacity INTEGER
);

CREATE TABLE IF NOT EXISTS public.season (
    season_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    competition_id BIGINT NOT NULL REFERENCES public.competition (competition_id),
    label TEXT NOT NULL,
    start_date DATE NOT NULL,
    end_date DATE NOT NULL
);

CREATE TABLE IF NOT EXISTS public.team (
    team_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    name TEXT NOT NULL,
    short_name TEXT NOT NULL,
    founded_year SMALLINT,
    venue_id BIGINT REFERENCES public.venue (venue_id)
);

CREATE TABLE IF NOT EXISTS public.player (
    player_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    given_name TEXT NOT NULL,
    family_name TEXT NOT NULL,
    date_of_birth DATE,
    nationality TEXT,
    preferred_foot TEXT,
    CONSTRAINT player_preferred_foot_chk CHECK (
        preferred_foot IS NULL OR preferred_foot IN ('left', 'right', 'both')
    )
);

CREATE TABLE IF NOT EXISTS public.player_contract (
    player_contract_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    player_id BIGINT NOT NULL REFERENCES public.player (player_id),
    team_id BIGINT NOT NULL REFERENCES public.team (team_id),
    valid_from DATE NOT NULL,
    valid_to DATE,
    shirt_number SMALLINT,
    is_loan BOOLEAN NOT NULL DEFAULT FALSE
);

CREATE TABLE IF NOT EXISTS public.matchday_round (
    round_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    season_id BIGINT NOT NULL REFERENCES public.season (season_id),
    round_number SMALLINT NOT NULL,
    label TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS public.fixture (
    fixture_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    season_id BIGINT NOT NULL REFERENCES public.season (season_id),
    round_id BIGINT NOT NULL REFERENCES public.matchday_round (round_id),
    home_team_id BIGINT NOT NULL REFERENCES public.team (team_id),
    away_team_id BIGINT NOT NULL REFERENCES public.team (team_id),
    venue_id BIGINT NOT NULL REFERENCES public.venue (venue_id),
    kickoff_utc TIMESTAMPTZ NOT NULL,
    status TEXT NOT NULL,
    CONSTRAINT fixture_status_chk CHECK (
        status IN ('scheduled', 'live', 'finished', 'postponed', 'cancelled')
    )
);

CREATE TABLE IF NOT EXISTS public.match_result (
    fixture_id BIGINT PRIMARY KEY REFERENCES public.fixture (fixture_id),
    home_goals SMALLINT NOT NULL,
    away_goals SMALLINT NOT NULL,
    home_xg NUMERIC(5, 3),
    away_xg NUMERIC(5, 3)
);
