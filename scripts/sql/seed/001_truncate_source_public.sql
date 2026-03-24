-- Reset source public data for a clean re-seed (postgres_source / source_db).
-- Run before 002 and 003 if tables already contain rows from a previous seed.

TRUNCATE TABLE
    public.match_result,
    public.fixture,
    public.matchday_round,
    public.player_contract,
    public.player,
    public.team,
    public.season,
    public.venue,
    public.competition
RESTART IDENTITY CASCADE;
