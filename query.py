match_info_database = """
CREATE SEQUENCE IF NOT EXISTS id_match_info_seq;
CREATE TABLE IF NOT EXISTS public.match_info
(
    id integer NOT NULL,
    match_id bigint NOT NULL,
    barracks_status_dire integer,
    barracks_status_radiant integer,
    dire_score smallint,
    radiant_score smallint,
    draft_timings json[],
    duration integer,
    firstblood_claimed integer,
    league_id smallint,
    lobby_type smallint,
    radiant_xp_adv integer[],
    start_time date,
    tower_status_dire integer,
    tower_status_radiant integer,
    skill smallint,
    patch smallint,
    region smallint,
    replay_url character varying COLLATE pg_catalog."default",
    CONSTRAINT match_info_pkey PRIMARY KEY (id),
    CONSTRAINT match_id_fk FOREIGN KEY (match_id)
        REFERENCES public.match (match_id) MATCH SIMPLE
        ON UPDATE NO ACTION
        ON DELETE NO ACTION
        NOT VALID
);
ALTER TABLE match_info ALTER id SET DEFAULT NEXTVAL('id_match_info_seq');
"""
match_info_insert = """
INSERT INTO public.match_info(
	match_id, barracks_status_dire, barracks_status_radiant, dire_score, radiant_score, draft_timings, duration, firstblood_claimed, league_id, lobby_type, radiant_xp_adv, start_time, tower_status_dire, tower_status_radiant, skill, patch, region, replay_url)
	VALUES (%s, %s, %s, %s, %s, %s::json[], %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
"""
match_database = """
CREATE SEQUENCE IF NOT EXISTS id_match_seq;
CREATE TABLE IF NOT EXISTS public.match
(
    id integer NOT NULL,
    match_id bigint NOT NULL,
    radiant_win boolean,
    duration integer,
    avg_mmr integer,
    num_mmr integer,
    lobby_type smallint,
    game_mode smallint,
    avg_rank_tier smallint,
    num_rank_tier smallint,
    cluster integer,
    CONSTRAINT id_pkey PRIMARY KEY (id),
    CONSTRAINT id_unique UNIQUE (id),
    CONSTRAINT match_unique UNIQUE (match_id)
);
ALTER TABLE match ALTER id SET DEFAULT NEXTVAL('id_match_seq');
"""
matches_insert = """
INSERT INTO public.match(
	match_id, radiant_win, duration, avg_mmr, num_mmr, lobby_type, game_mode, avg_rank_tier, rum_rank_tier, cluster)
	VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
"""

player_database = """
CREATE SEQUENCE IF NOT EXISTS id_player_seq;
CREATE TABLE IF NOT EXISTS public.player
(
    id integer NOT NULL,
    match_id bigint NOT NULL,
    account_id bigint,
    player_slot smallint,
    ability_upgrades_arr integer[],
    assists integer,
    kills integer,
    deaths integer,
    hero_id integer,
    item_0 integer,
    item_1 integer,
    item_2 integer,
    item_3 integer,
    item_4 integer,
    item_5 integer,
    item_neutral integer,
    backpack_0 integer,
    backpack_1 integer,
    backpack_2 integer,
    denies integer,
    last_hits integer,
    gold integer,
    gold_per_min integer,
    xp_per_min integer,
    net_worth integer,
    gold_spent integer,
    level smallint,
    hero_damage integer,
    hero_healing integer,
    tower_damage integer,
    rank_tier smallint,
    permanent_buffs json[],
    kills_per_min double precision,
    last_hits_per_min double precision,
    hero_damage_per_min double precision,
    hero_healing_per_min double precision,
    stuns_per_min double precision,
    CONSTRAINT player_pkey PRIMARY KEY (id),
    CONSTRAINT match_id_fk FOREIGN KEY (match_id)
        REFERENCES public.match (match_id) MATCH SIMPLE
        ON UPDATE NO ACTION
        ON DELETE NO ACTION
        NOT VALID
);
ALTER TABLE player ALTER id SET DEFAULT NEXTVAL('id_player_seq');
"""
player_insert = """
INSERT INTO public.player(
	match_id, account_id, player_slot, ability_upgrades_arr, assists, kills, deaths, hero_id, item_0, item_1, item_2, item_3, item_4, item_5, item_neutral, backpack_0, backpack_1, backpack_2, denies, last_hits, gold, net_worth, gold_spent, level, hero_damage, hero_healing, rank_tier, permanent_buffs, gold_per_min, xp_per_min, kills_per_min, last_hits_per_min, hero_damage_per_min, hero_healing_per_min, tower_damage, stuns_per_min)
	VALUES (%s, %s, %s, %s::integer[], %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s::json[], %s, %s, %s, %s, %s, %s, %s, %s);
"""

