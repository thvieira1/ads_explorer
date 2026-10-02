-- Rode uma vez no Supabase > SQL Editor para preparar as linhas que já existem.
-- Todas recebem o mesmo instante desta migração, versão 1 e ficam atuais.
begin;

alter table bronze.ads_information
    add column if not exists dt_loadtime timestamptz,
    add column if not exists flag_current boolean,
    add column if not exists version integer;

update bronze.ads_information
set dt_loadtime = coalesce(dt_loadtime, pg_catalog.statement_timestamp()),
    flag_current = coalesce(flag_current, true),
    version = coalesce(version, 1);

alter table bronze.ads_information
    drop constraint if exists ads_information_version_positive;

alter table bronze.ads_information
    alter column dt_loadtime set default now(),
    alter column dt_loadtime set not null,
    alter column flag_current set default true,
    alter column flag_current set not null,
    alter column version set default 1,
    alter column version set not null,
    add constraint ads_information_version_positive check (version > 0);

commit;
