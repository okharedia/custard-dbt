create table public.households (
  id uuid not null default gen_random_uuid (),
  name character varying(100) not null,
  created_at timestamp with time zone not null default CURRENT_TIMESTAMP,
  updated_at timestamp with time zone not null default CURRENT_TIMESTAMP,
  constraint households_pkey primary key (id)
) TABLESPACE pg_default;

create table public.parents (
  id uuid not null default gen_random_uuid (),
  name character varying(255) not null,
  household_id uuid not null,
  created_at timestamp with time zone not null default CURRENT_TIMESTAMP,
  updated_at timestamp with time zone not null default CURRENT_TIMESTAMP,
  constraint parents_pkey primary key (id),
  constraint parents_household_id_foreign foreign KEY (household_id) references households (id) on delete CASCADE
) TABLESPACE pg_default;

create table public.schedules (
  parent_id uuid not null,
  calendar text not null,
  created_at timestamp with time zone not null default CURRENT_TIMESTAMP,
  updated_at timestamp with time zone not null default CURRENT_TIMESTAMP,
  constraint schedules_pkey primary key (parent_id),
  constraint schedules_parent_id_foreign foreign KEY (parent_id) references parents (id) on delete CASCADE
) TABLESPACE pg_default;

create table public.base_agreements (
  id uuid not null default gen_random_uuid (),
  during tstzrange not null,
  parent_id uuid not null,
  constraint base_agreements_pkey primary key (id),
  constraint base_agreements_parent_id_fkey foreign KEY (parent_id) references parents (id) on delete CASCADE
) TABLESPACE pg_default;
create index IF not exists base_agreements_index on public.base_agreements using gist (during) TABLESPACE pg_default;

create table public.additional_agreements (
  id uuid not null default gen_random_uuid (),
  from_parent_id uuid not null,
  to_parent_id uuid not null,
  during tstzrange not null,
  created_at timestamp with time zone not null default CURRENT_TIMESTAMP,
  updated_at timestamp with time zone not null default CURRENT_TIMESTAMP,
  constraint additional_agreements_pkey primary key (id),
  constraint additional_agreements_from_parent_id_foreign foreign KEY (from_parent_id) references parents (id) on delete CASCADE,
  constraint additional_agreements_to_parent_id_foreign foreign KEY (to_parent_id) references parents (id) on delete CASCADE
) TABLESPACE pg_default;
create index IF not exists additional_agreements_during_index on public.additional_agreements using gist (during) TABLESPACE pg_default;
create index IF not exists additional_agreements_from_parent_id_index on public.additional_agreements using btree (from_parent_id) TABLESPACE pg_default;
create index IF not exists additional_agreements_to_parent_id_index on public.additional_agreements using btree (to_parent_id) TABLESPACE pg_default;

create table public.additional_agreements_details (
  additional_agreements_id uuid not null,
  reason text not null,
  created_at timestamp with time zone not null default CURRENT_TIMESTAMP,
  updated_at timestamp with time zone not null default CURRENT_TIMESTAMP,
  constraint additional_agreements_details_pkey primary key (additional_agreements_id),
  constraint additional_agreements_details_additional_agreements_id_foreign foreign KEY (additional_agreements_id) references additional_agreements (id) on delete CASCADE
) TABLESPACE pg_default;