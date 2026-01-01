-- Cache entity playbooks with freshness metadata
alter table public.entity_playbooks
  add column if not exists playbook_json jsonb,
  add column if not exists updated_at timestamptz default now(),
  add column if not exists last_run_created_at timestamptz;

create unique index if not exists entity_playbooks_entity_id_key
  on public.entity_playbooks (entity_id);

create index if not exists entity_playbooks_user_id_updated_at_idx
  on public.entity_playbooks (user_id, updated_at desc);
