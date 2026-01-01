-- Add user scoping to entities and playbooks
alter table public.entities
  add column if not exists user_id uuid;

alter table public.entity_playbooks
  add column if not exists user_id uuid;

create index if not exists entities_user_id_created_at_idx
  on public.entities (user_id, created_at desc);

create index if not exists entity_playbooks_user_id_created_at_idx
  on public.entity_playbooks (user_id, created_at desc);
