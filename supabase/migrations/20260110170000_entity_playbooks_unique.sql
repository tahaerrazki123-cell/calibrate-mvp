-- Ensure entity_playbooks has a unique key matching upsert target.
update entity_playbooks ep
set user_id = e.user_id
from entities e
where ep.user_id is null
  and ep.entity_id = e.id;

with ranked as (
  select
    id,
    row_number() over (
      partition by user_id, entity_id
      order by coalesce(updated_at, created_at) desc nulls last
    ) as rn
  from entity_playbooks
)
delete from entity_playbooks ep
using ranked r
where ep.id = r.id
  and r.rn > 1;

create unique index if not exists entity_playbooks_user_entity_unique
  on entity_playbooks (user_id, entity_id);
