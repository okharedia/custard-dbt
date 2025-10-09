-- models/staging/stg_parents.sql
{{ config(
    materialized='table',
    indexes=[
      {'columns': ['parent_id'], 'type': 'btree'},
      {'columns': ['household_id'], 'type': 'btree'}
    ]
) }}

{#----------------------------
Vars (with sensible defaults)
-----------------------------#}
{% set household_id = var('household_id', none) %}

select 
  p.id as parent_id, 
  p.name, 
  p.household_id
from {{ source('public', 'parents') }} p
{% if household_id %}
where p.household_id = {{ "'" ~ household_id ~ "'" }}::uuid
{% endif %}
