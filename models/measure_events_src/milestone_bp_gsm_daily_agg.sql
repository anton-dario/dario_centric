
{{ config(materialized='incremental',
          unique_key = 'dwh_hash_id'

          )
}} 


with source as (

select  {{ dbt_utils.surrogate_key(['stg.eid', 'stg.employer_id', 'stg.measured_dt']) }}  as dwh_hash_id,
        stg.*,
        sysdate() as dwh_modify_at
from {{ref('milestone_bp_gsm_measure_daily_stg')}} stg
)


select * 
from source

{% if is_incremental() %}

  -- this filter will only be applied on an incremental run
  --where date_day >= (select max(date_day) from {{ this }}) ---Optional

{% endif %}