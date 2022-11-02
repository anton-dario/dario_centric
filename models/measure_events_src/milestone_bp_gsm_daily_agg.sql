
{{ config(materialized='incremental',
          unique_key = ['eid', 'uid', 'employer_id', 'measured_dt'],
          on_schema_change='sync_all_columns'

          )
}} 


with source as (

select  
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