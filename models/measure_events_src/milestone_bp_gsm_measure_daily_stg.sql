
{{ config(materialized='table',
          database = target.database) }} 



with look_for_the_lowest_systolic_value_per_day as (

        select  eid
                ,uid
                ,employer_id
                ,date_trunc('day', measured_at) as measured_dt
                ,min(case when event_type_field_name = 'systolic' then event_field_value end) as bp_sys
        from {{ref('milestone_bp_gsm_measurements_stg')}} measure 
        group by 1,2,3,4
),

raw_sys as (

        select   low_sys.eid
                ,low_sys.uid
                ,low_sys.employer_id
                ,low_sys.measured_dt
                ,low_sys.bp_sys

                /*max only for getting 1 and one row in case multiple number of measurements*/
                ,max(event_generic_id) as event_generic_id
        
        from --(
            --select * from 
            {{ref('milestone_bp_gsm_measurements_stg')}} 
            --union all
            --select * from {{ref('milestone_bp_gsm_measurements_stg')}}__qa --relevant only for QA and should be removed afterwards
            --) 
            raw
        inner join look_for_the_lowest_systolic_value_per_day low_sys
                on raw.eid = low_sys.eid
                and raw.uid = low_sys.uid
                and raw.employer_id = low_sys.employer_id
                and date_trunc('day', raw.measured_at) = low_sys.measured_dt
                and low_sys.bp_sys = raw.event_field_value
        where  raw.event_type_field_name = 'systolic'
        group by 1,2,3,4,5
        ),

final as (

        select   raw_sys.eid
                ,raw_sys.uid
                ,raw_sys.employer_id
                ,raw_sys.measured_dt
                ,raw_sys.bp_sys
                ,bp_dis.event_field_value as bp_dis
                ,bp_dis.measured_at 
                ,bp_dis.server_arrived_at
                ,bp_dis.dwh_synced_at
        from raw_sys
        inner join {{ref('milestone_bp_gsm_measurements_stg')}} bp_dis
                on raw_sys.eid = bp_dis.eid
                and raw_sys.uid = bp_dis.uid
                and raw_sys.employer_id = bp_dis.employer_id
                and raw_sys.measured_dt = date_trunc('day', bp_dis.measured_at) 
                and raw_sys.event_generic_id = bp_dis.event_generic_id
        where  bp_dis.event_type_field_name = 'diastolic'
)

select * 
from final