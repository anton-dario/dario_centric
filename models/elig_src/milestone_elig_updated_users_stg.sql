{{ config(materialized='table',
          database = target.database) }}

with source_data as (

    select   eid
            ,elig.dario_app_uid as uid
            ,elig.attribute_1 as client_internal_uid
            ,elig.reseller_employee_id 
            ,elig.employer_id
            ,elig.status as elig_status
            ,to_timestamp_ntz(elig.created_at) as elig_created_at
            ,to_timestamp_ntz(elig.updated_at) as elig_last_updated_at
            ,to_timestamp_ntz(elig._fivetran_synced) as dwh_last_synced_at

            -- ,case when elig.status= 'ineligible'  then elig._fivetran_synced else null end as disenrolled_dwh_dt
            -- ,case when elig.status= 'enrolled'    then elig._fivetran_synced else null end as enrolled_dwh_dt
            -- ,case   when enrolled_dwh_dt is not null and datediff('day',elig_created_at, {{ var('run_date') }}) < 365 
            --         then elig_created_at 
            --         else dateadd ('day',(datediff('day',elig_created_at, {{ var('run_date') }})%365)*-1,{{ var('run_date') }} )  
            --  end renewal_date 
    from  {{ source('eligibility','eligibility_list') }} elig
        where not elig._fivetran_deleted 
        and elig._fivetran_synced >= {{ var('run_date') }}
        and elig._fivetran_synced < {{ var('run_date') }} + {{ var('incremental_loading_days_scope') }} 
        and elig.status in ('ineligible', 'enrolled') --at this point we didn't pull all "eligible" users 
        and elig.employer_id in ({{ var('employer_id') }})
        and not exists (select 1 
                        from users.test_users test
                        where test.last_app_id = elig.dario_app_uid)
)

select *
from source_data

/*
    uncomment the line below to remove records with null `id` values
*/

-- where id is not null
