
{{ config(materialized='table',
          database = target.database) }} 


with external_employer_id as ( 
/*needed for employer_id connectivity with eligibility_b2b_data table*/
select id as employer_id,
       name as employer_name,
       to_number(external_id) as external_id
from 
{{ source('eligibility','employers') }}
where id in ({{ var('employer_id') }})
),


bp_gsm_device_requested_users as (
select  elig_b2b.eid,
        elig_b2b.uid,
        ext.employer_id,
        ext.employer_name,
        elig_b2b._fivetran_synced as elig_b2b_fivetran_synced,
        pp.status as user_provisioning_status,
        pp.meta_data as device_details_json,
        api.dcuid as device_clinic_id, 
        api.name  as device_name, 
        api.config as device_config_json
from {{ source('bknd_dariocare','provisioning') }} pp
left outer join {{ source('bknd_dariocare','api_clinics') }} api         
        on pp.clinic_id = api.dcuid
left outer join {{ source('phi_db','eligibility_b2b_data') }} elig_b2b 
        on pp.uid = elig_b2b.uid
left outer join external_employer_id ext
        on elig_b2b.employer_id = ext.external_id
where lower(api.name) like '%dario%blood pressure%gsm%' --clinicid = 10529 31/10/2022
and not pp._fivetran_deleted
and not api._fivetran_deleted 
and not elig_b2b._fivetran_deleted 

and ext.employer_id in ({{ var('employer_id') }}) --bp gsm employers, current pre-prod state - atd,cfhc and colorado_acc clients
),

blood_pressure_types as 
(
  select f.id as event_type_field_id,
         f.name as event_type_field_name,
         t.id   as event_type_id
    from {{ source('phi_db','event_type') }} t
    inner join {{ source('phi_db','event_type_field') }} f
        on t.id = f.event_type_id
  where lower(t.name) like '%blood%pressure%'
    and f.name in ('systolic','diastolic')
    and not t._fivetran_deleted
    and not f._fivetran_deleted
)

select  u.eid
        ,u.uid
        ,e.id                           as event_generic_id
        ,bpt.event_type_field_name
        ,to_number(val.field_value)     as event_field_value
        ,u.employer_id
        ,e.deviceutc                    as measured_at
        ,e.localutc                     as server_arrived_at
        ,e._fivetran_synced             as dwh_synced_at
from {{ source('phi_db','event_generic') }} e 
inner join {{ source('phi_db','event_type_field_value') }} val 
        on e.id = val.event_generic_id
inner join blood_pressure_types bpt
        on val.event_type_field_id = bpt.event_type_field_id
inner join bp_gsm_device_requested_users u
        on e.uid = u.uid
where not e._fivetran_deleted
  and not val._fivetran_deleted
  and e._fivetran_synced >= {{ var('run_date') }}
  and e._fivetran_synced <  {{ var('run_date') }} + {{ var('incremental_loading_days_scope') }} 
