{{
  config(
          materialized='incremental',
          unique_key = 'dwh_hash_user_id'
        )
}}



with dim_refresh as (

 select  
             coalesce(new.eid, dim.eid) as eid,
             coalesce(new.uid, dim.uid) as uid,
             {{ dbt_utils.surrogate_key(['coalesce(new.eid, dim.eid)', 'coalesce(new.employer_id, dim.employer_id)']) }}  as dwh_hash_user_id,
             /*the reason why uid is not part of surr. key is technical, 
               the column not included in eligibility_list source 
               and that the reason that in a current situation uid holding a null value.
               It should be changed in a near future (01/11/2022) 
               then we reconsider to add uid into logical user unique key (EID + UID + EMPLOYER_ID)*/

             coalesce(new.client_internal_uid, dim.client_internal_uid)                   as client_internal_uid, 
             coalesce(new.reseller_employee_id, dim.reseller_employee_id)                 as reseller_employee_id, 
             coalesce(new.employer_id, dim.employer_id)                                   as employer_id, 

             
             coalesce(new.eid_billing_status, dim.eid_billing_status)                     as eid_billing_status,
             coalesce(new.elig_status, dim.elig_status)                                   as elig_status,


             coalesce(new.first_enrolled_dt, dim.first_enrolled_dt)                       as first_enrolled_dt,
             coalesce(new.last_ineligibility_dt, dim.last_ineligibility_dt)               as last_ineligibility_dt,

             case when new.user_new_status in ('new enrolled', 'new renewal period')
             then dateadd(year, 1, to_date(new.dwh_last_synced_at))                              
             when coalesce(new.last_ineligibility_dt, dim.last_ineligibility_dt) is not null 
             then null --no renewal needed for ineligible user 
             else dim.next_renewal_dt end                                                 as next_renewal_dt,
          
             case when new.user_new_status = 'new enrolled'
             then to_date(new.dwh_last_synced_at)
             when new.user_new_status = 'new renewal period'     
             then dim.next_renewal_dt 
             else dim.current_period_start_dt end                                         as current_period_start_dt,


             coalesce(new.dwh_last_synced_at, dim.dwh_last_synced_at)                     as dwh_last_synced_at,
             coalesce(new.elig_created_at, dim.elig_created_at)                           as elig_created_at,
             case when dim.dwh_created_at is not null 
                  then dim.dwh_created_at
                  else sysdate() end                                                      as dwh_created_at,
             case when new.eid is null  
                  then dim.dwh_updated_at
                  else sysdate() end                                                      as dwh_updated_at,

             case when new.milestone is null then dim.milestone                                                   
                  else new.milestone end as milestone,

             dim.m2_achieved_dt,
             dim.m3_achieved_dt,
             coalesce(new.last_milestone_change_dt, dim.last_milestone_change_dt) as last_milestone_change_dt,
             dim.is_m2_achieved,
             dim.is_m3_achieved

     from {{ref('milestone_billing_users_status_logic_stg')}} new 

     full outer join {{ this }} dim
          on new.eid = dim.eid
          and new.employer_id = dim.employer_id

          ),


dim_based_on_measurement_logic as (

     select  
             dim.eid,
             dim.uid,
             dim.dwh_hash_user_id,
             dim.client_internal_uid, 
             dim.reseller_employee_id, 
             dim.employer_id, 

             
             dim.eid_billing_status,
             dim.elig_status,


             dim.first_enrolled_dt,
             dim.current_period_start_dt,
             dim.next_renewal_dt,
             dim.last_ineligibility_dt,
             

             dim.dwh_last_synced_at,
             dim.elig_created_at,
             dim.dwh_created_at,


                              /* M2 & M3 date ranges */
             dim.current_period_start_dt + {{ var("m2_start_day") }}  as milestone2_start_dt,
             dim.current_period_start_dt + {{ var("m2_end_day") }}    as milestone2_end_dt,  

             dim.current_period_start_dt + {{ var("m3_start_day") }}  as milestone3_start_dt,
             dim.current_period_start_dt + {{ var("m3_end_day") }}    as milestone3_end_dt,

                              /* M2 & M3 count measurement days */
             sum(case when agg.measured_dt between milestone2_start_dt and milestone2_end_dt
                  then 1 else 0 end) as m2_measument_days,
             sum(case when agg.measured_dt between milestone3_start_dt and milestone3_end_dt
                  then 1 else 0 end) as m3_measument_days,      

                              /* M2 & M3 achivement logic*/
             case when m2_measument_days >=  {{ var("m2_bp_daily_measurement_count") }}
                  then true else false end as _is_m2_achieved,
             case when m3_measument_days >=  {{ var("m3_bp_daily_measurement_count") }}
                  then true else false end as _is_m3_achieved,

                              /*M2 & M3 achivement dates*/
             case when dim.m2_achieved_dt is null and _is_m2_achieved 
                  then {{ var("run_date") }} 
                  else dim.m2_achieved_dt end as m2_achieved_dt,

             case when dim.m3_achieved_dt is null and _is_m3_achieved 
                  then {{ var("run_date") }} 
                  else dim.m3_achieved_dt end as m3_achieved_dt,

                              /*Current Max Milestone achieved*/
             case when _is_m3_achieved then 3                                                   --M4 & M5 should be added here
                  when not _is_m3_achieved and _is_m2_achieved then 2
                  else dim.milestone end as milestone,

                              /*Last measurement date*/
             max(agg.measured_dt) as last_measurement_dt,

                         /*Last milestone change date*/
             greatest(dim.last_milestone_change_dt,
                      coalesce(m2_achieved_dt,'01-jan-1900'),
                      coalesce(m3_achieved_dt,'01-jan-1900'))    as last_milestone_change_dt,      --M4 & M5 should be added here


             case when last_milestone_change_dt > dim.dwh_updated_at
                  then last_milestone_change_dt
                  else dim.dwh_updated_at end                    as dwh_updated_at 

     from dim_refresh dim

     left outer join {{ref('milestone_bp_gsm_daily_agg')}} agg 
          on  dim.eid = agg.eid
          and dim.employer_id = agg.employer_id
          and not (dim.is_m2_achieved and dim.is_m3_achieved)                                   --M4 & M5 should be added here
          
     group by dim.eid,
             dim.uid,
             dim.dwh_hash_user_id,
             dim.client_internal_uid, 
             dim.reseller_employee_id, 
             dim.employer_id, 
             dim.eid_billing_status,
             dim.elig_status,
             dim.first_enrolled_dt,
             dim.current_period_start_dt,
             dim.next_renewal_dt,
             dim.last_ineligibility_dt,
             dim.dwh_last_synced_at,
             dim.elig_created_at,
             dim.dwh_created_at,
             dim.dwh_updated_at,   
             dim.m2_achieved_dt,
             dim.m3_achieved_dt,
             dim.milestone,
             dim.last_milestone_change_dt
               
     )



select         dim.eid,
               dim.uid,
               dim.dwh_hash_user_id,
               dim.client_internal_uid, 
               dim.reseller_employee_id, 
               dim.employer_id, 
               dim.eid_billing_status,
               dim.elig_status,
               dim.milestone,

               dim.first_enrolled_dt,
               dim.current_period_start_dt,
               dim.next_renewal_dt,
               dim.last_measurement_dt,
               dim.last_ineligibility_dt,
               dim.last_milestone_change_dt, 

               dim._is_m2_achieved as is_m2_achieved,
               dim._is_m3_achieved as is_m3_achieved,                  
               false as is_m4_achieved,               
               false as is_m5_achieved,                 
               dim.m2_achieved_dt,                  
               dim.m3_achieved_dt,                  
               null as m4_achieved_dt,                  
               null as m5_achieved_dt,                 

               dim.dwh_last_synced_at,
               dim.elig_created_at,
               dim.dwh_created_at,
               dim.dwh_updated_at
from dim_based_on_measurement_logic dim

/*
    uncomment the line below to remove records with null `id` values
*/

-- where id is not null



/*
create or replace table stg_dwh.dbt_dev_anton.milestone_billing_users_dim (
	eid                             varchar(45) not null,
	uid                             number,
	dwh_hash_user_id                varchar(32) not null,
	client_internal_uid             varchar(255),
	reseller_employee_id            varchar(255),
	employer_id                     number      not null,

	eid_billing_status              varchar(30) not null,
	elig_status                     varchar(45) not null,
	milestone                       number      not null,

	first_enrolled_dt               date        not null,
	current_period_start_dt         date        not null,
	next_renewal_dt                 date        not null,
	last_measurement_dt             date,
	last_ineligibility_dt           date,
	last_milestone_change_dt        date        not null,

     is_m2_achieved                  boolean default false,
     is_m3_achieved                  boolean default false,
     is_m4_achieved                  boolean default false,
     is_m5_achieved                  boolean default false,
     m2_achieved_dt                  date,
     m3_achieved_dt                  date,
     m4_achieved_dt                  date,
     m5_achieved_dt                  date,

	dwh_last_synced_at              timestamp   not null,
	elig_created_at                 timestamp   not null,
	dwh_created_at                  timestamp   not null,
	dwh_updated_at                  timestamp   not null,
    
    primary key (dwh_hash_user_id)
); 


*/