{{
  config(
          materialized='table'
        )
}}


                  /*Logic Steps in this model:
                        1. Calc user_new_status (CTE)
                        2. If the status != ignore then proceed to the next step - main query,
                              calc some of the Dim features
                  */

with full_outer_join_4_new_status as (

    select 
            /* identify new status of the user*/
            case when new.elig_status = 'enrolled' 
                        and curr.eid is null         
                 then 'new enrolled'
                 when new.elig_status = 'ineligible' 
                        and curr.eid_billing_status = 'active' 
                 then 'new ineligible'
                 when datediff('day', coalesce(curr.next_renewal_dt,'31-dec-2999'), 
                                      coalesce(date_trunc('day', new.dwh_last_synced_at), {{ var('run_date') }})) <= 0 /*run_date >= next_renewal_dt*/
                        and new.elig_status != 'ineligible' /*could be null or enrolled*/
                 then 'new renewal period'
                 else 'ignore' end as user_new_status

                 ,coalesce(new.eid, curr.eid) as eid
                 ,coalesce(new.employer_id, curr.employer_id) as employer_id

        from {{ref('milestone_elig_updated_users_stg')}} new 
        full outer join {{ source('milestone_billing','milestone_billing_users_dim') }} curr 
            on new.eid = curr.eid
            and new.uid = curr.uid
            and new.employer_id = curr.employer_id
        where user_new_status != 'ignore'

)

select 
        stt.eid
       ,stt.employer_id 
       ,new.uid                                                   as uid
       ,new.client_internal_uid                                   as client_internal_uid
       ,new.reseller_employee_id                                  as reseller_employee_id

       /*new billing status*/
       ,case when user_new_status = 'new ineligible' 
             then 'inactive'
             else 'active' end as eid_billing_status

       ,new.elig_status

       /*new milestone*/
       ,case when user_new_status in ('new enrolled', 'new renewal period')
             then 1
             else null end as milestone                                          -------needs to be updated when measurement data will be included!!!!

       ,case when user_new_status in ('new enrolled') 
             then date_trunc('day', new.dwh_last_synced_at)
             else null end as first_enrolled_dt

       ,null   as last_measurement_dt /*add logic that calc last usage/measurement date*/    -------needs to be updated when measurement data will be included!!!!

       ,case when eid_billing_status ='inactive' or user_new_status = 'new ineligible' --duplicate condition, 
                                                                                       --written like this in case that in the future those two statuses have different logic
             then date_trunc('day', new.dwh_last_synced_at)
             else null end as last_ineligibility_dt

       ,case when user_new_status in ('new enrolled', 'new renewal period')          
             then date_trunc('day', new.dwh_last_synced_at)
             else null end as last_milestone_change_dt           

       ,new.elig_created_at
       ,new.dwh_last_synced_at
       ,user_new_status

from full_outer_join_4_new_status stt
left outer join {{ref('milestone_elig_updated_users_stg')}} new  
      on    stt.eid = new.eid 
      and   stt.employer_id = new.employer_id

