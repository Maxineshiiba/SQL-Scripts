----Slots Payers Dashboard
-----written in Redshift
select 
case
when platform ilike '%i%' then 'iOS'
when platform ilike '%a%' then 'Android'
when platform ilike '%z%' then 'Amazon'
else 'Other'
end as platform
,date(install_cohort) as dte
,source_table
,count(distinct(case when is_payer = 1 then user_id else null end)) as payers
,count(distinct(user_id)) as all_users
from 
models.slots_user_summary
group by 1,2,3
