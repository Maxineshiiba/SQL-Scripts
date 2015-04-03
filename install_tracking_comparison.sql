-----------------------written in Redshift

truncate bi.installqa_fact_ua 
;
truncate bi.installqa_user_summary
;
truncate bi.installqa_dim_user
;
truncate bi.installqa_compilation
;

insert into  bi.installqa_fact_ua
(
select a.event_date as install_cohort, b.game_name, d.channel_name 
,case when platform_name ilike 'amazon' then 'amazon'
when platform_name ilike 'android' then 'android'
when platform_name ilike 'ios' then 'ios'
else 'others' 
end as platform,  
count(total_installs) as install_counts 
from data_mart.fact_ua_install a
inner join data_mart.dim_game b
on a.game_id = b.game_id
and a.event_date >= '2014-11-01'
inner join data_mart.dim_platform c
on a.platform_id = c.platform_id
inner join data_mart.dim_channel d
on a.channel = d.channel_id
group by 1,2,3,4
)
;

insert into  bi.installqa_user_summary
(
select install_cohort
,'dice' as game_name
,channel as channel_name
,case when platform ilike 'z' then 'amazon'
when platform ilike 'a' then 'android'
when platform ilike 'i' then 'ios'
else 'unknown'
end as platform
,count(distinct user_id) as install_counts
from models.dice_user_summary
where install_cohort >= '2014-11-01'
group by 1,2,3,4
union all
select install_cohort
,'slots' as game_name
,channel as channel_name
,case when platform ilike 'a' then 'android'
when platform ilike 'i' then 'ios'
when platform ilike 'z' then 'amazon'
else 'unknown'
end as platform
,count(distinct user_id) as install_counts
from models.slots_user_summary
where install_cohort >= '2014-11-01'
group by 1,2,3,4 
union all
select install_cohort
,'bees' as game_name
,case when publisher ilike 'Facebook' then 'paid'
when publisher ilike 'unknown' then 'organic'
else 'unknown'
end as channel_name
,case when platform ilike 'iOS' then 'ios'
when platform ilike 'Android' then 'android'
else'unknown'
end as platform
,count(distinct user_id) as install_counts
from models.discobees_user_summary
where install_cohort >= '2014-11-01'
group by 1,2,3,4
)
;
insert into  bi.installqa_dim_user
(
select install_date as install_cohort, b.game_name, d.channel_name 
,case when platform_name ilike 'amazon' then 'amazon'
when platform_name ilike 'android' then 'android'
when platform_name ilike 'ios' then 'ios'
else 'others' 
end as platform,  
count(distinct(device_token)) as install_counts 
from data_mart.dim_user a
inner join data_mart.dim_game b
on a.game_id = b.game_id 
and a.install_date >= '2014-11-01'
inner join data_mart.dim_platform c
on a.platform_id = c.platform_id
inner join data_mart.dim_channel d
on a.channel_id = d.channel_id
group by 1,2,3,4
)
;

insert into  bi.installqa_compilation
(
select
s.install_cohort
,s.game_name
,s.channel_name
,s.platform
,sum(s.install_counts) as installss_fact_ua
,sum(nvl(w.install_counts,0)) as installs_user_summary
,sum(nvl(u.install_counts,0)) as installs_dim_user
from bi.installqa_fact_ua s
left join 
bi.installqa_user_summary w
on s.install_cohort = w.install_cohort
and s.game_name = w.game_name
and s.channel_name = w.channel_name
and s.platform = w.platform
left join 
bi.installqa_dim_user u
on s.install_cohort = u.install_cohort
and s.game_name = u.game_name
and s.channel_name = u.channel_name
and s.platform = u.platform
group by 1,2,3,4
)
;

