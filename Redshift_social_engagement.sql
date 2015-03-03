create table maggie.dice_daus as
(
select
a.device_token_s
,a.dau_date as dte
,a.platform
from
(select device_token_s
,date(time_t) as dau_date
,case when _sys_platform_s ilike 'android' then 'Android'
when _sys_platform_s ilike 'ios' then 'iOS'
else 'other'
end as platform
from titan.dice_prod$sys_app_open
where 1=1
and (_sys_platform_s ilike 'ios' or _sys_platform_s ilike 'android')
--and time_t >= date(getdate()-90)
--and time_t < date(getdate()+1)
and date(time_t) between '2014-10-01' and '2015-01-11'
group by 1,2,3) a
left join data_mart.exception_cheaters b
on a.device_token_s = b.device_token
where b.device_token is null
group by 1,2,3) 
--end daus
;
create table maggie.dice_social as
--for dice 
(
select 
a.device_token_s
,a.viral_type
,a.total_social_engagements
,1 as flg
,a.dice_social_date as dte
from 
(select 
device_token_s
,date(time_t) as dice_social_date
,_viral_type_s as viral_type
,count (distinct time_t) as total_social_engagements
from titan.dice_prod$sys_viral 
where 1 =1 
and time_t < (date(getdate())+1)
group by 1,2,3
)a
group by 1,2,3,4,5
)
;
create table maggie.dice_social_count_daus as
(select
maggie.dice_daus.device_token_s
,maggie.dice_daus.dte
,maggie.dice_daus.platform
,nvl(maggie.dice_social.viral_type, 'none') as viral_type
,max(nvl(maggie.dice_social.flg,0)) as viral_flag
from maggie.dice_daus
left join maggie.dice_social
on maggie.dice_daus.device_token_s = maggie.dice_social.device_token_s
and maggie.dice_daus.dte = maggie.dice_social.dte
group by 1,2,3,4)
;
create table maggie.dice_social_daus_agg as
(
select 
dte
,platform
,count(distinct device_token_s) as daus
,count(distinct(case when viraL_flag = 1 then device_token_s else null end)) as dice_social_daus
from maggie.dice_social_count_daus
group by 1,2
)
;
create table maggie.dice_social_daus_percent as
(select 
dte
,platform
,case when (daus = 0 or daus is null) then dice_social_daus::float8/(1) else dice_social_daus::float8 / daus::float8 end as daus_percent
,daus
,dice_social_daus
from maggie.dice_social_daus_agg
)
select * from maggie.dice_social_daus_percent
;


create table maggie.bees_daus as
(
select
a.device_token_s
,a.dau_date as dte
,a.platform
from
(select device_token_s
,date(time_t) as dau_date
,case when _sys_platform_s ilike 'android' then 'Android'
when _sys_platform_s ilike 'ios' then 'iOS'
else 'other'
end as platform
from titan.disco_bees$sys_app_open
where 1=1
and (_sys_platform_s ilike 'ios' or _sys_platform_s ilike 'android')
--and time_t >= date(getdate()-90)
--and time_t < date(getdate()+1)
and date(time_t) between '2014-10-01' and '2015-01-11'
group by 1,2,3) a
left join data_mart.exception_cheaters b
on a.device_token_s = b.device_token
where b.device_token is null
group by 1,2,3)
--end daus
;
create table maggie.bees_social as
--for bees 
(
select 
a.device_token_s
,a.viral_type
,a.total_social_engagements
,1 as flg
,a.bees_social_date as dte
from 
(select 
device_token_s
,date(time_t) as bees_social_date
,_viral_type_s as viral_type
,count (distinct time_t) as total_social_engagements
from titan.disco_bees$sys_viral 
where 1 = 1 
and time_t < (date(getdate())+1)
group by 1,2,3
) a
group by 1,2,3,4,5
)
;
create table maggie.bees_social_count_daus as
(select
maggie.bees_daus.device_token_s
,maggie.bees_daus.dte
,maggie.bees_daus.platform
,nvl(maggie.bees_social.viral_type, 'none') as viral_type
,max(nvl(maggie.bees_social.flg,0)) as viral_flag
from maggie.bees_daus
left join maggie.bees_social
on maggie.bees_daus.device_token_s = maggie.bees_social.device_token_s
and maggie.bees_daus.dte = maggie.bees_social.dte
group by 1,2,3,4)
;
create table maggie.bees_social_daus_agg as
(
select 
dte
,platform
,count(distinct device_token_s) as daus
,count(distinct(case when viraL_flag = 1 then device_token_s else null end)) as bees_social_daus
from maggie.bees_social_count_daus
group by 1
)
;
create table maggie.bees_social_daus_percent as
(select 
dte
,platform
,case when (daus = 0 or daus is null) then bees_social_daus::float8/(1) else bees_social_daus::float8 / daus::float8 end as daus_percent
,daus
,bees_social_daus
from maggie.bees_social_daus_agg
);
select * from maggie.bees_social_daus_percent
