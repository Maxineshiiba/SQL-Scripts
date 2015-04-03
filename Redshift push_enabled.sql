-----written in Redshift

drop table  maggie.dice_push_enabled
;
drop table  maggie.dice_push_daus
;
drop table maggie.dice_push_daus_agg
;
drop table maggie.dice_push_daus_percent
;

drop table  maggie.bees_push_enabled
;
drop table  maggie.bees_push_daus
;
drop table maggie.bees_push_daus_agg
;
drop table maggie.bees_push_daus_percent




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
and time_t >= '2014-10-01'
and time_t < date(getdate()+1)
and _sys_platform_s ilike 'ios'
group by 1,2,3) a
left join data_mart.exception_cheaters b
on a.device_token_s = b.device_token
where b.device_token is null
group by 1,2,3)
--end daus
;
create table maggie.dice_push_enabled as
--ios only
(
select 
a.device_token_s
,a.platform
,a.push_flag
,1 as flg
from
(
select 
device_token_s
,time_t
,case when _sys_platform_s ilike 'android' then 'android' when _sys_platform_s ilike 'ios' then 'ios' else null end as platform
,case when _sys_push_enabled_b is true then 1 when  _sys_push_enabled_b is false then 0 end as push_flag
from titan.dice_prod$sys_connect
where 1=1
and time_t >= '2014-10-01'
and time_t < (date(getdate())+1)
and _sys_platform_s ilike 'ios'
--and (_sys_platform_s ilike 'android' or _sys_platform_s ilike 'ios')
and (_sys_push_enabled_b is true or _sys_push_enabled_b is false)
group by 1,2,3,4
)a
inner join
(
select 
device_token_s
,case when _sys_platform_s ilike 'android' then 'android' when _sys_platform_s ilike 'ios' then 'ios' else null end as platform
,max(time_t) as maxt
from titan.dice_prod$sys_connect
where 1=1
and time_t >= '2014-10-01'
and time_t < (date(getdate())+1)
and _sys_platform_s ilike 'ios'
--and (_sys_platform_s ilike 'android' or _sys_platform_s ilike 'ios')
and (_sys_push_enabled_b is true or _sys_push_enabled_b is false)
group by 1,2
) b
on a.device_token_s = b.device_token_s
and a.time_t = b.maxt
and a.platform = b.platform
group by 1,2,3,4
)
;
create table maggie.dice_push_daus as
(select
maggie.dice_daus.device_token_s
,maggie.dice_daus.dte
,maggie.dice_daus.platform
,max(nvl(maggie.dice_push_enabled.push_flag,0)) as push_flag
from maggie.dice_daus
left join maggie.dice_push_enabled
on maggie.dice_daus.device_token_s = maggie.dice_push_enabled.device_token_s
group by 1,2,3)
;
create table maggie.dice_push_daus_agg as
(
select 
dte
,platform
,count(distinct device_token_s) as daus
,count(distinct(case when push_flag = 1 then device_token_s else null end)) as push_daus
from maggie.dice_push_daus
group by 1,2
)
;
create table maggie.dice_push_daus_percent as
(select 
dte
,platform
,case when (daus = 0 or daus is null) then push_daus::float8/(1) else push_daus::float8 / daus::float8 end as daus_percent
,daus
,push_daus
from maggie.dice_push_daus_agg
)
;
select * from maggie.dice_push_daus_percent

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
and _sys_platform_s ilike 'ios'
and time_t >= '2014-10-01'
and time_t < date(getdate()+1)
group by 1,2,3) a
left join data_mart.exception_cheaters b
on a.device_token_s = b.device_token
where b.device_token is null
group by 1,2,3)
;

create table maggie.bees_push_enabled as
--ios only
(
select 
a.device_token_s
,a.platform
,a.push_flag
,1 as flg
from
(
select 
device_token_s
,time_t
,case when _sys_platform_s ilike 'android' then 'android' when _sys_platform_s ilike 'ios' then 'ios' else null end as platform
,case when _sys_push_enabled_b is true then 1 when  _sys_push_enabled_b is false then 0 end as push_flag
from titan.disco_bees$sys_connect
where 1=1
and time_t < (date(getdate())+1)
and _sys_platform_s ilike 'ios'
--and (_sys_platform_s ilike 'android' or _sys_platform_s ilike 'ios')
and (_sys_push_enabled_b is true or _sys_push_enabled_b is false)
group by 1,2,3,4
)a
inner join
(
select 
device_token_s
,case when _sys_platform_s ilike 'android' then 'android' when _sys_platform_s ilike 'ios' then 'ios' else null end as platform
,max(time_t) as maxt
from titan.disco_bees$sys_connect
where 1=1
and time_t < (date(getdate())+1)
and _sys_platform_s ilike 'ios'
--and (_sys_platform_s ilike 'android' or _sys_platform_s ilike 'ios')
and (_sys_push_enabled_b is true or _sys_push_enabled_b is false)
group by 1,2
) b
on a.device_token_s = b.device_token_s
and a.time_t = b.maxt
and a.platform = b.platform
group by 1,2,3,4
)
;

create table maggie.bees_push_daus as
(select
maggie.bees_daus.device_token_s
,maggie.bees_daus.dte
,maggie.bees_daus.platform
,max(nvl(maggie.bees_push_enabled.push_flag,0)) as push_flag
from maggie.bees_daus
left join maggie.bees_push_enabled
on maggie.bees_daus.device_token_s = maggie.bees_push_enabled.device_token_s
group by 1,2,3)
;
create table maggie.bees_push_daus_agg as
(
select 
dte
,platform
,count(distinct device_token_s) as daus
,count(distinct(case when push_flag = 1 then device_token_s else null end)) as push_daus
from maggie.bees_push_daus
group by 1,2)
;
create table maggie.bees_push_daus_percent as
(select 
dte
,platform
,case when (daus = 0 or daus is null) then push_daus::float8/(1) else push_daus::float8 / daus::float8 end as daus_percent
,daus
,push_daus
from maggie.bees_push_daus_agg
);
select * from maggie.bees_push_daus_percent
