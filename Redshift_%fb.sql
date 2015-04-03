-------------------written in Redshift

drop table maggie.dice_fb_con
;
drop table maggie.dice_fb_count_daus 
;
drop table maggie.dice_fb_daus_agg
;
drop table maggie.dice_fb_daus_percent
;
drop table maggie.bees_fb_con
;
drop table maggie.bees_fb_count_daus 
;
drop table maggie.bees_fb_daus_agg
;
drop table maggie.bees_fb_daus_percent
;
create table maggie.dice_daus as 
(
select
a.device_token_s
,a.dau_date as dte
,a.platform
from
(select device_token_s
,date(time_t) as dau_date
,
case when _sys_platform_s ilike 'android' then 'Android'
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
create table maggie.dice_fb_con as(
select
a.device_token_s
,a.connect_date as dte
,1 as fb_flg
from
(
select
device_token_s
,min(date(time_t)) as connect_date
from titan.dice_prod$sys_connect
where 1=1
--and time_t >= date(getdate()-90)
--and time_t < date(getdate()+1)
and date(time_t) <= '2015-01-11' 
--and date(time_t) between '2014-10-01' and '2015-01-11' 
and _connection_s = 'Facebook'
group by 1
)a
left join data_mart.exception_cheaters b
on a.device_token_s = b.device_token
where b.device_token is null
group by 1,2,3
)
;
create table maggie.dice_fb_count_daus as
(select
dice_daus.device_token_s
,dice_daus.dte
,dice_daus.platform
,max(nvl(dice_fb_con.fb_flg,0)) as fb_con_flag
from maggie.dice_daus dice_daus
left join maggie.dice_fb_con dice_fb_con
on dice_daus.device_token_s = dice_fb_con.device_token_s
and dice_daus.dte >= dice_fb_con.dte
group by 1,2,3)
;
create table
maggie.dice_fb_daus_agg as
(
select 
dte
,platform
,count(distinct device_token_s) as daus
,count(distinct(case when fb_con_flag = 1 then device_token_s else null end)) as fb_daus
from maggie.dice_fb_count_daus
group by 1,2
)
;
create table
maggie.dice_fb_daus_percent as
(select 
dte
,platform
,case when (daus = 0 or daus is null) then fb_daus::float8/(1) else fb_daus::float8 / daus::float8 end as daus_percent
,daus
,fb_daus
from maggie.dice_fb_daus_agg
)
;
select * from maggie.dice_fb_daus_percent

;
--disco bees
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
create table maggie.bees_fb_con as
(select
a.device_token_s
,a.connect_date as dte
,1 as fb_flg
from
(
select
device_token_s
,min(date(time_t)) as connect_date
from titan.disco_bees$sys_connect
where 1=1
--and time_t >= date(getdate()-90)
--and time_t < date(getdate()+1)
and date(time_t) <= '2015-01-11' 
--and date(time_t) between '2014-10-01' and '2015-01-11'  
and _connection_s = 'Facebook'
group by 1
)a
left join data_mart.exception_cheaters b
on a.device_token_s = b.device_token
where b.device_token is null
group by 1,2,3
)
;
create table maggie.bees_fb_count_daus as
(select
bees_daus.device_token_s
,bees_daus.dte
,bees_daus.platform
,max(nvl(bees_fb_con.fb_flg,0)) as fb_con_flag
from maggie.bees_daus bees_daus
left join maggie.bees_fb_con bees_fb_con
on bees_daus.device_token_s = bees_fb_con.device_token_s
and bees_daus.dte >= bees_fb_con.dte
group by 1,2,3)
;
create table maggie.bees_fb_daus_agg as
(
select 
dte
,platform
,count(distinct device_token_s) as daus
,count(distinct(case when fb_con_flag = 1 then device_token_s else null end)) as fb_daus
from maggie.bees_fb_count_daus
group by 1,2
)
;
create table maggie.bees_fb_daus_percent as
(select 
dte
,platform
,case when (daus = 0 or daus is null) then fb_daus::float8/(1) else fb_daus::float8 / daus::float8 end as daus_percent
,daus
,fb_daus
from maggie.bees_fb_daus_agg
)
;
select * from maggie.bees_fb_daus_percent
