
select * from data_mart.dim_platform limit 5
select * from data_mart.fact_titan_user_summary limit 5
select distinct date(time_t) from titan.dice_prod$sys_app_open limit 5

select * from maggie.base_churn limit 100

select count(distinct user_id) as churn from maggie.base_churn where L7 = 0 and L8 = 1
 

---------------------------------------------------------------------------------------------------
select count(*)as churn, sum(N8)as N8, 1 - sum(N8)/count(*)::float8 as N8churn, sum(N9)as N9, 1- sum(N9)/count(*)::float8 as N9churn
from maggie.base_churn
where L7 = 0 and L8 = 1

select churn, N8, 1 - N8/churn::float8 as N8Churn, N9, 1 - N9/churn::float8 as N9Churn,
N10, 1-N10/churn::float8 as N10Churn, N11, 1-N11/churn::float8 as N11Churn, 
N12, 1-N12/churn::float8 as N12Churn, N13, 1-N13/churn::float8 as N13Churn,
N14, 1-N14/churn::float8 as N14Churn, N15, 1-N15/churn::float8 as N15Churn,
N29, 1-N29/churn::float8 as N29Churn
from(
select sum(N8) as N8, sum(N9) as N9, sum(N10) as N10, sum(N11) as N11 ,sum(N12) as N12, sum(N13) as N13, sum(N14) as N14, sum(N15) as N15, sum(N29) as N29,
count(case when (L7 = 0 and L8 = 1) then 1 else null end) as churn
from maggie.base_churn
where L7 = 0 and L8 = 1
)


---------------------------------------------------------------
drop table maggie.churn_high_level

select * from maggie.churn_high_level limit 100

create table churn_high_level as
(
select base_date
,sum(churnL78) as ChurnL78
,sum(N8) as N8, 1 -  sum(N8)/sum(churnL78)::float8 as N8Churn
,sum(N9) as N9, 1 - sum(N9)/sum(churnL78)::float8 as N9Churn
,sum(N10) as N10, 1 - sum(N10)/sum(churnL78)::float8 as N10Churn
,sum(N11) as N11, 1 - sum(N11)/sum(churnL78)::float8 as N11Churn
,sum(N12) as N12, 1 - sum(N12)/sum(churnL78)::float8 as N12Churn
,sum(N13) as N13, 1 - sum(N13)/sum(churnL78)::float8 as N13Churn
,sum(N14) as N14, 1 - sum(N14)/sum(churnL78)::float8 as N14Churn
,sum(N15) as N15, 1 - sum(N15)/sum(churnL78)::float8 as N15Churn
,sum(N16) as N16, 1 - sum(N16)/sum(churnL78)::float8 as N16Churn
,sum(N17) as N17, 1 - sum(N17)/sum(churnL78)::float8 as N17Churn
,sum(N18) as N18, 1 - sum(N18)/sum(churnL78)::float8 as N18Churn
,sum(N19) as N19, 1 - sum(N19)/sum(churnL78)::float8 as N19Churn
,sum(N20) as N20, 1 - sum(N20)/sum(churnL78)::float8 as N20Churn
,sum(N24) as N24, 1 - sum(N24)/sum(churnL78)::float8 as N24Churn
,sum(N29) as N29, 1 - sum(N29)/sum(churnL78)::float8 as N29Churn 
from maggie.churn_low_level
group by 1
)


select * from maggie.churn_high_level


---------------------------------------------------------------
select * from maggie.churn_low_level limit 100
drop table maggie.churn_low_level

create table churn_low_level as(
select user_id
,device_token
,country
,platform
,game_id
,install_cohort
,base_date
,days_since_install
,last_play_date
,days_played
,N7, N8, N9, N10, N11
,N12,N13, N14, N15
,N16, N17, N18
,N19, N20, N21
,N24, N25, N29
,count(case when (L7 = 0 and L8 = 1) then 1 else null end) as churnL78
from maggie.base_churn
where L7 = 0 and L8 = 1
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28
)


---------------------------------------------------
drop table maggie.base_churn
create table base_churn as(
select user_id
,device_token
,country
,platform
,game_id
,install_cohort
,'2015-01-01' as base_date
,days_since_install
,last_play_date
,days_played
,L7,L8,L9,L10,L11,L12,L13,L14,L15,L16,L17,L18,L19,L20,L21,L24,L25,L29,L30
,N7,N8,N9,N10,N11,N12,N13,N14,N15,N16,N17,N18,N19,N20,N21,N24,N25,N29
from (
select 
case when platform_id = 19 then 'ios'
when platform_id = 26 then 'android'
else 'unknown'
end as platform
,ua.device_token
,ua.country
,ua.user_id
,ua.game_id
,ua.install_cohort
,ua.days_since_install
,max(time_t:: DATE) as last_play_date
,count(distinct DATE(sa.time_t)) as days_played
,case when (count(distinct 
       case when sa.time_t <= ua.install_cohort + 7 and sa.time_t >= ua.install_cohort then DATE(sa.time_t)
       else null end)) > 0 then 1 else 0 end as F7 --as first_7_days_played
,case when (count(distinct
       case when sa.time_t >= '2015-01-01' -7 and sa.time_t < '2015-01-01' then DATE(sa.time_t) else null end)) > 0 then count(distinct DATE(sa.time_t))  end as L7  -- as last_7_days_played
,case when (count(distinct
        case when sa.time_t >= '2015-01-01' - 8 and sa.time_t < '2015-01-01' then DATE(sa.time_t)else null end)) > 0 then count(distinct DATE(sa.time_t)) end as L8  -- as last_8_days_played
,case when (count(distinct
        case when sa.time_t >= '2015-01-01' -9 and sa.time_t < '2015-01-01' then DATE(sa.time_t) else null end)) >0 then count(distinct DATE(sa.time_t)) end as L9  --as last_10_days_played
,case when (count(distinct
        case when sa.time_t >= '2015-01-01' -10  and sa.time_t < '2015-01-01' then DATE(sa.time_t) else null end)) >0 then count(distinct DATE(sa.time_t)) end as L10  --as last_10_days_played
,case when (count(distinct
        case when sa.time_t >= '2015-01-01' -11 and sa.time_t < '2015-01-01' then DATE(sa.time_t) else null end)) > 0 then count(distinct DATE(sa.time_t)) end as L11 --as next_11_days_played
,case when (count(distinct
        case when sa.time_t >= '2015-01-01' -12 and sa.time_t < '2015-01-01' then DATE(sa.time_t) else null end)) >0 then count(distinct DATE(sa.time_t))  end as L12  --as last_10_days_played
,case when (count(distinct
        case when sa.time_t >= '2015-01-01' -13 and sa.time_t < '2015-01-01' then DATE(sa.time_t) else null end)) >0 then count(distinct DATE(sa.time_t))  end as L13  --as last_10_days_played
,case when (count(distinct
        case when sa.time_t >= '2015-01-01' -14 and sa.time_t < '2015-01-01' then DATE(sa.time_t) else null end)) >0 then count(distinct DATE(sa.time_t))  end as L14  --as last_10_days_played
,case when (count(distinct
        case when sa.time_t >= '2015-01-01' -15  and sa.time_t < '2015-01-01' then DATE(sa.time_t) else null end)) >0 then count(distinct DATE(sa.time_t))  end as L15  --as last_10_days_played
,case when (count(distinct
        case when sa.time_t >= '2015-01-01' -16  and sa.time_t < '2015-01-01' then DATE(sa.time_t) else null end)) >0 then count(distinct DATE(sa.time_t))  end as L16  --as last_10_days_played
,case when (count(distinct
        case when sa.time_t >= '2015-01-01' -17  and sa.time_t < '2015-01-01' then DATE(sa.time_t) else null end)) >0 then count(distinct DATE(sa.time_t))  end as L17  --as last_10_days_played
,case when (count(distinct
        case when sa.time_t >= '2015-01-01' -18  and sa.time_t < '2015-01-01' then DATE(sa.time_t) else null end)) >0 then count(distinct DATE(sa.time_t))  end as L18  --as last_10_days_played
,case when (count(distinct
        case when sa.time_t >= '2015-01-01' -19  and sa.time_t < '2015-01-01' then DATE(sa.time_t) else null end)) >0 then count(distinct DATE(sa.time_t))  end as L19  --as last_10_days_played
,case when (count(distinct
        case when sa.time_t >= '2015-01-01' -20  and sa.time_t < '2015-01-01' then DATE(sa.time_t) else null end)) >0 then count(distinct DATE(sa.time_t))  end as L20  --as last_10_days_played
,case when (count(distinct
        case when sa.time_t >= '2015-01-01' -21  and sa.time_t < '2015-01-01' then DATE(sa.time_t) else null end)) >0 then count(distinct DATE(sa.time_t)) end as L21  --as last_10_days_played
,case when (count(distinct
        case when sa.time_t >= '2015-01-01' -24  and sa.time_t < '2015-01-01' then DATE(sa.time_t) else null end)) >0 then count(distinct DATE(sa.time_t))  end as L24  --as last_10_days_played
,case when (count(distinct
        case when sa.time_t >= '2015-01-01' -25  and sa.time_t < '2015-01-01' then DATE(sa.time_t) else null end)) >0 then count(distinct DATE(sa.time_t))  end as L25  --as last_10_days_played
,case when (count(distinct
        case when sa.time_t >= '2015-01-01' -29 and sa.time_t < '2015-01-01' then DATE(sa.time_t) else null end)) > 0 then count(distinct DATE(sa.time_t))  end as L29 --as next_11_days_played
,case when (count(distinct
        case when sa.time_t >= '2015-01-01' -30 and sa.time_t < '2015-01-01' then DATE(sa.time_t) else null end)) > 0 then count(distinct DATE(sa.time_t))  end as L30 --as next_11_days_played
,case when (count(distinct
       case when sa.time_t <= '2015-01-01' + 7 and sa.time_t > '2015-01-01' then DATE(sa.time_t)else null end)) > 0 then count(distinct DATE(sa.time_t))   end as N7--as next_7_days_played
,case when (count(distinct 
       case when sa.time_t <= '2015-01-01' + 8 and sa.time_t > '2015-01-01' then DATE(sa.time_t) else null end)) > 0 then count(distinct DATE(sa.time_t))   end as N8 -- as next_8_days_played 
,case when (count(distinct 
       case when sa.time_t <= '2015-01-01' + 9 and sa.time_t > '2015-01-01' then DATE(sa.time_t) else null end)) > 0 then count(distinct DATE(sa.time_t))  end as N9 -- as next_8_days_played
,case when (count(distinct
       case when sa.time_t <= '2015-01-01' + 10 and sa.time_t > '2015-01-01' then DATE(sa.time_t) else null end)) >0 then count(distinct DATE(sa.time_t))  end as N10  --as next_10_days_played
,case when (count(distinct
       case when sa.time_t <= '2015-01-01' + 11 and sa.time_t > '2015-01-01' then DATE(sa.time_t) else null end)) > 0 then count(distinct DATE(sa.time_t))  end as N11  --as next_11_days_played
,case when (count(distinct 
       case when sa.time_t <= '2015-01-01' + 12 and sa.time_t > '2015-01-01' then DATE(sa.time_t) else null end)) > 0 then count(distinct DATE(sa.time_t))  end as N12 -- as next_8_days_played
,case when (count(distinct 
       case when sa.time_t <= '2015-01-01' + 13 and sa.time_t > '2015-01-01' then DATE(sa.time_t) else null end)) > 0 then count(distinct DATE(sa.time_t)) end as N13 -- as next_8_days_played
,case when (count(distinct 
       case when sa.time_t <= '2015-01-01' + 14 and sa.time_t > '2015-01-01' then DATE(sa.time_t) else null end)) > 0 then count(distinct DATE(sa.time_t))  end as N14 -- as next_8_days_played
,case when (count(distinct
       case  when sa.time_t <= '2015-01-01' + 15  and sa.time_t > '2015-01-01' then DATE(sa.time_t) else null end)) > 0 then count(distinct DATE(sa.time_t))  end as N15  --as next_15_days_played
,case when (count(distinct
       case  when sa.time_t <= '2015-01-01' + 16  and sa.time_t > '2015-01-01' then DATE(sa.time_t) else null end)) > 0 then count(distinct DATE(sa.time_t))  end as N16  --as next_15_days_played
,case when (count(distinct
       case  when sa.time_t <= '2015-01-01' + 17  and sa.time_t > '2015-01-01' then DATE(sa.time_t) else null end)) > 0 then count(distinct DATE(sa.time_t)) end as N17  --as next_15_days_played
,case when (count(distinct
       case  when sa.time_t <= '2015-01-01' + 18  and sa.time_t > '2015-01-01' then DATE(sa.time_t) else null end)) > 0 then count(distinct DATE(sa.time_t))  end as N18  --as next_15_days_played
,case when (count(distinct
       case  when sa.time_t <= '2015-01-01' + 19  and sa.time_t > '2015-01-01' then DATE(sa.time_t) else null end)) > 0 then 1 else 0 end as N19  --as next_15_days_played
,case when (count(distinct
       case  when sa.time_t <= '2015-01-01' + 20  and sa.time_t > '2015-01-01' then DATE(sa.time_t) else null end)) > 0 then 1 else 0 end as N20  --as next_15_days_played
,case when (count(distinct
       case  when sa.time_t <= '2015-01-01' + 21  and sa.time_t > '2015-01-01' then DATE(sa.time_t) else null end)) > 0 then 1 else 0 end as N21  --as next_15_days_played
,case when (count(distinct
       case  when sa.time_t <= '2015-01-01' + 24  and sa.time_t > '2015-01-01' then DATE(sa.time_t) else null end)) > 0 then 1 else 0 end as N24  --as next_15_days_played
,case when (count(distinct
       case  when sa.time_t <= '2015-01-01' + 25  and sa.time_t > '2015-01-01' then DATE(sa.time_t) else null end)) > 0 then 1 else 0 end as N25  --as next_15_days_played
,case when (count(distinct
       case  when sa.time_t <= '2015-01-01' + 29  and sa.time_t > '2015-01-01' then DATE(sa.time_t) else null end)) > 0 then 1 else 0 end as N29  --as next_15_days_played              
from data_mart.fact_titan_user_summary ua
left join
(
select * from 
(
select 
_sys_device_token_s
,time_t
,row_number()
over(partition by event_id_s order by time_n) as rank
from titan.dice_prod$sys_app_open
where time_t >= '2014-11-11'
)
where rank = 1
) sa
on ua.device_token = sa._sys_device_token_s
where ua.game_id = 0 
group by 1,2,3,4,5,6,7
) 
)
