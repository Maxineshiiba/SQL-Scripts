DELETE FROM maggie.staging_dice_turns WHERE dte >= (current_date::date - 7);

INSERT INTO maggie.staging_dice_turns (   
select 
device_token_s
,time_t::date as dte
,sum(_bonus_rolls_n) as total_bonus_roll_used
,count(*) as total_turns
,sum(case when _game_type_s ilike 'tournament' then 1 else 0 end) as tournament_turns
,sum(case when _game_type_s ilike 'pvp' then 1 else 0 end) as pvp_turns
,sum( case when _game_type_s ilike 'tournament' then _bonus_rolls_n else null end)  as tournament_bonus_rolls_used
,sum(case when _game_type_s ilike 'pvp' then _bonus_rolls_n else null end) as pvp_bonus_rolls_used 
from titan.dice_prod$turn_end 
WHERE time_t::date >= (current_date::date - 7)
group by 1,2);

----------------------------------------------------------------------------------------
delete from maggie.staging_base_churn

insert into maggie.staging_base_churn(       
  SELECT
    user_id,
    device_token,
    platform,
    game_id,
    '{{date}}' ::date AS base_date,
    days_since_install,
    last_play_date,
    F7,
    L7,
    L8,
    L9,
    L10,
    L11,
    L12,
    L13,
    L14,
    L15,
    L16,
    L17,
    L18,
    L19,
    L20,
    L21,
    L24,
    L25,
    L29,
    L30,
    N7,
    N8,
    N9,
    N10,
    N11,
    N12,
    N13,
    N14,
    N15,
    N16,
    N17,
    N18,
    N19,
    N20,
    N21,
    N24,
    N25,
    N29
  FROM (
    SELECT
      CASE WHEN platform_id = 19 THEN 'ios'
      WHEN platform_id = 26 THEN 'android'
      ELSE 'unknown'
      END                             AS platform,
      t1.device_token,
      t1.user_id,
      t1.game_id,
      t1.days_since_install,
      max(time_t :: DATE)             AS last_play_date,
      (count(DISTINCT
                       CASE WHEN sa.time_t <= t1.install_cohort + 7 AND sa.time_t >= t1.install_cohort THEN DATE(
                           sa.time_t)
                       ELSE NULL END)) AS F7, --as first_7_days_played,
      (count(DISTINCT
                       CASE WHEN sa.time_t >= '{{date}}' - 7 AND sa.time_t < '{{date}}' THEN DATE(sa.time_t)
                       ELSE NULL END)) AS L7,  -- as last_7_days_played,
      (count(DISTINCT
                       CASE WHEN sa.time_t >= '{{date}}' - 8 AND sa.time_t < '{{date}}' THEN DATE(sa.time_t)
                       ELSE NULL END)) AS L8,  -- as last_8_days_played,
      (count(DISTINCT
                       CASE WHEN sa.time_t >= '{{date}}' - 9 AND sa.time_t < '{{date}}' THEN DATE(sa.time_t)
                       ELSE NULL END)) AS L9,  --as last_10_days_played,
      (count(DISTINCT
                       CASE WHEN sa.time_t >= '{{date}}' - 10 AND sa.time_t < '{{date}}' THEN DATE(sa.time_t)
                       ELSE NULL END))AS L10,  --as last_10_days_played,
      (count(DISTINCT
                       CASE WHEN sa.time_t >= '{{date}}' - 11 AND sa.time_t < '{{date}}' THEN DATE(sa.time_t)
                       ELSE NULL END))AS L11, --as next_11_days_played,
      (count(DISTINCT
                       CASE WHEN sa.time_t >= '{{date}}' - 12 AND sa.time_t < '{{date}}' THEN DATE(sa.time_t)
                       ELSE NULL END))AS L12,  --as last_10_days_played,
      (count(DISTINCT
                       CASE WHEN sa.time_t >= '{{date}}' - 13 AND sa.time_t < '{{date}}' THEN DATE(sa.time_t)
                       ELSE NULL END))AS L13,  --as last_10_days_played,
      (count(DISTINCT
                       CASE WHEN sa.time_t >= '{{date}}' - 14 AND sa.time_t < '{{date}}' THEN DATE(sa.time_t)
                       ELSE NULL END))AS L14,  --as last_10_days_played,
      (count(DISTINCT
                       CASE WHEN sa.time_t >= '{{date}}' - 15 AND sa.time_t < '{{date}}' THEN DATE(sa.time_t)
                       ELSE NULL END))AS L15,  --as last_10_days_played,
      (count(DISTINCT
                       CASE WHEN sa.time_t >= '{{date}}' - 16 AND sa.time_t < '{{date}}' THEN DATE(sa.time_t)
                       ELSE NULL END))AS L16,  --as last_10_days_played,
      (count(DISTINCT
                       CASE WHEN sa.time_t >= '{{date}}' - 17 AND sa.time_t < '{{date}}' THEN DATE(sa.time_t)
                       ELSE NULL END))AS L17,  --as last_10_days_played,
      (count(DISTINCT
                       CASE WHEN sa.time_t >= '{{date}}' - 18 AND sa.time_t < '{{date}}' THEN DATE(sa.time_t)
                       ELSE NULL END))AS L18,  --as last_10_days_played,
      (count(DISTINCT
                       CASE WHEN sa.time_t >= '{{date}}' - 19 AND sa.time_t < '{{date}}' THEN DATE(sa.time_t)
                       ELSE NULL END))AS L19,  --as last_10_days_played,
      (count(DISTINCT
                       CASE WHEN sa.time_t >= '{{date}}' - 20 AND sa.time_t < '{{date}}' THEN DATE(sa.time_t)
                       ELSE NULL END))AS L20,  --as last_10_days_played,
      (count(DISTINCT
                       CASE WHEN sa.time_t >= '{{date}}' - 21 AND sa.time_t < '{{date}}' THEN DATE(sa.time_t)
                       ELSE NULL END))AS L21,  --as last_10_days_played,
      (count(DISTINCT
                       CASE WHEN sa.time_t >= '{{date}}' - 24 AND sa.time_t < '{{date}}' THEN DATE(sa.time_t)
                       ELSE NULL END))AS L24,  --as last_10_days_played,
      (count(DISTINCT
                       CASE WHEN sa.time_t >= '{{date}}' - 25 AND sa.time_t < '{{date}}' THEN DATE(sa.time_t)
                       ELSE NULL END))AS L25,  --as last_10_days_played,
      (count(DISTINCT
                       CASE WHEN sa.time_t >= '{{date}}' - 29 AND sa.time_t < '{{date}}' THEN DATE(sa.time_t)
                       ELSE NULL END))AS L29, --as next_11_days_played,
      (count(DISTINCT
                       CASE WHEN sa.time_t >= '{{date}}' - 30 AND sa.time_t < '{{date}}' THEN DATE(sa.time_t)
                       ELSE NULL END))AS L30, --as next_11_days_played,
      (count(DISTINCT
                       CASE WHEN sa.time_t <= '{{date}}' + 7 AND sa.time_t > '{{date}}' THEN DATE(sa.time_t)
                       ELSE NULL END))AS N7,--as next_7_days_played,
      (count(DISTINCT
                       CASE WHEN sa.time_t <= '{{date}}' + 8 AND sa.time_t > '{{date}}' THEN DATE(sa.time_t)
                       ELSE NULL END))AS N8, -- as next_8_days_played,
      (count(DISTINCT
                       CASE WHEN sa.time_t <= '{{date}}' + 9 AND sa.time_t > '{{date}}' THEN DATE(sa.time_t)
                       ELSE NULL END))AS N9, -- as next_8_days_played,
      (count(DISTINCT
                       CASE WHEN sa.time_t <= '{{date}}' + 10 AND sa.time_t > '{{date}}' THEN DATE(sa.time_t)
                       ELSE NULL END))AS N10,  --as next_10_days_played,
      (count(DISTINCT
                       CASE WHEN sa.time_t <= '{{date}}' + 11 AND sa.time_t > '{{date}}' THEN DATE(sa.time_t)
                       ELSE NULL END))AS N11,  --as next_11_days_played,
      (count(DISTINCT
                       CASE WHEN sa.time_t <= '{{date}}' + 12 AND sa.time_t > '{{date}}' THEN DATE(sa.time_t)
                       ELSE NULL END))AS N12, -- as next_8_days_played,
      (count(DISTINCT
                       CASE WHEN sa.time_t <= '{{date}}' + 13 AND sa.time_t > '{{date}}' THEN DATE(sa.time_t)
                       ELSE NULL END))AS N13, -- as next_8_days_played,
      (count(DISTINCT
                       CASE WHEN sa.time_t <= '{{date}}' + 14 AND sa.time_t > '{{date}}' THEN DATE(sa.time_t)
                       ELSE NULL END))AS N14, -- as next_8_days_played,
      (count(DISTINCT
                       CASE WHEN sa.time_t <= '{{date}}' + 15 AND sa.time_t > '{{date}}' THEN DATE(sa.time_t)
                       ELSE NULL END))AS N15,  --as next_15_days_played,
      (count(DISTINCT
                       CASE WHEN sa.time_t <= '{{date}}' + 16 AND sa.time_t > '{{date}}' THEN DATE(sa.time_t)
                       ELSE NULL END))AS N16,  --as next_15_days_played,
      (count(DISTINCT
                       CASE WHEN sa.time_t <= '{{date}}' + 17 AND sa.time_t > '{{date}}' THEN DATE(sa.time_t)
                       ELSE NULL END))AS N17,  --as next_15_days_played,
      (count(DISTINCT
                       CASE WHEN sa.time_t <= '{{date}}' + 18 AND sa.time_t > '{{date}}' THEN DATE(sa.time_t)
                       ELSE NULL END))AS N18,  --as next_15_days_played,
      (count(DISTINCT
                       CASE WHEN sa.time_t <= '{{date}}' + 19 AND sa.time_t > '{{date}}' THEN DATE(sa.time_t)
                       ELSE NULL END))AS N19,  --as next_15_days_played,
      (count(DISTINCT
                       CASE WHEN sa.time_t <= '{{date}}' + 20 AND sa.time_t > '{{date}}' THEN DATE(sa.time_t)
                       ELSE NULL END))AS N20,  --as next_15_days_played,
      (count(DISTINCT
                       CASE WHEN sa.time_t <= '{{date}}' + 21 AND sa.time_t > '{{date}}' THEN DATE(sa.time_t)
                       ELSE NULL END))AS N21,  --as next_15_days_played,
      (count(DISTINCT
                       CASE WHEN sa.time_t <= '{{date}}' + 24 AND sa.time_t > '{{date}}' THEN DATE(sa.time_t)
                       ELSE NULL END))AS N24,  --as next_15_days_played,
      (count(DISTINCT
                       CASE WHEN sa.time_t <= '{{date}}' + 25 AND sa.time_t > '{{date}}' THEN DATE(sa.time_t)
                       ELSE NULL END))AS N25,  --as next_15_days_played,
      (count(DISTINCT
                       CASE WHEN sa.time_t <= '{{date}}' + 29 AND sa.time_t > '{{date}}' THEN DATE(sa.time_t)
                       ELSE NULL END))AS N29 --as next_15_days_played
    FROM data_mart.fact_titan_user_summary t1  

    LEFT JOIN
      (
        SELECT *
        FROM
          (
            SELECT
              _sys_device_token_s,
              time_t,
              row_number()
              OVER (PARTITION BY event_id_s
                ORDER BY time_n) AS rank
            FROM titan.dice_prod$sys_app_open
            WHERE time_t >= '2014-11-11'
          )
        WHERE rank = 1
      ) sa
        ON t1.device_token = sa._sys_device_token_s  
     WHERE t1.game_id = 0 and install_cohort < = '{{date}}'
    GROUP BY 1, 2, 3, 4, 5  
  )   
) 

-------------------------------------------------------------------------------------------------
delete from maggie.churn_gross_revenue

insert into maggie.churn_gross_revenue (    
select
t1.user_id
,t1.device_token
,'{{date}}'::date as target_date
,case when t1.install_cohort::date <= '{{date}}' then t1.install_cohort::date else null end as install_cohort
,t2.channel_name  
,t4.publisher_name
,t1.gender
,t1.country
,t1.device_model
,t1.purchases
,t1.first_purchase
,t1.last_purchase   
,t1.days_to_first_purchase    
,t1.days_since_last_purchase  
,t1.is_payer
,t1.pre_titan_user
,('{{date}}' - t1.install_cohort::date)::int days_since_install     
,case when t3.platform_name ilike 'ios' then 'ios'
when t3.platform_name ilike 'android' then 'andorid'
when t3.platform_name ilike 'amazon' then 'amazon'
when t3.platform_name ilike 'facebook' then 'fb'
else 'other'
end as platform

,case when ('{{date}}' - 7 >= t1.install_cohort::date)::int then t1.gross_rev_at_7 else null end as gross_rev_at_7
,case when ('{{date}}' - 30 >= t1.install_cohort::date)::int then t1.gross_rev_at_30 else null end as gross_rev_at_30
,case when ('{{date}}' - 60 >= t1.install_cohort::date)::int then t1.gross_rev_at_60 else null end as gross_rev_at_60
,case when ('{{date}}'- 90 >= t1.install_cohort::date)::int then t1.gross_rev_at_90 else null end as gross_rev_at_90
,case when ('{{date}}' - 180 >= t1.install_cohort::date)::int then t1.gross_rev_at_180 else null end as gross_rev_at_180
,case when ('{{date}}' - 365 >= t1.install_cohort::date)::int then t1.gross_rev_at_365 else null end as gross_rev_at_365
,case when ('{{date}}' - 1095 >= t1.install_cohort::date)::int then t1.gross_rev_at_1095 else null end as gross_rev_at_1095 
,sum(case when time_t <= '{{date}}' then t5._amount_us_n/100 else null end) as gross_revenue  
from data_mart.fact_titan_user_summary t1
left join data_mart.dim_channel t2 on t1.channel_id = t2.channel_id
left join data_mart.dim_platform t3 on t1.platform_id = t3.platform_id
left join data_mart.dim_publisher t4 on t4.publisher_id = t1.publisher_id
left join titan.dice_prod$sys_payment t5 on t1.device_token = t5.device_token_s

where game_id = 0 
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25
)

------------------------------------------------

truncate table maggie.dice_churn

insert into maggie.dice_churn (
select  
t1.user_id
,t1.device_token
,t1.target_date::date
,t1.install_cohort
,t1.channel_name
,t1.publisher_name
,t1.gender
,t1.country
,t1.device_model
,t1.purchases
,t1.first_purchase
,t1.last_purchase
,t1.days_to_first_purchase
,t1.days_since_last_purchase
,t1.is_payer
,t1.pre_titan_user
,t1.days_since_install
,t1.platform
,t1.gross_rev_at_7
,t1.gross_rev_at_30
,t1.gross_rev_at_60
,t1.gross_rev_at_90
,t1.gross_rev_at_180
,t1.gross_rev_at_365
,t1.gross_rev_at_1095
,t1.gross_revenue
,t3.last_play_date
,t3.F7
,t3.L7
,t3.L8
,t3.L9
,t3.L10
,t3.L11
,t3.L12
,t3.L13
,t3.L14
,t3.L15
,t3.L16
,t3.L17
,t3.L18
,t3.L19
,t3.L20
,t3.L21
,t3.L24
,t3.L25
,t3.L29
,t3.L30
,t3.N7
,t3.N8
,t3.N9
,t3.N10
,t3.N11
,t3.N12
,t3.N13
,t3.N14
,t3.N15
,t3.N16
,t3.N17
,t3.N18
,t3.N19
,t3.N20
,t3.N21
,t3.N24
,t3.N25
,t3.N29
,sum(t2.total_bonus_roll_used) as total_bonus_roll_used
,sum(t2.total_turns) as total_turns
,sum(t2.tournament_turns) as tournament_turns
,sum(t2.pvp_turns) as pvp_turns
,sum(t2.tournament_bonus_rolls_used) as tournament_bonus_rolls_used
,sum(t2.pvp_bonus_rolls_used) as pvp_bonus_rolls_used
from maggie.churn_gross_revenue t1
left join
maggie.staging_dice_turns t2 on t1.device_token = t2.device_token_s
left join
maggie.staging_base_churn t3 on t1.device_token = t3.device_token
where t1.install_cohort <= t1.target_date
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,
21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,39,
40,41,42,43,44,45,46,47,48,49,50,51,52,53,54,55,56,57,58,59,
60,61,62,63,64,65 
)
