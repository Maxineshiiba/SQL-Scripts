 
select spend_date, (sum (spend))/(sum(installs)) as cost_per_install from ( 
select * from wwalker.spend_placeholderv2 t where channel_organic_paid ilike 'paid' and t.game_name ilike 'Dice' and t.platform ilike 'iOS'
) where installs > 0 group by 1

------------------------------------------------------------------------------------------------
(select 
a.install_date
,count(distinct a.device_token) as installs
,count(distinct b.device_token_s) as organics
,count(distinct b.device_token_s)::float /count(distinct a.device_token)::float as percentage_organics
from data_mart.dim_user a
left join 
(select * from (select *, row_number() over (partition by event_id_s order by time_n) as rank1  from
titan.disco_bees$sys_payment  
WHERE _success_b = 'true'
AND _error_s IS NULL
AND _store_sku_s not ilike '%scratchers%'
AND _store_sku_s not ilike '%removeads%'
AND _store_sku_s not ilike '%resetstat%'
and device_token_s not in (select device_token from data_mart.exception_cheaters group by 1)
) where rank1 = 1) b
on a.device_token = b.device_token_s
and a.publisher_id = 1309   ---Unmatched corresponding to 1309 
group by 1
)

-------------------------------------------------------------------------------------------------------
select count(distinct _email_s),count(distinct _email_1_s),count(distinct _sys_email_s)
from titan.dice_prod$sys_app_open
where time_t >= '2015-01-26'
and time_t < '2015-02-27'
---------------------------------------------------------------------------------------------------------
