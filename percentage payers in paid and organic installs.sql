-------------%Payers in Paid and Organic Installs Bees Report
-------------written in Redshift

select
a.channel_name
,a.device_token,
,a.install_date
,max(nvl(is_payer,0)) as is_payer ----nvl means if is_payer is null, then assign it as 0
from 
(select channel_name, a.device_token, a.install_date,b.platform_name,d.game_name
from data_mart.dim_user a
inner join 
data_mart.dim_platform b
on a.platform_id = b.platform_id
inner join data_mart.dim_channel c
on a.channel_id = c.channel_id
inner join 
data_mart.dim_game d
on a.game_id = d.game_id
and d.game_name = 'discobees'
group by 1,2,3,4,5
) a
left join 
(select device_token_s, date(time_t) as revdate, 1 as is_payer
from titan.disco_bees$sys_payment
where time_t< (date(getdate())+1)
and _success_b = 'true'
and _error_s IS NULL
group by 1,2,3
) b
on a.device_token = b.device_token_s
and b.revdate >= a.install_date
group by 1,2,3
