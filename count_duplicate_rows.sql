select event_id_s, count(distinct event_id_s) as rec
from titan.headshot$game_transaction
group by 1
having count(distinct event_id_s) >1
