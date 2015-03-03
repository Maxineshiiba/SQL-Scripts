create table  t1 as(     -------exchange rate table
select
base_currency
,currency_code
,date(datetime) as dte
,avg(rate) as exr --exchange rate
from revenue.openexchangerates
where date(datetime) >= '2014-12-20'
group by 1,2,3
);

create table t2p as(    ----base table
select
case
when sku ilike '%bees%' then 'Disco Bees'
when sku ilike '%slots%' then 'Slots'
when sku ilike '%dice%' then 'Dice'
when sku ilike '%golf%' then 'Golf'
when sku ilike '%wordly%' then 'Wordly'
when sku ilike '%bubble%' then 'Bubbles'
when sku ilike '%farkle%' then 'Farkle'
when sku ilike '%wheel%' then 'WOF'
when sku ilike '%skeeball%' then 'Skeeball'
when sku ilike '%wordwars%' then 'Word Wars'
when sku ilike '%jewel%' then 'Jewels'
when sku ilike '%barrel%' then 'Dice'
when sku ilike '%basket%' then 'Dice'
when sku ilike '%dumptruck%' then 'Dice'
when sku ilike '%handful%' then 'Dice'
when sku ilike '%pile%' then 'Dice'
when sku ilike '%scoop%' then 'Dice'
when sku ilike '%taste%' then 'Dice'
when sku ilike '%Skee Ball%' then 'Skeeball'

when title ilike '%bees%' then 'Disco Bees'
when title ilike '%slots%' then 'Slots'
when title ilike '%dice%' then 'Dice'
when title ilike '%golf%' then 'Golf'
when title ilike '%wordly%' then 'Wordly'
when title ilike '%bubble%' then 'Bubbles'
when title ilike '%farkle%' then 'Farkle'
when title ilike '%wheel%' then 'WOF'
when title ilike '%skeeball%' then 'Skeeball'
when title ilike '%wordwars%' then 'Word Wars'
when title ilike '%jewel%' then 'Jewels'
when title ilike '%barrel%' then 'Dice'
when title ilike '%basket%' then 'Dice'
when title ilike '%dumptruck%' then 'Dice'
when title ilike '%handful%' then 'Dice'
when title ilike '%pile%' then 'Dice'
when title ilike '%scoop%' then 'Dice'
when title ilike '%taste%' then 'Dice'
when title ilike '%Skee Ball%' then 'Skeeball'

when parent_identifier ilike '%bees%' then 'Disco Bees'
when parent_identifier ilike '%slots%' then 'Slots'
when parent_identifier ilike '%dice%' then 'Dice'
when parent_identifier ilike '%golf%' then 'Golf'
when parent_identifier ilike '%wordly%' then 'Wordly'
when parent_identifier ilike '%bubble%' then 'Bubbles'
when parent_identifier ilike '%farkle%' then 'Farkle'
when parent_identifier ilike '%wheel%' then 'WOF'
when parent_identifier ilike '%skeeball%' then 'Skeeball'
when parent_identifier ilike '%wordwars%' then 'Word Wars'
when parent_identifier ilike '%jewel%' then 'Jewels'
when parent_identifier ilike '%barrel%' then 'Dice'
when parent_identifier ilike '%basket%' then 'Dice'
when parent_identifier ilike '%dumptruck%' then 'Dice'
when parent_identifier ilike '%handful%' then 'Dice'
when parent_identifier ilike '%pile%' then 'Dice'
when parent_identifier ilike '%scoop%' then 'Dice'
when parent_identifier ilike '%taste%' then 'Dice'
when parent_identifier ilike '%Skee Ball%' then 'Skeeball'


else 'unknown'
end as game_name
,date(begin_date) as dte
,provider
,provider_code
,sku
,developer
,title
,product_type
,units
,developer_proceeds
,date(end_date) as end_dte
,customer_currency
,country_code
,proceed_currency
,apple_id
,customer_price
,parent_identifier
,promo_code
,sum((developer_proceeds/0.7)*units) as rev   ---adjust revenue to net revenue
from revenue.itunes
where 1=1
and begin_date = '2015-02-28'
and units > 0
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18
);

create table t3p as(     ---- identify local currencies which need to be converted
select
a.game_name
,a.dte
,a.rev
,b.exr
,a.provider
,a.provider_code
,a.sku
,a.developer
,a.title
,a.product_type
,a.units
,a.developer_proceeds
,a.end_dte
,a.customer_currency
,a.country_code
,a.proceed_currency
,a.apple_id
,a.customer_price
,a.parent_identifier
,a.promo_code
from maggie.t2p a
inner join maggie.t1 b
on a.proceed_currency = b.currency_code
and date_trunc('day', b.dte) = date_trunc('month', a.dte)
);

create table t4p as(        -----------------convert local currency to usd
select
game_name
,dte
,proceed_currency
,provider
,provider_code
,sku
,developer
,title
,product_type
,units
,developer_proceeds
,end_dte
,customer_currency
,country_code
,apple_id
,customer_price
,parent_identifier
,promo_code
,case when proceed_currency = 'USD' then rev
else (rev::float8/exr::float8) end as rev_USD
from maggie.t3p)

