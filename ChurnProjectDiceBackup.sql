newly created DICE user summary
-- combined the steps
-- has_offers.events_abridged -> staging.stg_dice_ho_users
-- rds_user2.user_games & rds_dice3.device -> staging.stg_dice_derived_users
-- update staging.stg_dice_ho_users set publisher to facebook if user is from
-- titan data mart with tracker name facebook
-- combine the results into staging.stg_dice_users
-- update if publisher is unknown with publisher from HO.install_install_tracking_abridged
--
--  Source Tables: has_offers.events_abridged, titan_data_marts.titan_fact_dice_installs
--             rds_user2.usergames, rds_dice3.device, models.store_enum
--             dynamodb.facebookrequest, data_mart.user_mapping
--             models.btyd_user_projections, models.dice_iap_with_prices
--            centipede.dice_turn
--

truncate table staging.stg_dice_ho_users;


  insert into staging.stg_dice_ho_users
    (user_id, install_cohort, country, publisher, publisher_sub_campaign,
      device_model, dt_created, platform, rank1)
      select user_id, install_cohort, country, publisher, publisher_sub_campaign,
      device_model, dt_created, platform, rank1
      from (
    select user_id,  min(install_date) install_cohort, nvl(max(country), 'unknown') as country,
        nvl(max(publisher), 'unknown') as publisher, max(publisher_sub_campaign) as publisher_sub_campaign,
        max(device_model) as device_model, sysdate dt_created,
        case when site ilike '%ios%' then  'i'
        when site ilike '%google%' then  'a'
        when site ilike '%amazon%' then  'z'
        else null end as platform,
        row_number() over (partition by user_id order by created) as rank1
    FROM has_offers.events_abridged
    WHERE site ILIKE '%dice%' AND user_id is not null
    group by user_id, created, site
      )
      where rank1  = 1;


    -- update publisher to FB for users in titan data marts with tracker "FB"
    update staging.stg_dice_ho_users
      set publisher = 'Facebook'
      where user_id in (SELECT user_id
        --FROM titan_data_marts.titan_fact_dice_installs
          From matt77adjust.raw_events
          WHERE tracker_name ilike '%facebook%'
                    and app_name ilike '%dice%'

      );


  --truncate table staging.stg_dice_derived_users;

  -- insert new users from rds_user2.usergames and rds_dice3.device

 insert into staging.stg_dice_derived_users (user_id, derived_install_cohort, platform, device_model,
   facebook_user_id, store)
  select a.user_id, min(a.added_date) as derived_install_cohort,
     max(CASE WHEN t1.os = 1 THEN 'i'
       when t1.os = 13 THEN 'i'
       WHEN t1.os = 2 THEN 'a'
       WHEN t1.os = 17 THEN 'fb'
       WHEN t1.os = 11 then 'w'
       ELSE 'other' END) AS platform, max(device_model) as device_model,
       max(facebook_user_id) facebook_user_id,
       max(t3.store) as store
   from rds_user2.usergames a
     left join rds_dice3.device t1 on (a.user_id = t1.user_id)
       left join models.store_enum t3 on t1.store = t3.enum
  where a.user_id not in (select user_id from staging.stg_dice_derived_users)
  and a.game_id = 0
   group by a.user_id;


--alter table staging.stg_dice_users add store varchar(255)
-- join the two tables into stg_dice_users

  truncate  table staging.stg_dice_users;

    -- insert new user records from stg_dice_derived_users
    insert into staging.stg_dice_users
      ( user_id,
        install_cohort,
        derived_install_date,
        platform,
        country,
        publisher,
        device_model,
        store,
        facebook_user_id,
        source_table
      )
      SELECT
      user_id::int
      , derived_install_cohort::date AS install_cohort
      , 1 as derived_install_cohort
      , platform as platform
      , 'unknown' AS country
      , 'organic' as Publisher
      , device_model
      , store
      , facebook_user_id
      , 'dice_rds' as source_table
      from staging.stg_dice_derived_users
      where user_id not in (select user_id
        from staging.stg_dice_ho_users
        WHERE LEN(user_id) BETWEEN 1 AND 8)
        and LEN(user_id) BETWEEN 1 AND 8;




    insert into staging.stg_dice_users
        ( user_id,
        install_cohort,
        derived_install_date,
        platform,
        country,
        publisher,
        publisher_sub_campaign,
        bid_type,
        creative,
        device,
        geo,
        interest,
        gender,
        copy,
        min_age,
        max_age,
        other,
        device_model,
        store,
        facebook_user_id,
        source_table
        )
    SELECT
        t1.user_id::int
      , CASE WHEN (t1.install_cohort IS NOT NULL AND t1.install_cohort <= t2.derived_install_cohort) THEN t1.install_cohort::date
         WHEN t2.derived_install_cohort IS NOT NULL THEN t2.derived_install_cohort::date
         ELSE NULL END AS install_cohort
      , CASE WHEN t1.install_cohort IS NOT NULL THEN 0
          WHEN t1.install_cohort IS NULL AND t2.derived_install_cohort IS NOT NULL THEN 1
            ELSE NULL END AS derived_install_date
      , CASE WHEN t1.platform is not null then t1.platform else t2.platform end as platform
      , CASE WHEN country IS NOT NULL THEN country ELSE 'unknown' END AS country
      , CASE
          --WHEN t4.user_id IS NOT NULL AND (publisher = 'organic' or publisher IS NULL) THEN 'Dice Aid (organic)'
          WHEN publisher IS NULL THEN 'unknown'
          WHEN publisher = 'Web | TMZ.com' THEN 'Kixer'
          ELSE publisher
        END AS publisher
      , publisher_sub_campaign
      ,(case position('-b' in t1.publisher_sub_campaign) when 0 then null else left(substring(t1.publisher_sub_campaign from position('-b' in t1.publisher_sub_campaign)+2), -1+(case position('-' in substring(t1.publisher_sub_campaign from position('-b' in t1.publisher_sub_campaign)+2)) when 0 then length(1+substring(t1.publisher_sub_campaign from position('-b' in t1.publisher_sub_campaign)+2)) else position('-' in substring(t1.publisher_sub_campaign from position('-b' in t1.publisher_sub_campaign)+2)) end)) end)
      as bid_type
      ,(case position('-c' in t1.publisher_sub_campaign) when 0 then null else left(substring(t1.publisher_sub_campaign from position('-c' in t1.publisher_sub_campaign)+2), -1+(case position('-' in substring(t1.publisher_sub_campaign from position('-c' in t1.publisher_sub_campaign)+2)) when 0 then length(1+substring(t1.publisher_sub_campaign from position('-c' in t1.publisher_sub_campaign)+2)) else position('-' in substring(t1.publisher_sub_campaign from position('-c' in t1.publisher_sub_campaign)+2)) end)) end)
        as creative
      ,(case position('-d' in t1.publisher_sub_campaign) when 0 then null else left(substring(t1.publisher_sub_campaign from position('-d' in t1.publisher_sub_campaign)+2), -1+(case position('-' in substring(t1.publisher_sub_campaign from position('-d' in t1.publisher_sub_campaign)+2)) when 0 then length(1+substring(t1.publisher_sub_campaign from position('-d' in t1.publisher_sub_campaign)+2)) else position('-' in substring(t1.publisher_sub_campaign from position('-d' in t1.publisher_sub_campaign)+2)) end)) end)
        as device
      ,(case position('-g' in t1.publisher_sub_campaign) when 0 then null else left(substring(t1.publisher_sub_campaign from position('-g' in t1.publisher_sub_campaign)+2), -1+(case position('-' in substring(t1.publisher_sub_campaign from position('-g' in t1.publisher_sub_campaign)+2)) when 0 then length(1+substring(t1.publisher_sub_campaign from position('-g' in t1.publisher_sub_campaign)+2)) else position('-' in substring(t1.publisher_sub_campaign from position('-g' in t1.publisher_sub_campaign)+2)) end)) end)
        as geo
      ,(case position('-i' in t1.publisher_sub_campaign) when 0 then null else left(substring(t1.publisher_sub_campaign from position('-i' in t1.publisher_sub_campaign)+2), -1+(case position('-' in substring(t1.publisher_sub_campaign from position('-i' in t1.publisher_sub_campaign)+2)) when 0 then length(1+substring(t1.publisher_sub_campaign from position('-i' in t1.publisher_sub_campaign)+2)) else position('-' in substring(t1.publisher_sub_campaign from position('-i' in t1.publisher_sub_campaign)+2)) end)) end)
        as interest
      ,(case position('-s' in t1.publisher_sub_campaign) when 0 then null else left(substring(t1.publisher_sub_campaign from position('-s' in t1.publisher_sub_campaign)+2), -1+(case position('-' in substring(t1.publisher_sub_campaign from position('-s' in t1.publisher_sub_campaign)+2)) when 0 then length(1+substring(t1.publisher_sub_campaign from position('-s' in t1.publisher_sub_campaign)+2)) else position('-' in substring(t1.publisher_sub_campaign from position('-s' in t1.publisher_sub_campaign)+2)) end)) end)
        as gender
      ,(case position('-t' in t1.publisher_sub_campaign) when 0 then null else left(substring(t1.publisher_sub_campaign from position('-t' in t1.publisher_sub_campaign)+2), -1+(case position('-' in substring(t1.publisher_sub_campaign from position('-t' in t1.publisher_sub_campaign)+2)) when 0 then length(1+substring(t1.publisher_sub_campaign from position('-t' in t1.publisher_sub_campaign)+2)) else position('-' in substring(t1.publisher_sub_campaign from position('-t' in t1.publisher_sub_campaign)+2)) end)) end)
        as "copy"
      ,(case position('-x' in t1.publisher_sub_campaign) when 0 then null else left(substring(t1.publisher_sub_campaign from position('-x' in t1.publisher_sub_campaign)+2), -1+(case position('-' in substring(t1.publisher_sub_campaign from position('-x' in t1.publisher_sub_campaign)+2)) when 0 then length(1+substring(t1.publisher_sub_campaign from position('-x' in t1.publisher_sub_campaign)+2)) else position('-' in substring(t1.publisher_sub_campaign from position('-x' in t1.publisher_sub_campaign)+2)) end)) end)
        as min_age
      ,(case position('-y' in t1.publisher_sub_campaign) when 0 then null else left(substring(t1.publisher_sub_campaign from position('-y' in t1.publisher_sub_campaign)+2), -1+(case position('-' in substring(t1.publisher_sub_campaign from position('-y' in t1.publisher_sub_campaign)+2)) when 0 then length(1+substring(t1.publisher_sub_campaign from position('-y' in t1.publisher_sub_campaign)+2)) else position('-' in substring(t1.publisher_sub_campaign from position('-y' in t1.publisher_sub_campaign)+2)) end)) end)
        as max_age
      ,(case position('-z' in t1.publisher_sub_campaign) when 0 then null else left(substring(t1.publisher_sub_campaign from position('-z' in t1.publisher_sub_campaign)+2), -1+(case position('-' in substring(t1.publisher_sub_campaign from position('-z' in t1.publisher_sub_campaign)+2)) when 0 then length(1+substring(t1.publisher_sub_campaign from position('-z' in t1.publisher_sub_campaign)+2)) else position('-' in substring(t1.publisher_sub_campaign from position('-z' in t1.publisher_sub_campaign)+2)) end)) end)
        as other
      , case when t1.device_model is null then t2.device_model else t1.device_model end as device_model
      , store
      , facebook_user_id
      , 'hasoffers.events_abridged' as source_table
from staging.stg_dice_ho_users t1
        left join staging.stg_dice_derived_users t2 on (t1.user_id = t2.user_id)
WHERE LEN(t1.user_id) BETWEEN 1 AND 8;



  -- update publisher in staging.stg_dice_derived_users
  update staging.stg_dice_users
    set publisher = 'Dice Aid (organic)'
    where publisher = 'organic'
    and facebook_user_id in (select t1.recipientfacebookuserid
      FROM dynamodb.facebookrequest t1
      WHERE game = 'Dice');


  -- update publisher if available in HO.install_tracking_abridged
  update staging.stg_dice_users
  set publisher = (select max(publisher)
                  from has_offers.install_tracking_abridged b
                  WHERE site ILIKE '%dice%' AND user_id is not null
                  and b.user_id = staging.stg_dice_users.user_id
                  and publisher != 'unknown')
   where publisher = 'unknown'
   and platform = 'i';

      -------------------------
    truncate table models.dice_user_channel;

      --  insert the end result to models.dice_user_channel


      insert into models.dice_user_channel
        ( user_id,
          install_cohort,
          derived_install_date,
          platform,
          country,
          publisher,
          publisher_sub_campaign,
          bid_type,
          creative,
          device,
          geo,
          interest,
          gender,
          copy,
          min_age,
          max_age,
          other,
          device_model,
          device_type,
          store,
          device_token,
          source_table
        )
        (
          SELECT
          t1.user_id::int,
          t1.install_cohort,
          t1.derived_install_date,
          case when platform = 'other' and (device_model ilike '%iphone%' or device_model ilike '%ipod%' or device_model ilike '%ipad%' )
          then 'i'
          else platform end as platform,
          country,
          publisher,
          publisher_sub_campaign,
          bid_type,
          creative,
          device,
          geo,
          interest,
          gender,
          copy,
          min_age,
          max_age,
          other,
          device_model,
          case when platform = 'i' and  (device_model ilike '%phone%' or device_model ilike '%pod%')  then 'Phone'
          when (platform = 'i'  and device_model ilike '%pad%') then 'Tablet'
          else  'unknown' end as device_type,
          store,
          device_token,
          source_table
          FROM staging.stg_dice_users t1
            left outer join data_mart.user_mapping t2  on t2.user_id = t1.user_id
          WHERE t2.priority = 1 or t2.priority is null
          ORDER BY t1.install_cohort
        );

commit;

 ------------------------------------------------------------------------------

 ---  source revenue projection from models.btyd_user_projections
 -- select the latest projections for the user

 truncate table staging.stg_dice_rev_projections;


   insert into staging.stg_dice_rev_projections (user_id, p_alive,
      projected_gross_rev_90,projected_gross_rev_180,projected_gross_rev_365,projected_gross_rev_1095)
   select user_id, avg(p_alive), sum(projected_gross_rev_90) projected_gross_rev_90, sum (projected_gross_rev_180) projected_gross_rev_180,
     sum (projected_gross_rev_365) projected_gross_rev_365, sum (projected_gross_rev_1095) projected_gross_rev_1095
   from (
       select user_id, p_alive,
       case when window = 90 then projected_gross_revenue else null end as projected_gross_rev_90,
       case when window = 180 then projected_gross_revenue else null end as projected_gross_rev_180,
       case when window = 365 then projected_gross_revenue else null end as projected_gross_rev_365,
       case when window = 1095 then projected_gross_revenue else null end as projected_gross_rev_1095
     from
       (select user_id, projected_gross_revenue, p_alive,application, window,
         row_number () over (partition by user_id, application, window order by timestamp desc) as rank1
         from models.btyd_user_projections
         where application = 'dice'
       )
       where rank1 =1)
   group by user_id ;



     -----------------------------------------------------

     truncate table staging.stg_dice_revenue;


     insert into staging.stg_dice_revenue (
       user_id,
       first_purchase,
       days_to_first_purchase,
       last_purchase,
       days_since_last_purchase,
       days_since_install,
       is_payer,
       purchases,
       gross_revenue
     )
     (
       select
           t1.user_id
           , min(t2.created)::date as first_purchase
           , min(t2.created)::date - min(install_cohort) as days_to_first_purchase
           , max(t2.created)::date as last_purchase
           , (current_date - max(t2.created)::date)::int as days_since_last_purchase
           , (current_date - install_cohort::date)::int days_since_install
           , case when count(price) >= 1 then 1 else 0 end as is_payer
           , nvl(count(price), 0) as purchases
           , nvl(sum(price), 0) as gross_revenue
       from models.dice_user_channel t1
          left join models.dice_iap_with_prices t2 on t1.user_id::varchar = t2.user_id::varchar
       where LEN(t1.user_id) BETWEEN 1 AND 8
       group by 1, t1.install_cohort
     );



     -- update the dice turn daily table with last 3 days of data

     delete from data_mart.daily_dice_turn
       where date_played >= (current_date::date - 2);

       insert into data_mart.daily_dice_turn
         (date_played,
           user_id,
           platform,
           total_turns,
           pvp_turns,
           tournament_turns,
           pvp_bonus_rolls_used,
           tourney_bonus_rolls_used,
           total_bonus_rolls_used
         )
      select timestamp::date date_played
         , a.user_id
         , max(platform) as platform
         , count(*) as total_turns
         , sum(case when tournament_id = '' then 1 else 0 end) as pvp_turns
         , sum(case when tournament_id != '' then 1 else 0 end) as tournament_turns
         , sum(case when used_bonus_roll IS TRUE AND tournament_id = '' then 1 else 0 end) as pvp_bonus_rolls_used
         , sum(case when used_bonus_roll IS TRUE AND tournament_id != '' then 1 else 0 end) as tourney_bonus_rolls_used
         , sum(case when used_bonus_roll IS TRUE then 1 else 0 end) as total_bonus_rolls_used
       from centipede.dice_turn a
       where timestamp::date >= (current_date::date - 2)
       group by 1,2
       order by timestamp::date;


         -- truncate and rebuild the dice user engagement table
         truncate table models.dice_user_engagement;

         -- delete and insert data for recent users
         insert into models.dice_user_engagement
           (user_id,
             platform,
             total_turns,
             pvp_turns,
             tournament_turns,
             days_played,
             days_since_last_turn,
             pvp_bonus_rolls_used,
             tourney_bonus_rolls_used,
             total_bonus_rolls_used,
             mean_total_turns_per_day,
             mean_pvp_turns_per_day,
             bonus_rolls_per_pvp_turn,
             bonus_rolls_per_tourney_turn,
             bonus_rolls_per_turn
           )
           (
             select
             a.user_id
             , max(platform) as platform
             , sum(total_turns) as total_turns
             , sum(pvp_turns) as pvp_turns
             , sum(tournament_turns) as tournament_turns
             , count(distinct date_played) as days_played
             , (current_date - max(date_played)::date) as days_since_last_turn
             , sum(pvp_bonus_rolls_used) as pvp_bonus_rolls_used
             , sum(tourney_bonus_rolls_used) as tourney_bonus_rolls_used
             , sum (total_bonus_rolls_used) as total_bonus_rolls_used
             , case when count(distinct date_played) > 0 then sum(nvl(total_turns,0)) / count(distinct date_played) else 0 end as mean_total_turns_per_day
             , case when count(distinct date_played) > 0 then sum(nvl(pvp_turns,0)) / count(distinct date_played) else 0 end  as mean_pvp_turns_per_day
             , case when sum(pvp_turns) > 0 then (sum(pvp_bonus_rolls_used::float) / sum(pvp_turns::float) )
             else 0 end  as bonus_rolls_per_pvp_turn
             , case when sum(tournament_turns) > 0 then
             (sum(tourney_bonus_rolls_used::float) / sum(tournament_turns::float)) else 0 end as bonus_rolls_per_tourney_turn
             , sum(total_bonus_rolls_used) / sum(total_turns) as bonus_rolls_per_turn
             from data_mart.daily_dice_turn a
             group by a.user_id
           );

commit;

            -- insert into final Dice User Summary table

            truncate table models.dice_user_summary_old;

            insert into models.dice_user_summary_old
              ( user_id
                , install_cohort
                , device_token
                , derived_install_date
                , platform
                , store
                , bundle
                , country
                , publisher
                , publisher_sub_campaign
                , bid_type
                , creative
                , device
                , geo
                , interest
                , gender
                , copy
                , age_range
                , other
                , device_model
                , device_type
                , days_since_install
                , total_turns
                , pvp_turns
                , tournament_turns
                , days_played
                , mean_total_turns_per_day
                , mean_pvp_turns_per_day
                , days_since_last_turn
                , pvp_bonus_rolls_used
                , tourney_bonus_rolls_used
                , total_bonus_rolls_used
                , bonus_rolls_per_pvp_turn
                , bonus_rolls_per_tourney_turn
                , bonus_rolls_per_turn
                , is_payer
                , first_purchase
                , days_to_first_purchase
                , last_purchase
                , days_since_last_purchase
                , purchases
                , gross_revenue
                , gross_rev_at_1
                , gross_rev_at_7
                , gross_rev_at_30
                , gross_rev_at_90
                , gross_rev_at_180
                , gross_rev_at_365
                , projected_gross_revenue_90
                , projected_gross_revenue_180
                , projected_gross_revenue_365
                , projected_gross_revenue_1095
                , p_alive
                , source_table
              )
              (
              select
                distinct t1.user_id
                , t1.install_cohort
                , t1.device_token
                , derived_install_date
                , t1.platform
                , store
                , null as bundle
                , country
                , publisher
                , publisher_sub_campaign
                , bid_type
                , creative
                , device
                , geo
                , interest
                , gender
                , 'copy' as copy
                , min_age || '-' || max_age as age_range
                , other
                , device_model
                , device_type
                , days_since_install
                , total_turns
                , pvp_turns
                , tournament_turns
                , days_played
                , mean_total_turns_per_day
                , mean_pvp_turns_per_day
                , days_since_last_turn
                , pvp_bonus_rolls_used
                , tourney_bonus_rolls_used
                , total_bonus_rolls_used
                , bonus_rolls_per_pvp_turn
                , bonus_rolls_per_tourney_turn
                , bonus_rolls_per_turn
                , is_payer
                , first_purchase
                , days_to_first_purchase
                , last_purchase
                , days_since_last_purchase
                , purchases
                , gross_revenue
                , case when days_since_install >= 1 then gross_rev_at_1 else null end as gross_rev_at_1
                , case when days_since_install >= 7 then gross_rev_at_7 else null end as gross_rev_at_7
                , case when days_since_install >= 30 then gross_rev_at_30 else null end as gross_rev_at_30
                , case when days_since_install >= 90 then gross_rev_at_90 else null end as gross_rev_at_90
                , case when days_since_install >= 180 then gross_rev_at_180 else null end as gross_rev_at_180
                , case when days_since_install >= 365 then gross_rev_at_365 else null end as gross_rev_at_365
                , projected_gross_rev_90 as projected_gross_revenue_90
                , projected_gross_rev_180 as projected_gross_revenue_180
                , projected_gross_rev_365 as projected_gross_revenue_365
                , projected_gross_rev_1095 as projected_gross_revenue_1095
                , p_alive
                , source_table
              from models.dice_user_channel t1
                  left join models.dice_user_engagement t4 on t1.user_id = t4.user_id::int
                  left join staging.stg_dice_revenue t5 on t1.user_id = t5.user_id::int
                  left join staging.stg_dice_gross_rev_at t6 on t1.user_id = t6.user_id::int
                  left join staging.stg_dice_rev_projections t7 on t1.user_id = t7.user_id::int
              where  t1.install_cohort::date >= '2011-01-01'::date
              );
