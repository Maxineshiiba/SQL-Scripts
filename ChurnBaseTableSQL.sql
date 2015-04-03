--------------written in Redshift

INSERT INTO maggie.base_churn_temp (
  SELECT
    user_id,
    device_token,
    country,
    platform,
    game_id,
    install_cohort,
    '{{date}}' AS base_date,
    days_since_install,
    last_play_date,
    days_played,
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
      ua.device_token,
      ua.country,
      ua.user_id,
      ua.game_id,
      ua.install_cohort,
      ua.days_since_install,
      max(time_t :: DATE)             AS last_play_date,
      count(DISTINCT DATE(sa.time_t)) AS days_played,
      CASE WHEN (count(DISTINCT
                       CASE WHEN sa.time_t <= ua.install_cohort + 7 AND sa.time_t >= ua.install_cohort THEN DATE(
                           sa.time_t)
                       ELSE NULL END)) > 0 THEN 1
      ELSE 0 END                      AS F7 --as first_7_days_played,
      CASE WHEN (count(DISTINCT
                       CASE WHEN sa.time_t >= '{{date}}' - 7 AND sa.time_t < '{{date}}' THEN DATE(sa.time_t)
                       ELSE NULL END)) > 0 THEN 1
      ELSE 0 END                      AS L7  -- as last_7_days_played,
      CASE WHEN (count(DISTINCT
                       CASE WHEN sa.time_t >= '{{date}}' - 8 AND sa.time_t < '{{date}}' THEN DATE(sa.time_t)
                       ELSE NULL END)) > 0 THEN 1
      ELSE 0 END                      AS L8  -- as last_8_days_played,
      CASE WHEN (count(DISTINCT
                       CASE WHEN sa.time_t >= '{{date}}' - 9 AND sa.time_t < '{{date}}' THEN DATE(sa.time_t)
                       ELSE NULL END)) > 0 THEN 1
      ELSE 0 END                      AS L9  --as last_10_days_played,
      CASE WHEN (count(DISTINCT
                       CASE WHEN sa.time_t >= '{{date}}' - 10 AND sa.time_t < '{{date}}' THEN DATE(sa.time_t)
                       ELSE NULL END)) > 0 THEN 1
      ELSE 0 END                      AS L10  --as last_10_days_played,
      CASE WHEN (count(DISTINCT
                       CASE WHEN sa.time_t >= '{{date}}' - 11 AND sa.time_t < '{{date}}' THEN DATE(sa.time_t)
                       ELSE NULL END)) > 0 THEN 1
      ELSE 0 END                      AS L11 --as next_11_days_played,
      CASE WHEN (count(DISTINCT
                       CASE WHEN sa.time_t >= '{{date}}' - 12 AND sa.time_t < '{{date}}' THEN DATE(sa.time_t)
                       ELSE NULL END)) > 0 THEN 1
      ELSE 0 END                      AS L12  --as last_10_days_played,
      CASE WHEN (count(DISTINCT
                       CASE WHEN sa.time_t >= '{{date}}' - 13 AND sa.time_t < '{{date}}' THEN DATE(sa.time_t)
                       ELSE NULL END)) > 0 THEN 1
      ELSE 0 END                      AS L13  --as last_10_days_played,
      CASE WHEN (count(DISTINCT
                       CASE WHEN sa.time_t >= '{{date}}' - 14 AND sa.time_t < '{{date}}' THEN DATE(sa.time_t)
                       ELSE NULL END)) > 0 THEN 1
      ELSE 0 END                      AS L14  --as last_10_days_played,
      CASE WHEN (count(DISTINCT
                       CASE WHEN sa.time_t >= '{{date}}' - 15 AND sa.time_t < '{{date}}' THEN DATE(sa.time_t)
                       ELSE NULL END)) > 0 THEN 1
      ELSE 0 END                      AS L15  --as last_10_days_played,
      CASE WHEN (count(DISTINCT
                       CASE WHEN sa.time_t >= '{{date}}' - 16 AND sa.time_t < '{{date}}' THEN DATE(sa.time_t)
                       ELSE NULL END)) > 0 THEN 1
      ELSE 0 END                      AS L16  --as last_10_days_played,
      CASE WHEN (count(DISTINCT
                       CASE WHEN sa.time_t >= '{{date}}' - 17 AND sa.time_t < '{{date}}' THEN DATE(sa.time_t)
                       ELSE NULL END)) > 0 THEN 1
      ELSE 0 END                      AS L17  --as last_10_days_played,
      CASE WHEN (count(DISTINCT
                       CASE WHEN sa.time_t >= '{{date}}' - 18 AND sa.time_t < '{{date}}' THEN DATE(sa.time_t)
                       ELSE NULL END)) > 0 THEN 1
      ELSE 0 END                      AS L18  --as last_10_days_played,
      CASE WHEN (count(DISTINCT
                       CASE WHEN sa.time_t >= '{{date}}' - 19 AND sa.time_t < '{{date}}' THEN DATE(sa.time_t)
                       ELSE NULL END)) > 0 THEN 1
      ELSE 0 END                      AS L19  --as last_10_days_played,
      CASE WHEN (count(DISTINCT
                       CASE WHEN sa.time_t >= '{{date}}' - 20 AND sa.time_t < '{{date}}' THEN DATE(sa.time_t)
                       ELSE NULL END)) > 0 THEN 1
      ELSE 0 END                      AS L20  --as last_10_days_played,
      CASE WHEN (count(DISTINCT
                       CASE WHEN sa.time_t >= '{{date}}' - 21 AND sa.time_t < '{{date}}' THEN DATE(sa.time_t)
                       ELSE NULL END)) > 0 THEN 1
      ELSE 0 END                      AS L21  --as last_10_days_played,
      CASE WHEN (count(DISTINCT
                       CASE WHEN sa.time_t >= '{{date}}' - 24 AND sa.time_t < '{{date}}' THEN DATE(sa.time_t)
                       ELSE NULL END)) > 0 THEN 1
      ELSE 0 END                      AS L24  --as last_10_days_played,
      CASE WHEN (count(DISTINCT
                       CASE WHEN sa.time_t >= '{{date}}' - 25 AND sa.time_t < '{{date}}' THEN DATE(sa.time_t)
                       ELSE NULL END)) > 0 THEN 1
      ELSE 0 END                      AS L25  --as last_10_days_played,
      CASE WHEN (count(DISTINCT
                       CASE WHEN sa.time_t >= '{{date}}' - 29 AND sa.time_t < '{{date}}' THEN DATE(sa.time_t)
                       ELSE NULL END)) > 0 THEN 1
      ELSE 0 END                      AS L29 --as next_11_days_played,
      CASE WHEN (count(DISTINCT
                       CASE WHEN sa.time_t >= '{{date}}' - 30 AND sa.time_t < '{{date}}' THEN DATE(sa.time_t)
                       ELSE NULL END)) > 0 THEN 1
      ELSE 0 END                      AS L30 --as next_11_days_played,
      CASE WHEN (count(DISTINCT
                       CASE WHEN sa.time_t <= '{{date}}' + 7 AND sa.time_t > '{{date}}' THEN DATE(sa.time_t)
                       ELSE NULL END)) > 0 THEN 1
      ELSE 0 END                      AS N7--as next_7_days_played,
      CASE WHEN (count(DISTINCT
                       CASE WHEN sa.time_t <= '{{date}}' + 8 AND sa.time_t > '{{date}}' THEN DATE(sa.time_t)
                       ELSE NULL END)) > 0 THEN 1
      ELSE 0 END                      AS N8 -- as next_8_days_played,
      CASE WHEN (count(DISTINCT
                       CASE WHEN sa.time_t <= '{{date}}' + 9 AND sa.time_t > '{{date}}' THEN DATE(sa.time_t)
                       ELSE NULL END)) > 0 THEN 1
      ELSE 0 END                      AS N9 -- as next_8_days_played,
      CASE WHEN (count(DISTINCT
                       CASE WHEN sa.time_t <= '{{date}}' + 10 AND sa.time_t > '{{date}}' THEN DATE(sa.time_t)
                       ELSE NULL END)) > 0 THEN 1
      ELSE 0 END                      AS N10  --as next_10_days_played,
      CASE WHEN (count(DISTINCT
                       CASE WHEN sa.time_t <= '{{date}}' + 11 AND sa.time_t > '{{date}}' THEN DATE(sa.time_t)
                       ELSE NULL END)) > 0 THEN 1
      ELSE 0 END                      AS N11  --as next_11_days_played,
      CASE WHEN (count(DISTINCT
                       CASE WHEN sa.time_t <= '{{date}}' + 12 AND sa.time_t > '{{date}}' THEN DATE(sa.time_t)
                       ELSE NULL END)) > 0 THEN 1
      ELSE 0 END                      AS N12 -- as next_8_days_played,
      CASE WHEN (count(DISTINCT
                       CASE WHEN sa.time_t <= '{{date}}' + 13 AND sa.time_t > '{{date}}' THEN DATE(sa.time_t)
                       ELSE NULL END)) > 0 THEN 1
      ELSE 0 END                      AS N13 -- as next_8_days_played,
      CASE WHEN (count(DISTINCT
                       CASE WHEN sa.time_t <= '{{date}}' + 14 AND sa.time_t > '{{date}}' THEN DATE(sa.time_t)
                       ELSE NULL END)) > 0 THEN 1
      ELSE 0 END                      AS N14 -- as next_8_days_played,
      CASE WHEN (count(DISTINCT
                       CASE WHEN sa.time_t <= '{{date}}' + 15 AND sa.time_t > '{{date}}' THEN DATE(sa.time_t)
                       ELSE NULL END)) > 0 THEN 1
      ELSE 0 END                      AS N15  --as next_15_days_played,
      CASE WHEN (count(DISTINCT
                       CASE WHEN sa.time_t <= '{{date}}' + 16 AND sa.time_t > '{{date}}' THEN DATE(sa.time_t)
                       ELSE NULL END)) > 0 THEN 1
      ELSE 0 END                      AS N16  --as next_15_days_played,
      CASE WHEN (count(DISTINCT
                       CASE WHEN sa.time_t <= '{{date}}' + 17 AND sa.time_t > '{{date}}' THEN DATE(sa.time_t)
                       ELSE NULL END)) > 0 THEN 1
      ELSE 0 END                      AS N17  --as next_15_days_played,
      CASE WHEN (count(DISTINCT
                       CASE WHEN sa.time_t <= '{{date}}' + 18 AND sa.time_t > '{{date}}' THEN DATE(sa.time_t)
                       ELSE NULL END)) > 0 THEN 1
      ELSE 0 END                      AS N18  --as next_15_days_played,
      CASE WHEN (count(DISTINCT
                       CASE WHEN sa.time_t <= '{{date}}' + 19 AND sa.time_t > '{{date}}' THEN DATE(sa.time_t)
                       ELSE NULL END)) > 0 THEN 1
      ELSE 0 END                      AS N19  --as next_15_days_played,
      CASE WHEN (count(DISTINCT
                       CASE WHEN sa.time_t <= '{{date}}' + 20 AND sa.time_t > '{{date}}' THEN DATE(sa.time_t)
                       ELSE NULL END)) > 0 THEN 1
      ELSE 0 END                      AS N20  --as next_15_days_played,
      CASE WHEN (count(DISTINCT
                       CASE WHEN sa.time_t <= '{{date}}' + 21 AND sa.time_t > '{{date}}' THEN DATE(sa.time_t)
                       ELSE NULL END)) > 0 THEN 1
      ELSE 0 END                      AS N21  --as next_15_days_played,
      CASE WHEN (count(DISTINCT
                       CASE WHEN sa.time_t <= '{{date}}' + 24 AND sa.time_t > '{{date}}' THEN DATE(sa.time_t)
                       ELSE NULL END)) > 0 THEN 1
      ELSE 0 END                      AS N24  --as next_15_days_played,
      CASE WHEN (count(DISTINCT
                       CASE WHEN sa.time_t <= '{{date}}' + 25 AND sa.time_t > '{{date}}' THEN DATE(sa.time_t)
                       ELSE NULL END)) > 0 THEN 1
      ELSE 0 END                      AS N25  --as next_15_days_played,
      CASE WHEN (count(DISTINCT
                       CASE WHEN sa.time_t <= '{{date}}' + 29 AND sa.time_t > '{{date}}' THEN DATE(sa.time_t)
                       ELSE NULL END)) > 0 THEN 1
      ELSE 0 END                      AS N29 --as next_15_days_played
    FROM data_mart.fact_titan_user_summary ua
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
        ON ua.device_token = sa._sys_device_token_s
    WHERE ua.game_id = 0
    GROUP BY 1, 2, 3, 4, 5, 6, 7
  )
)


