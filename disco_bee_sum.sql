INSERT INTO maggie.disco_sum
SELECT
  UserSum.*,
  AppOpen.sessions,
  AppOpen.days_played,
  AppOpen.first_7_days_played,
  AppOpen.last_7_days_played,
  AppOpen.last_14_days_played,
  AppOpen.last_30_days_played,
  CASE WHEN AppOpen.last_7_days_played = 0 AND AppOpen.last_8_days_played = 1 THEN 1
  ELSE 0 END AS churn_flag,
  LevelResults.first_level_failed,
  LevelResults.max_level,
  LevelResults.max_world,
  LevelResults.level_attempts,
  LevelResults.single_wins,
  LevelResults.single_losses,
  LevelResults.single_quits,
  LevelResults.single_win_perc,
  LevelResults.single_loss_perc,
  LevelResults.single_quit_perc,
  LevelResults.single_player_total_purchases,
  LevelResults.single_player_limit_purchases,
  LevelResults.single_player_booster_purchases,
  LevelResults.first_purchase_level,
  Viral.FacebookPost,
  Viral.FacebookRequest,
  Viral.FacebookInvite,
  Tournament.tournament_entries,
  Tournament.tournament_attmepts,
  Tournament.tournament_wins,
  Tournament.tournament_losses,
  Tournament.tournament_quits,
  Tournament.tournament_win_perc,
  Tournament.tournament_loss_perc,
  Tournament.tournament_quit_perc,
  Tournament.tournament_player_total_purchases,
  Tournament.tournament_player_limit_purchases,
  Tournament.tournament_player_booster_purchases
FROM (
--start w/sessions and days_played
       SELECT
         us.device_token,
         us.install_cohort,
         max(time_t :: DATE)             AS last_play_date,
         count(DISTINCT ao.session_s)    AS sessions,
         count(DISTINCT DATE(ao.time_t)) AS days_played,
         count(DISTINCT
               CASE WHEN ao.time_t <= us.install_cohort + 7 AND ao.time_t >= us.install_cohort THEN DATE(ao.time_t)
               ELSE NULL END)            AS first_7_days_played,
         count(DISTINCT CASE WHEN ao.time_t >= SYSDATE :: DATE - 7 THEN DATE(ao.time_t)
                        ELSE NULL END)   AS last_7_days_played,
         count(DISTINCT CASE WHEN ao.time_t >= SYSDATE :: DATE - 8 THEN DATE(ao.time_t)
                        ELSE NULL END)   AS last_8_days_played,
         count(DISTINCT CASE WHEN ao.time_t >= SYSDATE :: DATE - 14 THEN DATE(ao.time_t)
                        ELSE NULL END)   AS last_14_days_played,
         count(DISTINCT CASE WHEN ao.time_t >= SYSDATE :: DATE - 30 THEN DATE(ao.time_t)
                        ELSE NULL END)   AS last_30_days_played
       FROM data_mart.fact_titan_user_summary us
         LEFT JOIN
         (
           SELECT *
           FROM (
             SELECT
               device_token_s,
               session_s,
               time_t,
               row_number()
               OVER (PARTITION BY event_id_s
                 ORDER BY time_n) AS rank
             FROM titan.disco_bees$sys_app_open
             WHERE time_t >= '2014-11-11'
           )
           WHERE rank = 1
         ) ao
           ON ao.device_token_s = us.device_token
       WHERE us.game_id = 18
       GROUP BY 1, 2
     ) AppOpen
  LEFT JOIN (
-- Level Results Query
              SELECT
                us.device_token,
                us.install_cohort,
                min(CASE WHEN _outcome_s = 'Lose' THEN _overall_level_n
                    ELSE NULL END)                                          AS first_level_failed,
                max(_overall_level_n)                                       AS max_level,
                max(_world_n)                                               AS max_world,
                count(t.device_token_s)                                     AS level_attempts,
                sum(CASE WHEN _outcome_s = 'Win' THEN 1
                    ELSE 0 END)                                             AS single_wins,
                sum(CASE WHEN _outcome_s = 'Lose' THEN 1
                    ELSE 0 END)                                             AS single_losses,
                sum(CASE WHEN _outcome_s = 'Quit' THEN 1
                    ELSE 0 END)                                             AS single_quits,
                CASE WHEN count(t.device_token_s) = 0 THEN 0
                ELSE sum(CASE WHEN _outcome_s = 'Win' THEN 1
                         ELSE 0 END) :: FLOAT / count(t.device_token_s) END AS single_win_perc,
                CASE WHEN count(t.device_token_s) = 0 THEN 0
                ELSE sum(CASE WHEN _outcome_s = 'Lose' THEN 1
                         ELSE 0 END) :: FLOAT / count(t.device_token_s) END AS single_loss_perc,
                CASE WHEN count(t.device_token_s) = 0 THEN 0
                ELSE sum(CASE WHEN _outcome_s = 'Quit' THEN 1
                         ELSE 0 END) :: FLOAT / count(t.device_token_s) END AS single_quit_perc,
                SUM(_limits_purchased_n
                    + CASE WHEN _exploders_purchased_n IS NULL THEN 0
                      ELSE _exploders_purchased_n END
                    + CASE WHEN _colorbomb_purchased_n IS NULL THEN 0
                      ELSE _colorbomb_purchased_n END
                    + CASE WHEN _scotty_purchased_1_n IS NULL THEN 0
                      ELSE _scotty_purchased_1_n END
                    + CASE WHEN _stingers_purchased_n IS NULL THEN 0
                      ELSE _stingers_purchased_n END
                    + CASE WHEN _moves_pack_purchased_n IS NULL THEN 0
                      ELSE _moves_pack_purchased_n END)                     AS single_player_total_purchases,
                SUM(_limits_purchased_n)                                    AS single_player_limit_purchases,
                SUM(CASE WHEN _exploders_purchased_n IS NULL THEN 0
                    ELSE +_exploders_purchased_n END
                    + CASE WHEN _colorbomb_purchased_n IS NULL THEN 0
                      ELSE _colorbomb_purchased_n END
                    + CASE WHEN _scotty_purchased_1_n IS NULL THEN 0
                      ELSE _scotty_purchased_1_n END
                    + CASE WHEN _stingers_purchased_n IS NULL THEN 0
                      ELSE _stingers_purchased_n END
                    + CASE WHEN _moves_pack_purchased_n IS NULL THEN 0
                      ELSE _moves_pack_purchased_n END)                     AS single_player_booster_purchases,
                MIN(CASE WHEN _limits_purchased_n > 0 OR
                              _exploders_purchased_n > 0 OR
                              _colorbomb_purchased_n > 0 OR
                              _scotty_purchased_1_n > 0 OR
                              _stingers_purchased_n > 0 OR
                              _moves_pack_purchased_n > 0 THEN _overall_level_n
                    ELSE NULL END)                                          AS first_purchase_level
              FROM data_mart.fact_titan_user_summary us
                LEFT JOIN (
                            SELECT *
                            FROM (
                              SELECT
                                device_token_s,
                                _overall_level_n,
                                _world_n,
                                _outcome_s,
                                _limits_purchased_n,
                                _exploders_purchased_n,
                                _colorbomb_purchased_n,
                                _scotty_purchased_1_n,
                                _stingers_purchased_n,
                                _moves_pack_purchased_n,
                                ROW_NUMBER()
                                OVER (PARTITION BY event_id_s
                                  ORDER BY time_n) AS rank
                              FROM titan.disco_bees$level_results
                              WHERE time_t >= '2014-11-11'
                                    AND _overall_level_n < 500
                            )
                            WHERE rank = 1
                          ) t
                  ON us.device_token = t.device_token_s
              WHERE us.game_id = 18
              GROUP BY 1, 2
            ) LevelResults
    ON AppOpen.device_token = LevelResults.device_token
       AND AppOpen.install_cohort = LevelResults.install_cohort
  LEFT JOIN (
-- Sys_Viral
              SELECT
                us.device_token,
                us.install_cohort,
                sum(CASE WHEN v._viral_type_s = 'FacebookPost' THEN 1
                    ELSE 0 END) AS FacebookPost,
                sum(CASE WHEN v._viral_type_s = 'FacebookRequest' THEN 1
                    ELSE 0 END) AS FacebookRequest,
                sum(CASE WHEN v._viral_type_s = 'FacebookInvite' THEN 1
                    ELSE 0 END) AS FacebookInvite
              FROM data_mart.fact_titan_user_summary us
                LEFT JOIN
                (
                  SELECT *
                  FROM (
                    SELECT
                      device_token_s,
                      _viral_type_s,
                      ROW_NUMBER()
                      OVER (PARTITION BY event_id_s
                        ORDER BY time_n) AS rank
                    FROM titan.disco_bees$sys_viral
                    WHERE time_t >= '2014-11-11'
                  )
                  WHERE rank = 1
                ) v
                  ON us.device_token = v.device_token_s
              WHERE game_id = 18
              GROUP BY 1, 2
            ) Viral
    ON AppOpen.device_token = Viral.device_token
       AND AppOpen.install_cohort = Viral.install_cohort
LEFT JOIN (
-- Tournament Results
    SELECT
      us.device_token,
      us.install_cohort,
      count(DISTINCT _tournament_id_n)                            AS tournament_entries,
      count(t.device_token_s)                                     AS tournament_attmepts,
      sum(CASE WHEN _outcome_s = 'Win' THEN 1
          ELSE 0 END)                                             AS tournament_wins,
      sum(CASE WHEN _outcome_s = 'Lose' THEN 1
          ELSE 0 END)                                             AS tournament_losses,
      sum(CASE WHEN _outcome_s = 'Quit' THEN 1
          ELSE 0 END)                                             AS tournament_quits,
      CASE WHEN count(t.device_token_s) = 0 THEN 0
      ELSE sum(CASE WHEN _outcome_s = 'Win' THEN 1
               ELSE 0 END) :: FLOAT / count(t.device_token_s) END AS tournament_win_perc,
      CASE WHEN count(t.device_token_s) = 0 THEN 0
      ELSE sum(CASE WHEN _outcome_s = 'Lose' THEN 1
               ELSE 0 END) :: FLOAT / count(t.device_token_s) END AS tournament_loss_perc,
      CASE WHEN count(t.device_token_s) = 0 THEN 0
      ELSE sum(CASE WHEN _outcome_s = 'Quit' THEN 1
               ELSE 0 END) :: FLOAT / count(t.device_token_s) END AS tournament_quit_perc,
      SUM(CASE WHEN _limits_purchased_n IS NULL THEN 0
          ELSE _limits_purchased_n END
          + CASE WHEN _exploders_purchased_n IS NULL THEN 0
            ELSE _exploders_purchased_n END
          + CASE WHEN _colorbomb_purchased_n IS NULL THEN 0
            ELSE _colorbomb_purchased_n END
          + CASE WHEN _scotty_purchased_1_n IS NULL THEN 0
            ELSE _scotty_purchased_1_n END
          + CASE WHEN _stingers_purchased_n IS NULL THEN 0
            ELSE _stingers_purchased_n END)                       AS tournament_player_total_purchases,
      SUM(CASE WHEN _limits_purchased_n IS NULL THEN 0
          ELSE _limits_purchased_n END)                           AS tournament_player_limit_purchases,
      SUM(CASE WHEN _exploders_purchased_n IS NULL THEN 0
          ELSE +_exploders_purchased_n END
          + CASE WHEN _colorbomb_purchased_n IS NULL THEN 0
            ELSE _colorbomb_purchased_n END
          + CASE WHEN _scotty_purchased_1_n IS NULL THEN 0
            ELSE _scotty_purchased_1_n END
          + CASE WHEN _stingers_purchased_n IS NULL THEN 0
            ELSE _stingers_purchased_n END)                       AS tournament_player_booster_purchases
    FROM data_mart.fact_titan_user_summary us
      LEFT JOIN (
                  SELECT *
                  FROM (
                    SELECT
                      device_token_s,
                      _tournament_id_n,
                      _outcome_s,
                      _limits_purchased_n,
                      _exploders_purchased_n,
                      _colorbomb_purchased_n,
                      _scotty_purchased_1_n,
                      _stingers_purchased_n,
                      ROW_NUMBER()
                      OVER (PARTITION BY event_id_s
                        ORDER BY time_n) rank
                    FROM titan.disco_bees$tournament_level_results
                    WHERE _tournament_id_n <= 5000
                          AND time_t >= '2014-11-11'
                  )
                  WHERE rank = 1
                ) t
        ON t.device_token_s = us.device_token
    WHERE game_id = 18
    GROUP BY 1, 2
    ) Tournament
ON AppOpen.device_token = Tournament.device_token
AND AppOpen.install_cohort = Tournament.install_cohort
  LEFT JOIN (
    SELECT * FROM data_mart.fact_titan_user_summary
    WHERE game_id = 18
    ) UserSum
ON AppOpen.device_token = UserSum.device_token
AND AppOpen.install_cohort = UserSum.install_cohort
  WHERE UserSum.game_id = 18;
