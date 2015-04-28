SELECT
  date(time_t),
  _sys_platform_s,
  _context_s,
  _transaction_type_s,
  sum(_bonusroll_n) as broll_in_out,
  count(*) as total_events,
  count(distinct device_token_s) as unique_device_tokens
FROM (
  SELECT * FROM (
    SELECT
      time_t,
      _sys_platform_s,
      _context_s,
      _transaction_type_s,
      _bonusroll_n,
      device_token_s,
      row_number() over(PARTITION BY event_id_s ORDER BY time_n) as rank
    from titan.dice_prod$sys_game_transaction
  ) WHERE rank = 1
) WHERE time_t >= '2014-11-11'
GROUP BY 1, 2, 3, 4;
