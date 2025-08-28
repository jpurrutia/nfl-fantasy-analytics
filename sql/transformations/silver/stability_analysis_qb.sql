
-- ypa for previous season
WITH bounds AS (
  SELECT MAX(season) AS max_season
  FROM bronze.nfl_play_by_play
  ),
  agg AS (
    SELECT
      passer_player_id AS passer_id
      ,passer_player_name AS passer_name
      ,season
      ,pass_length
      ,COUNT(1) AS attempts
      ,AVG(passing_yards) AS ypa
    FROM nfl_analytics.bronze.nfl_play_by_play t
    JOIN bounds b ON TRUE
    WHERE t.season IN (b.max_season, b.max_season - 1)
      AND pass_length IN ('deep', 'short')
    GROUP BY passer_id, passer_name, season, pass_length
  ),
  with_prev AS (
    SELECT passer_id
      ,passer_name
      ,pass_length
      ,season
      ,ypa
      ,LAG(ypa) OVER (
          PARTITION BY passer_id, pass_length
          ORDER BY season
      ) AS ypa_last,
      attempts
    FROM agg
  ),
  current_rows AS (
    SELECT *
    FROM with_prev
    WHERE season = (SELECT max_season FROM bounds)
  ),
  thresholds AS (
    SELECT
      passer_id,
      MAX(attempts) FILTER (WHERE pass_length = 'short') AS short_attempts,
      MAX(attempts) FILTER (WHERE pass_length = 'deep')  AS deep_attempts
    FROM current_rows
    GROUP BY passer_id
  )
  SELECT
    cr.passer_id
    ,cr.passer_name
    ,cr.pass_length
    ,cr.season
    ,ROUND(cr.ypa, 2) AS ypa
    ,ROUND(cr.ypa_last, 2) AS ypa_last
  FROM current_rows cr
  JOIN thresholds th USING (passer_id)
  WHERE th.short_attempts > 100
    AND th.deep_attempts > 30
  ORDER BY cr.passer_id, season DESC, cr.ypa DESC

/*
Potential additions:
1. visualization - scatterplots -> x axis -> predictor, or causal variable.. y-axis includes response, or effect variable (ypa)
 - use case would be previous year's data as predictor for YPA in current year
2. data smoothing - similar to geom_smooth in R
3. add prediction intervals

Notes: while pearson's correlation coefficient, a qp performance on shorter passes is tweice as stable
Throwing deep passes is more valuable than short passes, but it's difficult to say whether or not a qb is good at deep throws
a wekaer quarterback who generates a high YPA (or EPA per pass attempt) on deep passes one year--without a corresponding increase in such metrics on shorter passes,
the more stableof the two--might be what a regression candidate.

*/
