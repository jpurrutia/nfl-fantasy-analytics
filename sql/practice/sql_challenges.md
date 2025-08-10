# SQL Practice Challenges - LeetCode Style

## Challenge 1: Top Performers Query
**Difficulty: Easy**

Write a query to find the top 5 RBs by average rushing yards over the last 3 weeks.

```sql
-- YOUR SOLUTION HERE:
-- Hint: You'll need to filter by position and week, then calculate averages





-- Expected columns: player_name, avg_rushing_yards, games_played
```

## Challenge 2: Target Share Analysis
**Difficulty: Medium**

Write a query to calculate target share by week for all WRs and TEs, showing only players with >20% target share.

```sql
-- YOUR SOLUTION HERE:
-- Hint: Target share = player targets / team total targets
-- You'll need a window function





-- Expected columns: player_name, week, targets, target_share_pct
```

## Challenge 3: Consistency Metrics
**Difficulty: Medium**

Find players who have scored within 20% of their season average for at least 5 consecutive weeks.

```sql
-- YOUR SOLUTION HERE:
-- Hint: Use window functions to calculate rolling averages
-- Compare each week to the season average





-- Expected columns: player_name, consistent_weeks, avg_points
```

## Challenge 4: Red Zone Efficiency
**Difficulty: Hard**

Calculate red zone touchdown conversion rate for all players, joining opportunity and performance data.

```sql
-- YOUR SOLUTION HERE:
-- Hint: Join player_opportunity and player_performance
-- RZ TD rate = RZ TDs / RZ touches





-- Expected columns: player_name, rz_touches, rz_tds, conversion_rate
```

## Challenge 5: Matchup Analysis
**Difficulty: Hard**

Find the best matchups for next week by analyzing how positions perform against specific defenses.

```sql
-- YOUR SOLUTION HERE:
-- Hint: Calculate average points allowed by defense to each position
-- Join with schedule data to find next week's matchups





-- Expected columns: position, defense, avg_points_allowed, next_week_matchup
```

## Solutions
Once you've attempted these, I can provide solutions and optimizations!