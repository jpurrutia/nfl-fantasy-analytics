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

## ESPN League Challenges
*Using your actual league data from bronze.espn_* tables*

## Challenge 6: Roster Construction Analysis
**Difficulty: Easy**

Find which team drafted the most "zero RB" strategy (fewest RBs, most WRs).

```sql
-- YOUR SOLUTION HERE:
-- Hint: Use CASE statements and GROUP BY
SELECT 
    t.team_name,
    -- YOUR CODE HERE: Count RBs
    -- YOUR CODE HERE: Count WRs
    -- YOUR CODE HERE: Calculate WR/RB ratio
FROM bronze.espn_teams t
LEFT JOIN bronze.espn_rosters r ON t.team_id = r.team_id
WHERE t.league_id = '537814'
GROUP BY t.team_name
-- YOUR CODE HERE: Order by strategy

-- Expected columns: team_name, rb_count, wr_count, wr_rb_ratio
```

## Challenge 7: Position Scarcity Index
**Difficulty: Medium**

Create a "scarcity score" for each position. Score = (Total players - Rostered players) / Rostered players.

```sql
-- YOUR SOLUTION HERE:
WITH position_counts AS (
    -- YOUR CODE HERE: Count rostered players by position
),
total_pool AS (
    -- Assume: 50 QBs, 80 RBs, 100 WRs, 40 TEs available total
    SELECT 'QB' as position, 50 as total UNION ALL
    SELECT 'RB', 80 UNION ALL
    SELECT 'WR', 100 UNION ALL
    SELECT 'TE', 40
)
-- YOUR CODE HERE: Calculate scarcity score
-- JOIN the CTEs and calculate the score

-- Expected columns: position, rostered, available, scarcity_score
```

## Challenge 8: Trade Partner Finder
**Difficulty: Medium**

Find the best trade partner based on complementary roster needs.

```sql
-- YOUR SOLUTION HERE:
-- You need WRs (you have 6, league avg is 6.7)
-- Find teams with excess WRs and need for your excess positions
WITH team_needs AS (
    SELECT 
        t.team_name,
        -- YOUR CODE HERE: Calculate position counts
        -- YOUR CODE HERE: Calculate surplus/deficit vs league avg
    FROM bronze.espn_teams t
    LEFT JOIN bronze.espn_rosters r ON t.team_id = r.team_id
    WHERE t.league_id = '537814'
    GROUP BY t.team_name
)
-- YOUR CODE HERE: Find teams with opposite needs to yours

-- Expected columns: team_name, your_excess_position, their_excess_position, trade_score
```

## Challenge 9: Roster Uniqueness Score
**Difficulty: Hard**

Calculate how "unique" each team's roster is. Uniqueness = % of players that no other team has.

```sql
-- YOUR SOLUTION HERE:
WITH player_ownership AS (
    -- YOUR CODE HERE: Count how many teams own each player
),
team_uniqueness AS (
    -- YOUR CODE HERE: Calculate unique players per team
)
-- YOUR CODE HERE: Calculate uniqueness percentage

-- Expected columns: team_name, total_players, unique_players, uniqueness_pct
```

## Challenge 10: Position Run Detector
**Difficulty: Hard**

Detect "position runs" in your draft. A "run" = 3+ players of same position drafted consecutively.

```sql
-- YOUR SOLUTION HERE:
-- DuckDB can read JSON files directly!
WITH draft_sequence AS (
    SELECT 
        json_extract(draft_data, '$.draft_progress.drafted_players') as picks
    FROM 
        (SELECT json(readfile('draft_state_Weenieless_Wanderers.json')) as draft_data)
)
-- YOUR CODE HERE: Detect consecutive position picks
-- Hint: Use LAG/LEAD window functions

-- Expected columns: start_pick, end_pick, position, run_length
```

## Interactive Query Building Exercise

**Goal: Create a "Team Power Score"**

Start simple and build complexity:

### Step 1: Basic roster count
```sql
SELECT team_name, COUNT(*) as roster_size
FROM bronze.espn_teams t
LEFT JOIN bronze.espn_rosters r ON t.team_id = r.team_id
WHERE t.league_id = '537814'
GROUP BY team_name;
```

### Step 2: Add position weighting
```sql
-- Add weights: QB=4, RB=3, WR=2, TE=1
-- YOUR CODE HERE
```

### Step 3: Add roster balance bonus
```sql
-- Bonus = 10 points if team has optimal position distribution
-- YOUR CODE HERE
```

### Step 4: Create final power ranking
```sql
-- Combine everything into final query
-- YOUR CODE HERE
```

## Query Optimization Exercise

**Optimize this slow query:**

```sql
-- SLOW VERSION
SELECT DISTINCT player_name
FROM bronze.espn_rosters
WHERE player_name IN (
    SELECT player_name 
    FROM bronze.espn_rosters 
    GROUP BY player_name 
    HAVING COUNT(*) > 1
);

-- YOUR OPTIMIZED VERSION:
-- Hint: Use EXISTS or rewrite with JOIN
```

## Window Functions Practice

```sql
-- Use window functions to rank teams by position count
SELECT 
    team_name,
    position,
    COUNT(*) as count,
    -- YOUR CODE: Add RANK() OVER (PARTITION BY position ORDER BY COUNT(*) DESC)
FROM bronze.espn_teams t
LEFT JOIN bronze.espn_rosters r ON t.team_id = r.team_id
WHERE t.league_id = '537814'
GROUP BY team_name, position;
```

## Solutions
Once you've attempted these, I can provide solutions and optimizations!