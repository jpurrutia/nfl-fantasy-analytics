# NFL Analytics SQL Queries

## Stability Analysis

### Key Statistical Findings

#### 1. QB Consistency (Week-to-Week Variance)
- **Metric**: Coefficient of Variation (CV = stddev/mean)
- **Most Consistent QBs (2024)**:
  1. Kyler Murray - CV: 0.153
  2. Kirk Cousins - CV: 0.160  
  3. Patrick Mahomes - CV: 0.166
- **Key Insight**: Lower CV indicates more predictable weekly performance

#### 2. Stability Patterns
- **Short passes**: 2x more stable than deep passes year-over-year
- **Statistical basis**: Higher R² for short pass correlations
- **Application**: Weight short-pass efficiency higher in projections

#### 3. Mean Reversion Analysis

**Regression Candidates (SELL HIGH)**:
- Jayden Daniels: Deep YPA z-score = 2.01 (extreme outlier)
- Lamar Jackson: Short YPA z-score = 2.1 (unsustainable)

**TD Regression Likely**:
- QBs with deep TD% > 7% historically regress to ~5-6%
- Russell Wilson (12.1%), Joe Burrow (12.4%) are prime candidates

**Statistical Methods Used**:
- Z-score normalization for outlier detection
- Regression coefficient: ~0.6 for QB stats (40% regression to mean)
- Percentile rankings for peer comparison

## Usage

### Run stability analysis:
```bash
duckdb data/nfl_analytics.duckdb < sql/analytics/stability/qb_week_to_week_variance.sql
```

### Run mean reversion:
```bash
duckdb data/nfl_analytics.duckdb < sql/analytics/regression/mean_reversion_analysis.sql
```

## Future Enhancements
- [ ] Add Bayesian credibility adjustments for small samples
- [ ] Implement aging curves for projection adjustments
- [ ] Add opponent-adjusted metrics
- [ ] Create composite ranking system