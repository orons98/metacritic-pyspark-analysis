# Metacritic Game Ratings Analysis with PySpark

This project was completed as part of a university assignment focused on data analysis using PySpark and received a final mark of **71%**. The task involved querying and summarising a large dataset of Metacritic game reviews to uncover insights about platforms, genres, and developers.

## Objectives
- Filter and summarise games with high critic scores (e.g. ≥ 95)
- Group by developer to calculate:
  - Number of titles
  - Average critic/user scores
  - Total critics/users
- Identify the developer with the most multi-platform releases
- Detect the platform with the highest share of open-world games
- Count distinct single-player titles and their associated developers

## Tools and Technologies
- PySpark (DataFrames API)
- Google Colab  
- CSV dataset (~10,000+ rows)

## Skills Demonstrated
- Data wrangling and transformation with PySpark
- Filtering, aggregation, and grouping
- Handling multi-platform duplicates
- Applying logical conditions to string fields (e.g. identifying “Single Player” titles)
- Working with large datasets in a distributed computing environment

## Notes
This assignment was graded via CodeGrade with AutoTests and received a **71%**.  
Although not all outputs were exact matches, all logic and core query structures were successfully implemented.

## Example Queries

Below are representative PySpark queries used in this project. These showcase key data transformations and aggregations applied to the Metacritic dataset.

```python
# 1. Filter games with score ≥ 95 released after 2017
top_games = df.filter((df['score'] >= 95) & (df['release_year'] >= 2017))
top_games.select('title', 'score', 'release_year').show()

# 2. Developer summary: number of titles, average scores, totals
from pyspark.sql.functions import avg, countDistinct, sum

developer_summary = df.groupBy('developer').agg(
    countDistinct('title').alias('num_titles'),
    avg('score').alias('avg_critic_score'),
    avg('user_score').alias('avg_user_score'),
    sum('num_critics').alias('total_critics'),
    sum('num_users').alias('total_users')
).orderBy('num_titles', ascending=False)
developer_summary.show()

# 3. Identify multi-platform titles (appear on ≥ 2 platforms)
multi_platform = df.groupBy('title').agg(
    countDistinct('platform').alias('platform_count')
).filter("platform_count >= 2")
multi_platform.show()

# 4. Developer with most distinct platform releases
most_platforms = df.groupBy('developer').agg(
    countDistinct('platform').alias('distinct_platforms')
).orderBy('distinct_platforms', ascending=False).limit(1)
most_platforms.show()

# 5. Platform with highest percentage of Open-World titles
from pyspark.sql.functions import col

open_world = df.filter(df['genre'].contains('Open-World'))
open_world_count = open_world.groupBy('platform').count().alias('open_world_count')
total_count = df.groupBy('platform').count().alias('total_count')

platform_percent = open_world_count.join(total_count, 'platform') \
    .withColumn('percent_open_world', (col('open_world_count') / col('count')) * 100) \
    .orderBy('percent_open_world', ascending=False)
platform_percent.select('platform', 'percent_open_world').show()

# 6. Count distinct Single Player titles and developers
single_player = df.filter(
    (df['feature_player'] == '1 Player') | (df['feature_player'] == 'No Online Multiplayer')
)
num_titles = single_player.select('title').distinct().count()
num_developers = single_player.select('developer').distinct().count()
