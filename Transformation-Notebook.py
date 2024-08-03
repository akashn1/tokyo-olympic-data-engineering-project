# Databricks notebook source
# MAGIC %md
# MAGIC ##Reading Raw Data

# COMMAND ----------

# Reading the CSV files from the raw data folder
medals_df = spark.read.csv("/mnt/raw-data/Medals.csv", header=True, inferSchema=True)
entries_gender_df = spark.read.csv("/mnt/raw-data/EntriesGender.csv", header=True, inferSchema=True)
coaches_df = spark.read.csv("/mnt/raw-data/Coaches.csv", header=True, inferSchema=True)
athletes_df = spark.read.csv("/mnt/raw-data/Athletes.csv", header=True, inferSchema=True)
team_df = spark.read.csv("/mnt/raw-data/Teams.csv", header=True, inferSchema=True)


# COMMAND ----------

# MAGIC %md ##Data Cleaning

# COMMAND ----------

# Removing duplicates and handling missing values, then renaming columns for consistency
athletes_df = athletes_df.dropDuplicates().fillna({'Country': 'Unknown', 'Discipline': 'Unknown'}).withColumnRenamed('Country', 'AthleteCountry')
coaches_df = coaches_df.dropDuplicates().fillna({'Country': 'Unknown', 'Discipline': 'Unknown', 'Event': 'Unknown'}).withColumnRenamed('Country', 'CoachCountry')
entries_gender_df = entries_gender_df.dropDuplicates().fillna({'Female': 0, 'Male': 0, 'Total': 0})
medals_df = medals_df.dropDuplicates().fillna({'TeamCountry': 'Unknown', 'Gold': 0, 'Silver': 0, 'Bronze': 0, 'Total': 0, 'Rank by Total': 0}).withColumnRenamed('TeamCountry', 'MedalCountry')
team_df = team_df.dropDuplicates().fillna({'Discipline': 'Unknown', 'Country': 'Unknown', 'Event': 'Unknown'}).withColumnRenamed('Country', 'TeamCountry')

# Saving cleaned data to cleansed-data folder
athletes_df.write.parquet("/mnt/cleansed-data/Athletes.parquet")
coaches_df.write.parquet("/mnt/cleansed-data/Coaches.parquet")
entries_gender_df.write.parquet("/mnt/cleansed-data/EntriesGender.parquet")
medals_df.write.parquet("/mnt/cleansed-data/Medals.parquet")
team_df.write.parquet("/mnt/cleansed-data/Team.parquet")


# COMMAND ----------

# MAGIC %md
# MAGIC ##Joining DataFrames

# COMMAND ----------

# Joining DataFrames to enrich the data with additional information
athletes_medals_df = athletes_df.join(medals_df, athletes_df['AthleteCountry'] == medals_df['MedalCountry'], 'left')
athletes_medals_entries_df = athletes_medals_df.join(entries_gender_df, athletes_df['Discipline'] == entries_gender_df['Discipline'], 'left')
full_df = athletes_medals_entries_df.join(coaches_df, athletes_df['Discipline'] == coaches_df['Discipline'], 'left')
final_df = full_df.join(team_df, athletes_df['Discipline'] == team_df['Discipline'], 'left')
# Show joined data
print("Joined DataFrame:")
final_df.show()


# COMMAND ----------

# MAGIC %md
# MAGIC ##Aggregations

# COMMAND ----------

# Summarizing data by calculating the count of medals per country
medals_per_country = medals_df.groupBy('MedalCountry').count()

# Summarizing data by calculating the count of participants per discipline
participants_per_discipline = athletes_df.groupBy('Discipline').count()
# Show aggregated data
print("Medals per Country:")
medals_per_country.show()
print("Participants per Discipline:")
participants_per_discipline.show()


# COMMAND ----------

# MAGIC %md
# MAGIC ##Window Functions

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import rank, sum as _sum

# Using window functions to rank athletes based on medals and calculate cumulative medals per country
window_spec = Window.partitionBy('MedalCountry').orderBy(medals_df['Total'].desc())
ranked_df = medals_df.withColumn('Rank', rank().over(window_spec))
cumulative_medals_df = medals_df.withColumn('CumulativeMedals', _sum('Total').over(window_spec))

# Show window function results
print("Ranked DataFrame:")
ranked_df.show()
print("Cumulative Medals DataFrame:")
cumulative_medals_df.show()


# COMMAND ----------

# MAGIC %md
# MAGIC ##Pivot and Unpivot

# COMMAND ----------

from pyspark.sql.functions import sum, col

# Pivoting data to get the count of each type of medal per country
pivot_df = medals_df.groupBy('MedalCountry').agg(
    sum(col('Gold')).alias('Gold'),
    sum(col('Silver')).alias('Silver'),
    sum(col('Bronze')).alias('Bronze')
).fillna(0)

# Unpivoting data to revert back to long format
unpivot_df = pivot_df.selectExpr(
    "MedalCountry", 
    "stack(3, 'Gold', Gold, 'Silver', Silver, 'Bronze', Bronze) as (Medal, Count)"
)

# Show pivoted and unpivoted data
print("Pivoted DataFrame:")
display(pivot_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##User-Defined Functions

# COMMAND ----------

from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

# Defining a UDF to categorize countries by region and applying it to enrich data
def categorize_region(country):
    if country in ['USA', 'Canada', 'Mexico']:
        return 'North America'
    elif country in ['UK', 'France', 'Germany']:
        return 'Europe'
    else:
        return 'Other'

region_udf = udf(categorize_region, StringType())
athletes_df = athletes_df.withColumn('Region', region_udf(athletes_df['AthleteCountry']))

display(athletes_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##String Operations

# COMMAND ----------

from pyspark.sql.functions import upper, substring

# Performing string operations like converting country names to uppercase and extracting substrings from event names
athletes_df = athletes_df.withColumn('UpperCountry', upper(athletes_df['AthleteCountry']))

display(athletes_df)


# COMMAND ----------

# MAGIC %md
# MAGIC ##Saving all transformed data in confirmed folder

# COMMAND ----------

# Saving all the transformed data in the 'confirmed-data' folder with the same name and format

athletes_df.write.mode('overwrite').parquet('/mnt/confirmed-data/athletes_df')
athletes_medals_df.write.mode('overwrite').parquet('/mnt/confirmed-data/athletes_medals_df')
coaches_df.write.mode('overwrite').parquet('/mnt/confirmed-data/coaches_df')
cumulative_medals_df.write.mode('overwrite').parquet('/mnt/confirmed-data/cumulative_medals_df')
entries_gender_df.write.mode('overwrite').parquet('/mnt/confirmed-data/entries_gender_df')
medals_df.write.mode('overwrite').parquet('/mnt/confirmed-data/medals_df')
medals_per_country.write.mode('overwrite').parquet('/mnt/confirmed-data/medals_per_country')
participants_per_discipline.write.mode('overwrite').parquet('/mnt/confirmed-data/participants_per_discipline')
pivot_df.write.mode('overwrite').parquet('/mnt/confirmed-data/pivot_df')
ranked_df.write.mode('overwrite').parquet('/mnt/confirmed-data/ranked_df')
team_df.write.mode('overwrite').parquet('/mnt/confirmed-data/team_df')


# COMMAND ----------

# MAGIC %md
# MAGIC ##Importing Matplotlib and Setting Up

# COMMAND ----------

import matplotlib.pyplot as plt
import pandas as pd

# Convert Spark DataFrames to Pandas DataFrames for Matplotlib
athletes_pd = athletes_df.toPandas()
medals_pd = medals_df.toPandas()
entries_gender_pd = entries_gender_df.toPandas()

# COMMAND ----------

# MAGIC %md
# MAGIC ##Distribution of Medals by Country (Stacked Bar Chart)

# COMMAND ----------

# Plotting the distribution of medals by country
medals_pd.plot(kind='bar', x='MedalCountry', y=['Gold', 'Silver', 'Bronze'], stacked=True, figsize=(10, 6))
plt.title('Distribution of Medals by Country')
plt.xlabel('Country')
plt.ylabel('Number of Medals')
plt.legend(title='Medal Type')
plt.xticks(rotation=45)
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ##Gender Participation in Different Disciplines (Pie Chart)

# COMMAND ----------

# Plotting gender participation in different disciplines using a pie chart
gender_totals = entries_gender_pd[['Female', 'Male']].sum()
gender_totals.plot(kind='pie', autopct='%1.1f%%', figsize=(8, 8), startangle=90)
plt.title('Gender Participation in Different Disciplines')
plt.ylabel('')  # Hide the y-label
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ##Top 10 Countries by Total Medals (Horizontal Bar Chart)

# COMMAND ----------

# Plotting top 10 countries by total medals using a horizontal bar chart
top_10_countries = medals_pd.nlargest(10, 'Total')
top_10_countries.plot(kind='barh', x='MedalCountry', y='Total', figsize=(10, 6))
plt.title('Top 10 Countries by Total Medals')
plt.xlabel('Total Medals')
plt.ylabel('Country')
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC # END
