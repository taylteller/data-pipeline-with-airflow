# Data Pipeline with Airflow, Redshift and AWS

## Introduction

A fictional music streaming company, Sparkify, has decided that it is time to introduce more automation and monitoring to their data warehouse ETL pipelines and come to the conclusion that the best tool to achieve this is Apache Airflow.

The goal is to create high grade data pipelines that are dynamic and built from reusable tasks, can be monitored, and allow easy backfills. The source data resides in S3 and needs to be processed in Sparkify's data warehouse in Amazon Redshift. The source datasets consist of JSON logs that tell about user activity in the application and JSON metadata about the songs the users listen to.

This project is part of the Udacity Data Engineering nanodegree.

## Databse Schema Design

Data is transferred from the two staging tables in S3 into a star schema (one fact table where each row references data from multiple dimension tables). This schema design facilitates analytical querying, as it denormalizes data according to the particular aspects of the data we want to dig further into. In other words, it's optimized to minimize the number of JOINs required to query the data. 

### Fact Table

**songplays** - records in log data associated with song plays i.e. records with page NextSong
- songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent

### Dimension Tables

**users** - users in the app
- user_id, first_name, last_name, gender, level

**songs** - songs in music database
- song_id, title, artist_id, year, duration

**artists** - artists in music database
- artist_id, name, location, latitude, longitude

**time** - timestamps of records in songplays broken down into specific units
- start_time, hour, day, week, month, year, weekday