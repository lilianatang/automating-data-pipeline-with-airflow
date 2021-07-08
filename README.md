# INTRODUCTION
### Purpose of the project:
This project is to create high grade data pipelines that are dynamic and built from reusable tasks, can be monitored, and allow easy backfills.
### What is Sparkify?
Sparkify is a startup that has decided that it is time to introduce more automation and monitoring to their data warehouse ETL pipelines and come to the conclusion that the best tool to achieve this is Apache Airflow. The source data resides in S3 and needs to be processed in Sparkify's data warehouse in Amazon Redshift. The source datasets consist of JSON logs that tell about user activity in the application and JSON metadata about the songs the users listen to.
# DATABASE SCHEMA DESIGN & ETL PROCESS
### Database Schema Design (Star Schema)
##### **Fact Table**
songplays (songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)** - records in log data associated with song plays i.e. records with page NextSong
##### **Dimension Tables**
1. users (user_id, first_name, last_name, gender, level) - users in the app
2. songs (song_id, title, artist_id, year, duration) - songs in music database
3. artists (artist_id, name, location, latitude, longitude) - artists in music database
4. time (start_time, hour, day, week, month, year, weekday) - timestamps of records in **songplays** broken down into specific units
### Operators
1. Stage Operator resides in plugins/operators/stage_redshift.py. Stage Operator loads any JSON formatted files from S3 to Amazon Redshift. The operator creates and runs a SQL COPY statement based on the parameters provided. 
2. Fact Operator resides in plugins/operators/load_fact.py. Fact Operator runs INSERT statement to populate data in songplays table.
3. Dimension Operator resides in plugins/operators/load_dimension.py. Dimension Operator runs INSERT statements to populate data in users, songs, artists, time table.
4. Data Quality Operator resides in plugins/operators/data_quality.py. Data Quality Operator runs checks on the data itself.

# FILES IN REPOSITORY
* create_table.py creates fact and dimension tables for the star schema in Redshift.
* sql_queries.py defines SQL statements, which will be imported into the create_table.py
* README.md provides discussion on the project
* stage_redshift.py loads any JSON formatted files from S3 to Amazon Redshift. The operator creates and runs a SQL COPY statement based on the parameters provided. 
* load_fact.py runs INSERT statement to populate data in songplays table.
* load_dimension.py runs INSERT statements to populate data in users, songs, artists, time table.
* data_quality.py runs checks on the data itself.
* main_dag.py defines the order and execution of operators above mentioned.
# HOW TO RUN THE PYTHON SCRIPTS
* In the command line, run python create_tables.py to create table schemas.
* In the command line, run /opt/airflow/start.sh to start the Airflow web server. Once the Airflow web server is ready, you can access the Airflow UI by clicking on the blue Access Airflow button.
