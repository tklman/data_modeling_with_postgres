# ETL scripts for Song Play Analysis

### Purpose 
* The ETL scripts in this GIT repo are extracting data 
from raw song and log datasets and loading them in Postgres 
database tables. The data is normalized and is stored in Postgres
with 1 Fact table (songplays) and 4 Dimension tables (users, songs, artists, time).

### ETL Process (pipeline)
* First the database and tables are created via create_tables.py.
* Second, the songs and artists table are populated with data from the 'song_data' dataset.
* Third, the 'log_data' dataset is filtered by the action NextSong and the attribute 'ts' is converted to a
compatible Postgres data type.
* Fourth, the users table is populated with data from the 'log_data' dataset.
* The last step is to lookup the song_id and artist_id in order to insert the songplay actions log data into the
songplays table.

### Database design
* The entity relationship diagram below describes the database design.
![ERD for Song Play Analysis data](https://udacity-reviews-uploads.s3.us-west-2.amazonaws.com/_attachments/33760/1589342613/Song_ERD.png)

### How to run
* Download the necessary data sets.
* Clone this repo to a project dir.
* Setup a local or remote postgres db instance.
* Run create_tables.py first.
* And run etl.py second to load all the data in the database.
* Start querying the songplays fact table.


