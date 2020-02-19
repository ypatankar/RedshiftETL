# Data Modeling with Amazon Redshift

The goal of this project is to build ETL pipeline that extracts data from Amazon S3, stages it in Amazon Redshift and transforming data into dimensional tables for analysis. The data is about song listening activity and song/artist information. The data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs. 

## Getting Started

Cloning the repository will get you a copy of the project up and running on your local machine for development and testing purposes. 

- `git@github.com:ypatankar/RedshiftETL.git`
- `https://github.com/ypatankar/RedshiftETL.git`

### Prerequisites

* python 3.7
* AWS account
* Amazon Redshift 4 node cluster and IAM role to let Redshift access S3 bucket
* Open an incoming TCP port in security group to access the cluster endpoint

### Contents

* `sql_queries.py` : All the sql queries are stored in this file and are referenced by other files.
* `create_tables.py` : Drops and creates your tables. Can be used to reset tables.
* `etl.py` : Processes all data files, loads data in Amazon Redshift tables
* `dwh.cfg` : Contains Redshift cluster configuration and IAM role information

### Database Design
![alt text](https://github.com/ypatankar/PostgreETL/blob/master/Database%20Structure.png)

Table Name | Sort Key | Distribution Style
--- | --- | ---
fact_songplay | `start_time` | distkey
dim_song | `song_id` | distkey
dim_artist | `artist_id` | ALL
dim_user | `user_id` | ALL
dim_time | `start_time` | ALL

### Deployment Steps
1. Execute `create_tables.py` to create and drop tables
3. Run `etl.py` to process the data set loading

### Executing SQL for analytics
* Finding popular songs:
```
SELECT title AS song_title, 
       COUNT(songplay_id) AS count
FROM fact_songplay sp INNER JOIN dim_song s
       ON sp.song_id = s.song_id
GROUP BY title
ORDER BY 2 DESC;
```


* User activity by location
```
SELECT location, 
         COUNT(songplay_id) AS user_activity
FROM fact_songplay
GROUP BY location
ORDER BY COUNT(songplay_id) desc;
```