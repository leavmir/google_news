# google_news
To install this project you will need to pull my repository from docker:
docker pull leavmir/google-news:latest

Everything else is inside git repository.

There are 3 jobs inside this project.

First job parse_news parses news from Google Api and writes them to json.

Second job change_json_to_parq transforms received data, drops unnecessary columns and then writes output into parquet.

Third job write_to_postgre groups received data by source and counts number of articles. After that final output is saved to Postgres database.

This project uses Apache Airflow as workflow management.

