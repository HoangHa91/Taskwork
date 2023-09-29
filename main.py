"""Entry point for the ETL application

Sample usage:
docker-compose run etl python main.py \
    --source /opt/data/activity.csv \
    --database warehouse
    --table user_activity
"""

# TODO: Implement a pipeline that loads the provided activity.csv file, performs the required
# transformations, and loads the result into the PostgreSQL table.

# Note: You can write the ETL flow with regular Python code, or you can also make use of a
# framework such as PySpark or others. The choice is yours.
