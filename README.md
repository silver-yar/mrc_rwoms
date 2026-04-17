# Retail Work Order Management System

## Run
```
python create_database.py
```

## ChartDB
If you want to generate your own ERD you can clone the ChartDB repository and use the output of **chartdb_script.sql** in your local instance of ChartDB.

### How to Generate ChartDB Query Output
```
sqlite3 rwoms_database.db < chartdb_script.sql
```
