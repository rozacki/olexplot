# Intro
Pipeline to transform olexplot data into geojson
Step:
1. Generate csv file from olexplot data
2. Load into postgres
3. Generate geojson files from 

# Generate csv file from olexplot data
TBA 

#  Load into postgres
## Open session with postgres
psql --d default

## Create postgres table
 - drop table if exists olexplot;
 - create table olexplot(long float, lat float, time timestamp,  cursor varchar, type varchar, name varchar);

## Prepare table
 - truncate table olexplot;

## To load data to postgres
 - copy olexplot(long, lat, time,  cursor, type, name) from '/Users/chris.rozacki/projects/olexlpot/data/olexplot.csv'  delimiter ',' csv header QUOTE '''';

## Check if data is loded
- select count(*) from olexplot;
  count
---------
 10000
(1 row)

# Generate csv data from olexplot

# Load csv data to postgres

# Generate geojson files

