import configparser

# CONFIG
from pathlib import Path

config = configparser.ConfigParser()
config.read(Path(__file__).with_name("dwh.cfg"))
ARN = config.get("CLUSTER", "DWH_ROLE_ARN")
# DROP TABLES

flights_table_drop = "DROP TABLE IF EXISTS flights"
covid_table_drop = "DROP TABLE IF EXISTS covid"
cancellation_table_drop = "DROP TABLE IF EXISTS cancellation"
state_abbreviation_table_drop = "DROP TABLE IF EXISTS state_abbreviation"
city_market_id_table_drop = "DROP TABLE IF EXISTS city_market_id"
airport_seq_id_table_drop = "DROP TABLE IF EXISTS airport_seq_id"
airport_id_table_drop = "DROP TABLE IF EXISTS airport_id"
wac_table_drop = "DROP TABLE IF EXISTS world_area_codes"
state_fips_table_drop = "DROP TABLE IF EXISTS state_fips"

# CREATE TABLES
covid_table_create = ("""
    CREATE TABLE IF NOT EXISTS covid (
        country CHARACTER VARYING,
        continent CHARACTER VARYING,
        geoId CHARACTER VARYING,
        cases INTEGER,
        deaths  INTEGER ,
        dateRep  DATE ,
        day INTEGER ,
        month INTEGER ,
        year  INTEGER
    )
     COMPOUND SORTKEY(year,month,country)
""")



flights_table_create = ("""
    CREATE TABLE IF NOT EXISTS flights (
    fl_date DATE NOT NULL ,
    op_unique_carrier CHARACTER VARYING NOT NULL,
    tail_num CHARACTER VARYING,
    origin_airport_id INTEGER NOT NULL,
    origin_airport_seq_id INTEGER NOT NULL, 
    origin_city_market_id INTEGER NOT NULL,
    origin CHARACTER VARYING NOT NULL,
    origin_city_name CHARACTER VARYING NOT NULL,
    origin_state_abr CHARACTER VARYING NOT NULL,
    origin_state_fips CHARACTER VARYING NOT NULL,
    origin_state_nm CHARACTER VARYING NOT NULL,
    origin_wac CHARACTER VARYING NOT NULL,
    dest_airport_id INTEGER NOT NULL,
    dest_airport_seq_id INTEGER NOT NULL,
    dest_city_market_id INTEGER NOT NULL,
    dest CHARACTER VARYING NOT NULL,
    dest_city_name CHARACTER VARYING NOT NULL,
    dest_state_abr CHARACTER VARYING NOT NULL,
    dest_state_fips CHARACTER VARYING NOT NULL,
    dest_state_nm CHARACTER VARYING NOT NULL,
    dest_wac CHARACTER VARYING NOT NULL,
    dep_time CHARACTER VARYING,
    dep_delay FLOAT, 
    dep_delay_new FLOAT, 
    dep_del15 FLOAT, 
    arr_time CHARACTER VARYING,
    arr_delay FLOAT, 
    arr_delay_new FLOAT, 
    arr_del15 FLOAT, 
    cancelled INTEGER NOT NULL, 
    cancellation_code CHARACTER VARYING,
    year INTEGER NOT NULL, 
    month INTEGER NOT NULL
    )
""")

cancellation_table_create = ("""
    CREATE TABLE IF NOT EXISTS cancellation (
        code  CHARACTER VARYING,
        description CHARACTER VARYING
        
    )
""")

state_fips_table_create = ("""
    CREATE TABLE IF NOT EXISTS state_fips (
        code  CHARACTER VARYING,
        description CHARACTER VARYING
    )
""")

wac_table_create = ("""
    CREATE TABLE IF NOT EXISTS world_area_codes (
     code  CHARACTER VARYING,
     description CHARACTER VARYING
    )
""")

state_abbreviation_table_create = ("""
    CREATE TABLE IF NOT EXISTS state_abbreviation (
     code  CHARACTER VARYING,
     description CHARACTER VARYING
    )
""")

city_market_id_table_create = ("""
    CREATE TABLE IF NOT EXISTS city_market_id (
     code  CHARACTER VARYING,
     description CHARACTER VARYING
    )
""")

airport_seq_id_table_create = ("""
    CREATE TABLE IF NOT EXISTS airport_seq_id (
     code  CHARACTER VARYING,
     description CHARACTER VARYING
    )
""")

airport_id_table_create = ("""
    CREATE TABLE IF NOT EXISTS airport_id (
     code  CHARACTER VARYING,
     description CHARACTER VARYING
    )
""")

#  TABLES COPY FROM S3

covid_copy = ("""
    COPY covid
    FROM '{}'
    IAM_ROLE '{}'
    FORMAT AS PARQUET
""").format(config.get("S3", "COVID_DATA"), ARN)

flights_copy = ("""
    COPY flights
    FROM '{}'
    IAM_ROLE '{}'
    FORMAT AS PARQUET
""").format(config.get("S3", "FLIGHTS_DATA"), ARN)

cancellation_copy = ("""
    COPY cancellation
    FROM '{}'
    IAM_ROLE '{}'
    CSV
    IGNOREHEADER 1
    """).format(config.get("S3", "CANCELLATION"), ARN)

state_fips_copy = ("""
    COPY state_fips
    FROM '{}'
    IAM_ROLE '{}'
    CSV
    IGNOREHEADER 1
    """).format(config.get("S3", "STATE_FIPS"), ARN)

wac_copy = ("""
    COPY world_area_codes
    FROM '{}'
    IAM_ROLE '{}'
    CSV
    IGNOREHEADER 1
    """).format(config.get("S3", "WAC"), ARN)

state_abbreviation_copy = ("""
    COPY state_abbreviation
    FROM '{}'
    IAM_ROLE '{}'
    CSV
    IGNOREHEADER 1
    """).format(config.get("S3", "ABBR"), ARN)

city_market_id_copy = ("""
    COPY city_market_id
    FROM '{}'
    IAM_ROLE '{}'
    CSV
    IGNOREHEADER 1
    """).format(config.get("S3", "MARKET_ID"), ARN)

airport_id_copy = ("""
    COPY airport_id
    FROM '{}'
    IAM_ROLE '{}'
    CSV
    IGNOREHEADER 1
    """).format(config.get("S3", "AIRPORT_ID"), ARN)

airport_seq_id_copy = ("""
    COPY airport_seq_id
    FROM '{}'
    IAM_ROLE '{}'
    CSV
    IGNOREHEADER 1
    """).format(config.get("S3", "AIRPORT_SEQ_ID"), ARN)

# DATA QUALITY CHECKS

# QUERY LISTS

create_table_queries = [('airport_id', airport_id_table_create), ('airport_seq_id', airport_seq_id_table_create),
                        ('cancellation', cancellation_table_create),
                        ('city_market_id', city_market_id_table_create),
                        ('flights', flights_table_create), ('covid', covid_table_create),
                        ('state_abbreviation', state_abbreviation_table_create),
                        ('world_area_codes', wac_table_create),
                        ('state_fips', state_fips_table_create)]

drop_table_queries = [('airport_id', airport_id_table_drop), ('airport_seq_id', airport_seq_id_table_drop),
                      ('cancellation', cancellation_table_drop),
                      ('city_market_id', city_market_id_table_drop),
                      ('flights', flights_table_drop), ('covid', covid_table_drop),
                      ('state_abbreviation', state_abbreviation_table_drop),
                      ('world_area_codes', wac_table_drop),
                      ('state_fips', state_fips_table_drop)]

copy_table_queries = [('airport_id', airport_id_copy), ('airport_seq_id', airport_seq_id_copy),
                      ('cancellation', cancellation_copy),
                      ('city_market_id', city_market_id_copy),
                      ('flights', flights_copy), ('covid', covid_copy),
                      ('state_abbreviation', state_abbreviation_copy),
                      ('world_area_codes', wac_copy),
                      ('state_fips', state_fips_copy)]
