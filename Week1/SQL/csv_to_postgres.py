# %%
import pathlib
import subprocess
PATH_DIRECTORY = pathlib.Path().resolve()
DATA_DIRECTORY = PATH_DIRECTORY / "Data"
subprocess.call('./Data/get_data.sh', shell=True)

# %%
# Load green tripdata
import pandas as pd
tripdata_df = pd.read_csv(DATA_DIRECTORY / "green_tripdata_2019-01.csv.gz", compression='gzip')
tripdata_df.head(10)

# %%
# transform lpep_pickup_datetime and lpep_dropoff_datetime to datetime type
tripdata_df['lpep_pickup_datetime'] = pd.to_datetime(tripdata_df['lpep_pickup_datetime'])
tripdata_df['lpep_dropoff_datetime'] = pd.to_datetime(tripdata_df['lpep_dropoff_datetime'])
print(tripdata_df.dtypes)

# %%
# Load taxi zone
zone_df = pd.read_csv(DATA_DIRECTORY / "taxi+_zone_lookup.csv", delimiter=',')
zone_df.head(10)

# %%
# Replacement dictionary to map pandas dtypes to SQL dtypes
replacement = {
    'object': 'varchar',
    'float64': 'float',
    'int64': 'int',
    'datetime64[ns]': 'timestamp',
    'timedelta64[ns]': 'varchar',
}

col_tripdata = ", ".join("{} {}"
              .format(n, d) for (n ,d)
              in zip(tripdata_df.columns, tripdata_df.dtypes.replace(replacement)))

col_zone = ", ".join("{} {}"
          .format(n, d) for (n, d)
          in zip(zone_df.columns, zone_df.dtypes.replace(replacement)))

# %%
# Save tripdata df to csv
tripdata_df.to_csv(DATA_DIRECTORY / "tripdata.csv",
                   header=tripdata_df.columns,
                   index=False, encoding='utf-8')

# Check csv created
check = pd.read_csv(DATA_DIRECTORY / "tripdata.csv")
check.head(10)

# %%
import psycopg2

host = 'localhost'
default_db = 'postgres'
user = 'postgres'
password = ''

dbname = 'zoomcamp_week1'
tbname_trip = 'green_tripdata'
tbname_zone = 'taxi_zone'

# %%
def create_database():
    conn = None
    try:
        conn = psycopg2.connect(
            host=host,
            dbname=default_db,
            user=user,
            password=password
        )

        conn.autocommit = True
        with conn.cursor() as cur:
            cur.execute("CREATE DATABASE %s;" % dbname)

    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    
    finally:
        if conn is not None:
            conn.close()
        print("PotsgreSQL connection is closed")

# %%
def create_tables():
    conn = None
    try:
        conn_str = "host=%s dbname=%s user=%s password=%s" % (host, dbname, user, password)
        with psycopg2.connect(conn_str) as conn:
            with conn.cursor() as cur:
                
                # Create table trip data
                cur.execute("CREATE TABLE %s (%s);" % (tbname_trip, col_tripdata))
                print("Table tripdata created successfully")

                # Open csv tripdata
                tripdata_file = open(DATA_DIRECTORY / "tripdata.csv")
                
                # SQL Copy tripdata to db
                SQL = "COPY %s FROM STDIN WITH CSV HEADER DELIMITER AS ','" % tbname_trip
                cur.copy_expert(sql=SQL, file=tripdata_file)

                # Create table taxi zone
                cur.execute("CREATE TABLE %s (%s);" % (tbname_zone, col_zone))
                print("Table taxi zone created successfully")

                # # Open csv taxi zone
                zone_file = open(DATA_DIRECTORY / "taxi+_zone_lookup.csv")

                # # SQL Copy taxi zone to db
                SQL = "COPY %s FROM STDIN WITH CSV HEADER DELIMITER AS ','" % tbname_zone
                cur.copy_expert(sql=SQL, file=zone_file)
    
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    
    finally:
        if conn is not None:
            conn.close()
        print("PostgreSQL connection is closed")

# %%
def drop_tables():
    conn = None
    try:
        conn_str = "host=%s dbname=%s user=%s password=%s" % (host, dbname, user, password)
        with psycopg2.connect(conn_str) as conn:
            with conn.cursor() as cur:
                
                # Drop table trip data
                cur.execute("DROP TABLE IF EXISTS %s;" % tbname_trip)

                # Drop table taxi zone
                cur.execute("DROP TABLE IF EXISTS %s;" % tbname_zone)
                
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    
    finally:
        if conn is not None:
            conn.close()
        print("PostgreSQL connection is closed")

# %%
create_database()
drop_tables()
create_tables()


