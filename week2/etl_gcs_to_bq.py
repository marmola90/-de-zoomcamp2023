from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from prefect_gcp import GcpCredentials

@task()
def extract_from_gcs(color:str,year:int,month:int) -> Path:
    """Download trip data from GCS"""
    #gcs_path=f"data/{color}/{color}_tripdata_{year}-{month:02}.parquet"
    gcs_path=f"{color}/{color}_tripdata_{year}-{month:02}.parquet"
    gcs_block = GcsBucket.load("zomm-gcs")
    gcs_block.get_directory(from_path=gcs_path, local_path=f"../")
    return Path(f"../{gcs_path}")

@task()
def transform(path:Path) -> pd.DataFrame:
    """Data cleaning example"""
    df= pd.read_parquet(path)
   # print(f"pre: missing passenger count: {df['passenger_count'].isna().sum()}")
    #df['passenger_count'].fillna(0,inplace=True)
    #print(f"post: missing passenger count: {df['passenger_count'].isna().sum()}")

    return df

@task()
def write_bq(df:pd.DataFrame) -> None:
    """Write DataFrame to BigQuery"""
    gcp_credentials_block = GcpCredentials.load("zoom-gcp-creds")

    df.to_gbq(
        #destination_table="trips_data_all.yellow_taxi_data",
        destination_table="trips_data_all.green_tripdata",
        project_id="de-zoomcampam",
        credentials=gcp_credentials_block.get_credentials_from_service_account(),
        chunksize=500_000,
        if_exists="append"
    )


@flow()
def etl_gcs_to_bq(color:str, year:int, month:int)-> None:
    """Main ETL flow to load data into Big Query"""

    path=extract_from_gcs(color,year,month)
    df = transform(path)
    write_bq(df)
    #rows=len(df)
    #return rows

@flow(log_prints=True)
def etl_principal(color:str="green",year:int=2019,months:list[int]=[1,2,3])-> None:
    """Main Flow"""
    #rows=0
    for month in months:
        etl_gcs_to_bq(color,year,month)
    
   # print(f"total Rows: {rows}")

if __name__== '__main__':
    color:str="green"
    months:list[int]=[1,2,3,4,5,6,7,8,9,10,11,12]
    year:int=2019
    etl_principal(color,year,months)