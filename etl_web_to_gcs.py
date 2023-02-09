from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket

@task(retries=3)
def fetch(dataset_url:str)-> pd.DataFrame:
    """Read taxi data from web into pandas DataFrame"""

    df= pd.read_csv(dataset_url)
    return df

@task(log_prints=True)
def clean(df=pd.DataFrame)-> pd.DataFrame:
    """Fix dtype issues"""
    df['passenger_count'].fillna(0,inplace=True)
    df['tpep_pickup_datetime']=pd.to_datetime(df['tpep_pickup_datetime'])
    df['tpep_dropoff_datetime']=pd.to_datetime(df['tpep_dropoff_datetime'])
    df['passenger_count']=df['passenger_count'].astype('int64')
    df['payment_type']=df['payment_type'].astype('int64')

    #df['lpep_pickup_datetime']=pd.to_datetime(df['lpep_pickup_datetime'])
    #df['lpep_dropoff_datetime']=pd.to_datetime(df['lpep_dropoff_datetime'])
    print(df.head(2))
    print(f"columns: {df.dtypes}")
    print(f"rows: {len(df)}")
    return df
@task(retries=3,log_prints=True)
def write_local(df:pd.DataFrame,color:str, dataset_file:str)-> Path:
    """Write DataFrame out locally as parquet file"""
    # path_file=f"{dataset_file}.parquet"
    # path_dir=Path(f"data/{color}")
    # path_dir.mkdir(parents=True, exist_ok=True)
    path=Path(f"C:/Users/amarmol/OneDrive - Comisión Nacional de Bancos y Seguros (CNBS)/Documentos/Proyectos/de-zoomcamp/-de-zoomcamp2023/week2/data/{color}/{dataset_file}.parquet").as_posix() 
    df.to_parquet(path, compression="gzip")
    return path

@task()
def remote_path(color:str,data_file:str)-> Path:
    """Path for remote bucket"""
    path=Path(f"data/{color}/{data_file}.parquet").as_posix() 
    return path

@task(retries=3)
def write_gcs(path:Path, pathRemote:Path)-> None:
    """Upload local parquet file to GCS"""
    gcs_block = GcsBucket.load("zomm-gcs")
    gcs_block.upload_from_path(from_path=f"{path}",to_path=pathRemote)
    return

@flow(retries=3)
def etl_web_to_gcs(color:str, year:int, month:int)-> None:
    """The main ETL function"""
    dataset_file=f"{color}_tripdata_{year}-{month:02}"
    dataset_url =f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{color}/{dataset_file}.csv.gz"

    df=fetch(dataset_url)
    df_clena=clean(df)

    pathRemote= remote_path(color,dataset_file)
    path=write_local(df_clena,color, dataset_file)
    write_gcs(path,pathRemote)

@flow()
def etl_parent_flow(color:str="yellow",months:list[int]=[1,2],year:int=2020):
    for month in months:
        etl_web_to_gcs(color,year,month)

if __name__== '__main__':
    color="yellow"
    months=[1,2,3]
    year=2020
    etl_parent_flow(color,months,year)