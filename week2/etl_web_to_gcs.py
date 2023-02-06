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
    df['tpep_pickup_datetime']=pd.to_datetime(df['tpep_pickup_datetime'])
    df['tpep_dropoff_datetime']=pd.to_datetime(df['tpep_dropoff_datetime'])
    print(df.head(2))
    print(f"columns: {df.dtypes}")
    print(f"rows: {len(df)}")
    return df
@task()
def write_local(df:pd.DataFrame,color:str, dataset_file:str)-> Path:
    """Write DataFrame out locally as parquet file"""
    path=Path(f"C:/Users/amarmol/OneDrive - Comisión Nacional de Bancos y Seguros (CNBS)/Documentos/Proyectos/de-zoomcamp/-de-zoomcamp2023/week 2/data/{color}/{dataset_file}.parquet").as_posix() 
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

@flow()
def etl_web_to_gcs()-> None:
    """The main ETL function"""
    color="green"
    year=2020
    month=11
    dataset_file=f"{color}_tripdata_{year}-{month:02}"
    dataset_url =f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{color}/{dataset_file}.csv.gz"

    df=fetch(dataset_url)
    df_clena=clean(df)

    pathRemote= remote_path(color,dataset_file)
    path=write_local(df_clena,color, dataset_file)
    write_gcs(path,pathRemote)

if __name__== '__main__':
    etl_web_to_gcs()
