import load_configuration
import requests
import polars as pl
import gzip
from minio import Minio
from glob import glob
import os
import shutil

def get_data(cfg) -> None:
    year, month, day, hour = cfg["timestamp"]["year"], cfg["timestamp"]["month"], cfg["timestamp"]["day"], cfg["timestamp"]["hour"]
    headers = {"User-Agent": "Mozilla/5.0"}

    # hour is 0-23
    url = f"https://data.gharchive.org/{year}-{month:02d}-{day:02d}-{hour}.json.gz"
    print(f"Extracting data from {url}...")
    data = requests.get(url, headers=headers)
    if data.status_code != 200:
        raise ValueError(f"Failed to extract data from {url}")
    
    print(f"Data extracted from {url}")
    

    json_data = str(gzip.decompress(data.content))

    df = pl.DataFrame(json_data)
    folder_name = f"resources/sample_data/{cfg["folder_name"]}"
    isExist = os.path.exists(folder_name)
    if not isExist:
    # Create a new directory because it does not exist
        os.makedirs(folder_name)
    df.write_parquet(f"{folder_name}/{year}-{month:02d}-{day:02d}-{hour}.parquet")
  
def export_to_datalake(cfg):  
    client = Minio(
        endpoint=cfg["endpoint"],
        access_key=cfg["access_key"],
        secret_key=cfg["secret_key"],
        secure=False,
    )
    found = client.bucket_exists(bucket_name=cfg["bucket_name"])
    if not found:
        client.make_bucket(bucket_name=cfg["bucket_name"])
    else:
        print(f'Bucket {cfg["bucket_name"]} already exists, skip creating!')

    folder_path = f"resources/sample_data/{cfg["folder_name"]}"
    # Upload files
    all_fps = glob(
        os.path.join(
            folder_path,
            "*.parquet"
        )
    )
    for fp in all_fps:
        print(f"Uploading {fp}")
        client.fput_object(
            bucket_name=cfg["bucket_name"],
            object_name=os.path.join(
                cfg["folder_name"],
                os.path.basename(fp)
            ),
            file_path=fp
        )
        print(f"Finished uploading {fp}")
    
    # Clean up after uploading
    try:
        shutil.rmtree(folder_path)
    except OSError as e:
        # If it fails, inform the user.
        print("Error: %s - %s." % (e.filename, e.strerror))

if __name__ == '__main__':
    CFG_FILE = 'resources/config.yaml'
    cfg = load_configuration.load_cfg_file(CFG_FILE)
    folder_name = f"{cfg["timestamp"]["year"]}-{cfg["timestamp"]["month"]:02d}-{cfg["timestamp"]["day"]:02d}"
    cfg["folder_name"] = folder_name
    cfg["datalake"]["folder_name"] = folder_name

    get_data(cfg)
    export_to_datalake(cfg["datalake"])