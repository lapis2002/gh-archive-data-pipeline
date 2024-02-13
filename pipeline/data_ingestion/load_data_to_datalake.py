import load_configuration
import requests
import polars as pl
import json

def get_data(cfg) -> None:
    year, month, day, hour = cfg["year"], cfg["month"], cfg["day"], cfg["hour"]
    headers = {"User-Agent": "Mozilla/5.0"}

    # hour is 0-23
    url = f"https://data.gharchive.org/{year}-{month:02d}-{day:02d}-{hour}.json.gz"
    print(f"Extracting data from {url}...")
    data = requests.get(url, headers=headers)
    if data.status_code != 200:
        raise ValueError(f"Failed to extract data from {url}")
    
    print(f"Data extracted from {url}")
    json_data = json.loads(data.text)
    print(json_data)
    df = pl.read_json(data)
    pdf.write_parquet(f"{year}-{month:02d}-{day:02d}-{hour}.parquet")
  
def export_to_datalake(cfg):  
    found = client.bucket_exists(bucket_name=cfg["bucket_name"])
    if not found:
        client.make_bucket(bucket_name=cfg["bucket_name"])
    else:
        print(f'Bucket {cfg["bucket_name"]} already exists, skip creating!')

    # Upload files.
    all_fps = glob(
        os.path.join(
            fake_data_cfg["folder_path"],
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

if __name__ == '__main__':
    CFG_FILE = 'resources/config.yaml'
    cfg = load_configuration.load_cfg_file(CFG_FILE)
    cfg["year"] = "2015"
    cfg["month"] = 1
    cfg["day"] = 2
    cfg["hour"] = "1"
    get_data(cfg)
    export_to_datalake(datalake_cfg)