import pandas as pd
import sys
from utils import download_s3_cached, WMO

# Read the text file (cached)
local_path = download_s3_cached("s3://argo-gdac-sandbox/pub/idx/argo_bio-profile_index.txt")
df = pd.read_csv(local_path, comment="#", sep=",")

# Process the dataframe
tmp = (
    df.assign(
        wmo=lambda x: x["file"].astype(str).str.extract(r"(\d{7})")[0].astype(float),
        cycle=lambda x: x["file"]
        .astype(str)
        .str.extract(r"(\d{3})(?=\.nc$)")[0]
        .astype(float),
        date=lambda x: pd.to_datetime(
            x["date_update"].astype(str).str[:8], format="%Y%m%d"
        ),
    )[["wmo", "cycle", "latitude", "longitude", "date"]]
    .query("wmo in @WMO")
    .dropna()
    .astype({"wmo": int, "cycle": int})
    .reset_index(drop=True)
)

# Output to stdout for Observable Framework
tmp.to_csv(sys.stdout, index=False)
