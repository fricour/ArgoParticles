import s3fs
import pandas as pd
import sys
from utils import WMO

fs = s3fs.S3FileSystem(anon=True)

# Read the text file
with fs.open("s3://argo-gdac-sandbox/pub/idx/argo_bio-profile_index.txt", "rb") as f:
    df = pd.read_csv(f, comment="#", sep=",")

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
