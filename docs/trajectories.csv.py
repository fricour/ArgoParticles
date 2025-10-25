import s3fs
import pandas as pd
import sys

fs = s3fs.S3FileSystem(anon=True)

# Read the text file
with fs.open("s3://argo-gdac-sandbox/pub/idx/argo_bio-profile_index.txt", "rb") as f:
    df = pd.read_csv(f, comment="#", sep=",")

# Define WMO list
WMO = [
    1902578,
    1902593,
    1902601,
    1902637,
    1902685,
    2903783,
    2903787,
    2903794,
    3902471,
    3902498,
    4903634,
    4903657,
    4903658,
    4903660,
    4903739,
    4903740,
    5906970,
    6904240,
    6904241,
    6990503,
    6990514,
    7901028,
]

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
    .reset_index()
)

# Output to stdout for Observable Framework
tmp.to_csv(sys.stdout, index=False)
