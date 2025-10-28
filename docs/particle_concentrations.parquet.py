import sys
import pandas as pd
import numpy as np
import xarray as xr
import s3fs
import pyarrow as pa
import pyarrow.parquet as pq
from utils import remove_outliers, extract_LPM, WMO

fs = s3fs.S3FileSystem(anon=True)

# Extract particle data for each float
dfs = []

for wmo in WMO:
    try:
        with fs.open(
            f"s3://argo-gdac-sandbox/pub/aux/coriolis/{wmo}/{wmo}_Rtraj_aux.nc", "rb"
        ) as f:
            ds = xr.open_dataset(f)
            df = extract_LPM(ds)
            dfs.append(df)
    except Exception as e:
        print(f"Error processing {wmo}: {e}", file=sys.stderr)
        continue

# Combine all dataframes
tmp = pd.concat(dfs, ignore_index=True)

# Reshape data: pivot longer
tmp = tmp.melt(
    id_vars=["depth", "park_depth", "cycle", "juld", "wmo"],
    value_vars=[col for col in tmp.columns if col.startswith("NP_Size_")],
    var_name="size",
    value_name="concentration",
).assign(size=lambda x: x["size"].str.split("_").str[2].astype(float))

# Remove outliers
tmp = (
    tmp.groupby(["wmo", "size", "cycle", "park_depth"], group_keys=False)
    .apply(lambda x: x.assign(concentration=remove_outliers(x["concentration"])))
    .dropna(subset=["concentration"])
    .reset_index(drop=True)
)


# Add oceanic zones
def assign_zone(wmo):
    zone_map = {
        "Labrador Sea": [6904240, 6904241, 1902578, 4903634],
        "Arabian Sea": [4903660, 6990514],
        "Guinea Dome": [3902498, 1902601],
        "Apero mission": [1902637, 4903740, 4903739],
        "West Kerguelen": [2903787, 4903657],
        "East Kerguelen": [1902593, 4903658],
        "Tropical Indian Ocean": [5906970, 3902473, 6990503, 3902471],
        "South Pacific Gyre": [2903783],
        "California Current": [6903093, 6903094],
        "Nordic Seas": [7901028, 2903794],
        "North Pacific Gyre": [1902685],
    }

    for zone, wmos in zone_map.items():
        if wmo in wmos:
            return zone
    return None


tmp["zone"] = tmp["wmo"].astype(int).apply(assign_zone)

# Based on https://observablehq.observablehq.cloud/framework-example-loader-python-to-parquet/
# Write DataFrame to a temporary file-like object
buf = pa.BufferOutputStream()
table = pa.Table.from_pandas(tmp)
pq.write_table(table, buf, compression="snappy")

# Get the buffer as a bytes object
buf_bytes = buf.getvalue().to_pybytes()

# Write the bytes to standard output
sys.stdout.buffer.write(buf_bytes)
