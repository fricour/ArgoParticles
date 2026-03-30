import sys
import pandas as pd
import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
from utils import remove_outliers, extract_LPM, assign_zone, open_nc_cached, WMO

# Extract particle data for each float
dfs = []

for wmo in WMO:
    try:
        ds = open_nc_cached(f"s3://argo-gdac-sandbox/pub/aux/coriolis/{wmo}/{wmo}_Rtraj_aux.nc")
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
