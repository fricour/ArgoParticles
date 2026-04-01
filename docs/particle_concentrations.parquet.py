import sys
import pandas as pd
import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
from utils import remove_outliers, extract_LPM, open_nc_cached, WMO

# Extract particle data for each float
dfs = []

for wmo in WMO:
    try:
        ds = open_nc_cached(f"s3://argo-gdac-sandbox/pub/aux/coriolis/{wmo}/{wmo}_Rtraj_aux.nc")
        df = extract_LPM(ds)
        if len(df) > 0:
            dfs.append(df)
    except Exception as e:
        print(f"Error processing {wmo}: {e}", file=sys.stderr)
        continue

# Combine all dataframes
if len(dfs) == 0:
    print("No data found for any WMO", file=sys.stderr)
    sys.exit(1)
tmp = pd.concat(dfs, ignore_index=True)

# Reshape data: pivot longer
tmp = tmp.melt(
    id_vars=["park_depth", "cycle", "juld", "wmo"],
    value_vars=[col for col in tmp.columns if col.startswith("NP_Size_")],
    var_name="size",
    value_name="concentration",
).assign(size=lambda x: x["size"].str.split("_").str[2].astype(float))

# Remove outliers
tmp = (
    tmp.groupby(["wmo", "size", "cycle", "park_depth"], group_keys=True)
    .apply(lambda x: x.assign(concentration=remove_outliers(x["concentration"])))
    .dropna(subset=["concentration"])
    .reset_index(drop=False)
)
tmp = tmp.loc[:, ~tmp.columns.duplicated()]

# Drop columns not used in frontend queries
tmp = tmp.drop(columns=["level_4", "cycle"], errors="ignore")

# Sort by size, park_depth, wmo for better predicate pushdown in DuckDB-WASM
tmp = tmp.sort_values(["size", "park_depth", "wmo"]).reset_index(drop=True)

# Based on https://observablehq.observablehq.cloud/framework-example-loader-python-to-parquet/
# Write DataFrame to a temporary file-like object
buf = pa.BufferOutputStream()
table = pa.Table.from_pandas(tmp)
# Use row_group_size so each size class lands in its own row group
rows_per_size = len(tmp) // tmp["size"].nunique() if tmp["size"].nunique() > 0 else len(tmp)
pq.write_table(table, buf, compression="snappy", row_group_size=rows_per_size)

# Get the buffer as a bytes object
buf_bytes = buf.getvalue().to_pybytes()

# Write the bytes to standard output
sys.stdout.buffer.write(buf_bytes)
