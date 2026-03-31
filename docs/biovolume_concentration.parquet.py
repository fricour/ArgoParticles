import sys
import pandas as pd
import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
from utils import extract_biovolume, open_nc_cached, WMO

# Extract particle data for each float
dfs = []

for wmo in WMO:
    try:
        ds = open_nc_cached(f"s3://argo-gdac-sandbox/pub/aux/coriolis/{wmo}/{wmo}_Rtraj_aux.nc")
        df = extract_biovolume(ds)
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
    value_vars=[col for col in np.array(tmp.columns[6:])],
    var_name="taxo_class",
    value_name="biovolume"
)

tmp = tmp.loc[:, ~tmp.columns.duplicated()]

# Based on https://observablehq.observablehq.cloud/framework-example-loader-python-to-parquet/
# Write DataFrame to a temporary file-like object
buf = pa.BufferOutputStream()
table = pa.Table.from_pandas(tmp)
pq.write_table(table, buf, compression="snappy")

# Get the buffer as a bytes object
buf_bytes = buf.getvalue().to_pybytes()

# Write the bytes to standard output
sys.stdout.buffer.write(buf_bytes)
