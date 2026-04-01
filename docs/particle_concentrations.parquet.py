import sys
import pandas as pd
import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
from utils import extract_LPM, get_launch_date, open_nc_cached, WMO

# Extract particle data for each float
dfs = []

for wmo in WMO:
    try:
        ds = open_nc_cached(f"s3://argo-gdac-sandbox/pub/aux/coriolis/{wmo}/{wmo}_Rtraj_aux.nc")
        launch_date = get_launch_date(wmo)
        df = extract_LPM(ds, launch_date=launch_date)
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

tmp = tmp.drop(columns=["cycle"], errors="ignore")

# Compute boxplot stats for a group
def boxplot_stats(group):
    vals = group["concentration"]
    q1 = vals.quantile(0.25)
    q3 = vals.quantile(0.75)
    iqr = q3 - q1
    median = vals.median()
    whisker_lo = vals[vals >= q1 - 1.5 * iqr].min()
    whisker_hi = vals[vals <= q3 + 1.5 * iqr].max()
    return pd.Series({
        "q1": q1,
        "median": median,
        "q3": q3,
        "whisker_lo": whisker_lo,
        "whisker_hi": whisker_hi,
        "n": len(vals),
    })

# Compute stats at daily, weekly and monthly resolution
all_stats = []
for bin_label, period in [("daily", "D"), ("weekly", "W"), ("monthly", "M")]:
    if period == "D":
        tmp["period"] = tmp["juld"].dt.floor("D")
    elif period == "W":
        tmp["period"] = tmp["juld"].dt.to_period("W").dt.to_timestamp()
    else:
        tmp["period"] = tmp["juld"].dt.to_period("M").dt.to_timestamp()

    s = (
        tmp.groupby(["period", "wmo", "size", "park_depth"])
        .apply(boxplot_stats)
        .reset_index()
    )
    s["bin"] = bin_label
    all_stats.append(s)

stats = pd.concat(all_stats, ignore_index=True)

# Sort for efficient predicate pushdown
stats = stats.sort_values(["bin", "size", "park_depth", "wmo", "period"]).reset_index(drop=True)

# Write to parquet
buf = pa.BufferOutputStream()
table = pa.Table.from_pandas(stats)
pq.write_table(table, buf, compression="snappy")
buf_bytes = buf.getvalue().to_pybytes()
sys.stdout.buffer.write(buf_bytes)
