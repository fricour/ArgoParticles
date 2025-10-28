import numpy as np
import pandas as pd
import xarray as xr
import sys
import s3fs
import pyarrow as pa
import pyarrow.parquet as pq
from utils import WMO

fs = s3fs.S3FileSystem(anon=True)

fs = s3fs.S3FileSystem(anon=True)


def slide(x, k, fun, n=1, **kwargs):
    """
    Apply a function in a sliding window along a vector.

    Allows to compute a moving average, moving median, or even moving standard
    deviation, etc. in a generic way.

    Parameters:
    -----------
    x : array-like
        Input numeric vector
    k : int
        Order of the window; the window size is 2k+1
    fun : callable
        Function to apply in the moving window
    n : int, optional
        Number of times to pass the function over the data (default=1)
    **kwargs : dict
        Arguments passed to fun (e.g., na.rm equivalent would be handled by nanmean, nanmedian, etc.)

    Returns:
    --------
    np.ndarray
        The data passed through fun, n times
    """
    x = np.array(x, dtype=float)

    if n >= 1:
        for t in range(n):
            # Pad the extremities of data with NaN
            x_padded = np.concatenate([np.full(k, np.nan), x, np.full(k, np.nan)])

            # Apply the rolling function
            result = []
            for i in range(k, len(x_padded) - k):
                window = x_padded[(i - k):(i + k + 1)]
                result.append(fun(window, **kwargs))

            x = np.array(result)

    return x


def despike(x, k=3, method="median", threshold=2):
    """
    Despike data using a reference calculated with a moving window.

    This is a simplified version inspired by oce::despike. The oce package uses
    a more sophisticated approach, but this captures the essential functionality.

    Parameters:
    -----------
    x : array-like
        Input numeric vector
    k : int
        Order of the window; the window size is 2k+1 (default=3)
    method : str
        Method for reference calculation: 'median' or 'mean' (default='median')
    threshold : float
        Number of standard deviations for spike detection (default=2)

    Returns:
    --------
    np.ndarray
        Despiked data
    """
    x = np.array(x, dtype=float)

    # Calculate reference using sliding window
    if method == "median":
        reference = slide(x, k, np.nanmedian)
    else:
        reference = slide(x, k, np.nanmean)

    # Calculate residuals
    residuals = x - reference

    # Calculate MAD (Median Absolute Deviation) for robust threshold
    mad = np.nanmedian(np.abs(residuals - np.nanmedian(residuals)))

    # Identify spikes (using MAD-based threshold, more robust than SD)
    threshold_value = threshold * mad * 1.4826  # 1.4826 converts MAD to SD equivalent
    is_spike = np.abs(residuals) > threshold_value

    # Replace spikes with reference values
    x_despiked = x.copy()
    x_despiked[is_spike] = reference[is_spike]

    return x_despiked


def extract_cp_data(wmo, ds):
    """
    Extract transmissometer data from netCDF file.

    Parameters:
    -----------
    wmo : str
        WMO float identifier
    ds

    Returns:
    --------
    pd.DataFrame
        DataFrame with CP660 data
    """

    # Extract data
    value = ds["CP660"].values
    depth = ds["PRES"].values
    mc = ds["MEASUREMENT_CODE"].values
    juld = ds["JULD"].values
    cycle = ds["CYCLE_NUMBER"].values

    # Create DataFrame
    df = pd.DataFrame(
        {
            "wmo": wmo,
            "cycle": cycle,
            "juld": juld,
            "mc": mc,
            "depth": depth,
            "cp": value,
        }
    )

    # Clean data
    df = df[(df["cycle"] >= 1) & (df["mc"] == 290)].dropna(subset=["cp"]).copy()

    # Compute park_depth
    df["park_depth"] = df["depth"].apply(
        lambda x: 200 if x < 350 else (1000 if x > 750 else 500)
    )

    df = df.drop(columns=["mc"])

    return df


def derive_ost_flux(data, wmo_float):
    """
    Derive optical sediment trap flux from CP data.

    Parameters:
    -----------
    data : pd.DataFrame
        DataFrame containing CP data
    wmo_float : str
        WMO float identifier

    Returns:
    --------
    pd.DataFrame
        DataFrame with computed fluxes
    """
    # Filter and sort data chronologically
    tmp = data[data["wmo"] == wmo_float].copy()
    tmp = tmp.sort_values("juld").reset_index(drop=True)

    # Despike cp data with a 7-point moving window (k=3)
    tmp["cp"] = despike(tmp["cp"].values, k=3)

    # Smooth cp data with a 3-point moving median, n time(s)
    tmp["cp"] = slide(tmp["cp"].values, fun=np.nanmedian, k=3, n=1)

    # Compute slope between two adjacent points
    delta_x = tmp["juld"].diff().dt.total_seconds() / (24 * 3600)  # Convert to days
    delta_y = tmp["cp"].diff()
    tmp["slope"] = delta_y / delta_x

    # Compute Z score on the slopes
    mean_slope = tmp["slope"].mean()
    std_slope = tmp["slope"].std()
    tmp["zscore"] = (tmp["slope"] - mean_slope) / std_slope

    # Spot outliers using IQR method
    Q1 = tmp["zscore"].quantile(0.25)
    Q3 = tmp["zscore"].quantile(0.75)
    IQR = Q3 - Q1

    spikes_down = tmp["zscore"] < (Q1 - 1.5 * IQR)
    spikes_up = tmp["zscore"] > (Q3 + 1.5 * IQR)
    spikes = spikes_down | spikes_up

    tmp["spikes"] = spikes

    # Assign colour code to cp signal
    tmp["colour"] = "base signal"
    tmp.loc[tmp["spikes"], "colour"] = "jump"

    # Add group to compute the slope of each group of points
    tmp["group"] = None

    # Index of jumps
    jump_index = tmp[tmp["colour"] == "jump"].index.tolist()

    # Assign group identity
    for i in jump_index:
        mask = (tmp.index < i) & (tmp["group"].isna())
        tmp.loc[mask, "group"] = f"group_{i}"

    tmp.loc[tmp["group"].isna(), "group"] = "last_group"

    # Compute slope for each subgroup
    slope_list = []
    for group_name, group_data in tmp[tmp["colour"] == "base signal"].groupby("group"):
        group_data = group_data.dropna(subset=["slope"])
        if len(group_data) > 3:
            min_time = group_data["juld"].min()
            max_time = group_data["juld"].max()
            nb_points = len(group_data)
            first_cp = group_data.iloc[0]["cp"]
            last_cp = group_data.iloc[-1]["cp"]
            delta_x = (max_time - min_time).total_seconds() / (24 * 3600)
            delta_y = (last_cp - first_cp) * 0.25  # Convert cp to ATN
            slope = delta_y / delta_x

            if slope > 0:  # Remove negative slopes
                slope_list.append(
                    {
                        "group": group_name,
                        "min_time": min_time,
                        "max_time": max_time,
                        "nb_points": nb_points,
                        "first_cp": first_cp,
                        "last_cp": last_cp,
                        "delta_x": delta_x,
                        "delta_y": delta_y,
                        "slope": slope,
                    }
                )

    slope_df = pd.DataFrame(slope_list)

    if len(slope_df) > 0:
        # Compute weighted average slope
        mean_slope = (slope_df["nb_points"] * slope_df["slope"]).sum() / slope_df[
            "nb_points"
        ].sum()

        # Convert cp to POC using Estapa's relationship
        poc_flux = 633 * (mean_slope**0.77)
    else:
        poc_flux = 0

    # Build dataframe to plot each subgroup
    part1 = slope_df[["group", "min_time", "first_cp"]].rename(
        columns={"min_time": "time", "first_cp": "cp"}
    )
    part2 = slope_df[["group", "max_time", "last_cp"]].rename(
        columns={"max_time": "time", "last_cp": "cp"}
    )
    part_slope = pd.concat([part1, part2], ignore_index=True)

    # Spot negative jump
    negative_jump_mask = (tmp["colour"] == "jump") & (tmp["slope"] < 0)
    tmp.loc[negative_jump_mask, "colour"] = "negative jump"

    # Add large particles flux
    rows_to_keep = []
    for idx in jump_index:
        if idx > 0:
            rows_to_keep.extend([idx - 1, idx])

    tmp2 = tmp.loc[
        rows_to_keep, ["juld", "cp", "slope", "colour", "group"]
    ].sort_values("juld")

    # Remove negative jumps, if any
    check_colour = tmp2["colour"].unique()
    if len(check_colour) >= 2:  # At least one jump
        tmp2["diff_jump"] = tmp2["cp"].diff()
        tmp2 = tmp2.iloc[1::2]  # Keep even indexes
    else:
        tmp2 = None

    if tmp2 is None or len(tmp2) == 0:
        large_part_poc_flux = 0
        tmp3 = None
    else:
        tmp3 = tmp2[tmp2["diff_jump"] > 0]
        if len(tmp3) == 0:
            large_part_poc_flux = 0
        else:
            delta_y = tmp3["diff_jump"].sum() * 0.25  # Convert to ATN
            max_time = tmp["juld"].max()
            min_time = tmp["juld"].min()
            delta_x = (max_time - min_time).total_seconds() / (24 * 3600)
            slope_large_part = delta_y / delta_x
            large_part_poc_flux = 633 * (slope_large_part**0.77)

    # Compute total drifting time
    max_time = tmp["juld"].max()
    min_time = tmp["juld"].min()
    drifting_time = (max_time - min_time).total_seconds() / (24 * 3600)

    # Create result DataFrame
    result = pd.DataFrame(
        {
            "max_time": [max_time],
            "min_time": [min_time],
            "small_flux": [poc_flux],
            "large_flux": [large_part_poc_flux],
            "park_depth": [data["park_depth"].iloc[0]],
            "wmo": [data["wmo"].iloc[0]],
            "cycle": [data["cycle"].iloc[0]],
        }
    )

    return result


# Extract particle data for each float
dfs = []
for wmo in WMO:
    try:
        with fs.open(
            f"s3://argo-gdac-sandbox/pub/dac/coriolis/{wmo}/{wmo}_Rtraj.nc", "rb"
        ) as f:
            ds = xr.open_dataset(f)
            # dfs.append(df)
    except Exception as e:
        print(f"Error processing {wmo}: {e}", file=sys.stderr)
        continue


def extract_ost_data(wmo, ds):
    """
    Extract optical sediment trap data.

    Parameters:
    -----------
    wmo_float : int or str
        WMO float identifier
    path_to_data : str
        Path to data directory

    Returns:
    --------
    pd.DataFrame
        DataFrame with OST flux data
    """
    # Parking depths
    park_depths = [200, 500, 1000]

    # Extract cp data from the float
    data = extract_cp_data(wmo, ds)

    res = []
    max_cycle = data["cycle"].max()

    for park_depth in park_depths:
        for cycle in range(1, int(max_cycle) + 1):
            # print(park_depth, cycle)
            tmp = data[(data["park_depth"] == park_depth) & (data["cycle"] == cycle)]
            # print(tmp)

            if len(tmp) == 0:  # No data for this cycle or at this parking depth
                continue
            elif len(tmp) < 3:  # Not enough data
                continue
            else:
                try:
                    output = derive_ost_flux(tmp, wmo)
                    res.append(output)
                except:
                    continue

    if len(res) > 0:
        return pd.concat(res, ignore_index=True)
    else:
        return pd.DataFrame()


# Process all floats
results = []
for wmo in WMO:
    result = extract_ost_data(wmo, ds)
    if len(result) > 0:
        results.append(result)

tmp = pd.concat(results, ignore_index=True)


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
