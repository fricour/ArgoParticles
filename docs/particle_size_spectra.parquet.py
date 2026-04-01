import sys
import pandas as pd
import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
from utils import extract_LPM, get_launch_date, open_nc_cached, remove_outliers, WMO


def compute_slope(i, data_spectra, mid_DSE, size_bin):
    """
    Compute spectral slope for a single spectrum using linear regression.

    Parameters:
    -----------
    i : int
        Index of the spectrum to process
    data_spectra : np.ndarray
        Array of particle spectra (rows are observations, columns are size classes)
    mid_DSE : np.ndarray
        Center of size bins
    size_bin : np.ndarray
        Length of size bins

    Returns:
    --------
    float
        Spectral slope (coefficient from linear regression)
    """
    spectrum = data_spectra[i, :]
    spectrum_norm = spectrum / size_bin

    # Prepare data for linear regression
    Y = np.log(spectrum_norm)
    X = np.log(mid_DSE)

    # Check for finite values
    h = np.isfinite(Y)
    Y = Y[h]
    X = X[h]

    # Perform linear regression (returns [slope, intercept])
    coefficients = np.polyfit(X, Y, deg=1)
    slope = coefficients[0]

    return slope


def compute_spectral_slope(wmo_float, ds, launch_date=None):
    """
    Compute spectral slope from UVP data at parking depth.

    Parameters:
    -----------
    wmo_float : str
        WMO float identifier
    ds : xr.dataset
    launch_date : pd.Timestamp, optional
        Float deployment date for filtering bogus dates

    Returns:
    --------
    pd.DataFrame
        DataFrame with computed spectral slopes
    """
    # Extract UVP data at parking
    data = extract_LPM(ds, launch_date=launch_date)

    # Particle size classes
    lpm_classes = [
        "NP_Size_102",
        "NP_Size_128",
        "NP_Size_161",
        "NP_Size_203",
        "NP_Size_256",
        "NP_Size_323",
        "NP_Size_406",
        "NP_Size_512",
        "NP_Size_645",
        "NP_Size_813",
        "NP_Size_1020",
        "NP_Size_1290",
        "NP_Size_1630",
        "NP_Size_2050",
    ]

    # "Center" of the size bin (pseudo center with a geometric progression of 2/3)
    mid_DSE = np.array(
        [
            0.1147968,
            0.1446349,
            0.1822286,
            0.2295937,
            0.2892699,
            0.3644572,
            0.4591873,
            0.5785398,
            0.7289145,
            0.9183747,
            1.1570796,
            1.4578289,
            1.83674934,
            2.31415916,
        ]
    )

    # Length of the size bin
    size_bin = np.array(
        [
            0.02640633,
            0.03326989,
            0.04191744,
            0.05281267,
            0.06653979,
            0.08383488,
            0.10562533,
            0.13307958,
            0.16766976,
            0.21125066,
            0.26615915,
            0.33533952,
            0.422501323,
            0.532318310,
        ]
    )

    # Keep useful columns
    data["wmo"] = wmo_float
    cols_to_keep = ["wmo", "juld", "cycle", "depth"] + lpm_classes
    data = data[cols_to_keep].copy()
    data = data.dropna()

    # Remove data when the smallest size class is 0 or non-finite
    # (could indicate an instrument failure)
    data = data[(data["NP_Size_102"] > 0) & (np.isfinite(data["NP_Size_102"]))]

    # Compute slope
    particle_spectra = data[lpm_classes].values
    slopes = np.array(
        [
            compute_slope(i, particle_spectra, mid_DSE, size_bin)
            for i in range(len(particle_spectra))
        ]
    )
    data["spectral_slope"] = slopes

    # Clean data and compute daily mean slope
    data = data.drop(columns=lpm_classes)

    # Compute park_depth
    data["park_depth"] = data["depth"].apply(
        lambda x: 200 if x < 350 else (1000 if x > 750 else 500)
    )

    # Round juld to daily resolution for aggregation
    data["juld"] = data["juld"].dt.floor("D")

    # Group by and summarize
    result = data.groupby(
        ["wmo", "cycle", "park_depth", "juld"], as_index=False
    ).agg(mean_slope=("spectral_slope", lambda x: x.mean(skipna=True)))

    return result


# Extract particle data and compute for each float
dfs = []

for wmo in WMO:
    try:
        ds = open_nc_cached(f"s3://argo-gdac-sandbox/pub/aux/coriolis/{wmo}/{wmo}_Rtraj_aux.nc")
        launch_date = get_launch_date(wmo)
        df = compute_spectral_slope(wmo, ds, launch_date=launch_date)
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

# Ensure wmo is string
tmp["wmo"] = tmp["wmo"].astype(str)

# Remove outliers on mean_slope per wmo and park_depth
tmp["mean_slope"] = (
    tmp.groupby(["wmo", "park_depth"])["mean_slope"]
    .transform(remove_outliers)
)
tmp = tmp.dropna(subset=["mean_slope"])

# Based on https://observablehq.observablehq.cloud/framework-example-loader-python-to-parquet/
# Write DataFrame to a temporary file-like object
buf = pa.BufferOutputStream()
table = pa.Table.from_pandas(tmp)
pq.write_table(table, buf, compression="snappy")

# Get the buffer as a bytes object
buf_bytes = buf.getvalue().to_pybytes()

# Write the bytes to standard output
sys.stdout.buffer.write(buf_bytes)
