import os
import sys
import tempfile
import pandas as pd
import numpy as np
import xarray as xr
import s3fs

# --- S3 caching layer ---

def get_cache_dir():
    """Return the local cache directory for S3 files, creating it if needed."""
    cache_dir = os.path.join(os.path.dirname(__file__), ".cache", "s3")
    os.makedirs(cache_dir, exist_ok=True)
    return cache_dir

def download_s3_cached(s3_path):
    """Download an S3 file to local cache if not already present. Returns local path."""
    # Build a safe local filename from the S3 key
    safe_name = s3_path.replace("s3://", "").replace("/", "__")
    local_path = os.path.join(get_cache_dir(), safe_name)

    if not os.path.exists(local_path):
        print(f"Downloading {s3_path} ...", file=sys.stderr)
        fs = s3fs.S3FileSystem(anon=True)
        # Atomic write: download to temp file, then rename
        fd, tmp_path = tempfile.mkstemp(dir=get_cache_dir())
        try:
            os.close(fd)
            fs.get(s3_path, tmp_path)
            os.rename(tmp_path, local_path)
        except Exception:
            # Clean up temp file on failure
            if os.path.exists(tmp_path):
                os.remove(tmp_path)
            raise
    else:
        print(f"Using cached {s3_path}", file=sys.stderr)

    return local_path

def open_nc_cached(s3_path):
    """Download a NetCDF file from S3 (cached) and return an xarray Dataset."""
    local_path = download_s3_cached(s3_path)
    return xr.open_dataset(local_path, engine="scipy")

# --- Dynamic WMO list ---

def get_wmo_list():
    """Fetch the list of WMOs with LPM data from the S3 aux-profile index."""
    local_path = download_s3_cached(
        "s3://argo-gdac-sandbox/pub/idx/argo_aux-profile_index.txt"
    )
    df = pd.read_csv(local_path, comment="#", sep=",")
    mask = (
        df["parameters"].str.contains("CONCENTRATION_LPM", na=False)
    )
    wmos = (
        df[mask]["file"]
        .str.extract(r"(\d{7})")[0]
        .dropna()
        .astype(int)
        .unique()
    )
    return sorted(wmos)

# --- Utility functions ---

def remove_outliers(series):
    """Remove outliers based on IQR method"""
    q1 = series.quantile(0.25)
    q3 = series.quantile(0.75)
    iqr = q3 - q1
    lower_bound = q1 - 1.5 * iqr
    upper_bound = q3 + 1.5 * iqr
    return series.where((series >= lower_bound) & (series <= upper_bound))

def extract_LPM(ds):
    """Extract particle data at parking depth from NetCDF file"""

    # Extract variables
    pres = ds["PRES"].values
    mc = ds["MEASUREMENT_CODE"].values
    juld = ds["JULD"].values
    cycle = ds["CYCLE_NUMBER"].values
    wmo = ds["PLATFORM_NUMBER"].values.astype(str).item().strip()
    #part_spectra = ds["NB_SIZE_SPECTRA_PARTICLES"].values
    #image_number = ds["NB_IMAGE_PARTICLES"].values
    lpm_concentration = ds["CONCENTRATION_LPM"].values

    # Particle class sizes
    lpm_classes = [
        "NP_Size_50.8",
        "NP_Size_64",
        "NP_Size_80.6",
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
        "NP_Size_2580",
    ]

    # Create DataFrame from transposed particle spectra
    df = pd.DataFrame(lpm_concentration, columns=lpm_classes)

    # Divide by image volume (0.7L * number of images)
    #for col in lpm_classes:
    #    df[col] = df[col] / (0.7 * image_number)

    # Add metadata
    df["depth"] = pres
    df["mc"] = mc
    df["cycle"] = cycle
    df["juld"] = juld
    df["wmo"] = wmo

    # Filter and clean
    df = (
        df.dropna(subset=["NP_Size_50.8"])
        .query("mc == 290")  # only keep data when the float is parked
        .drop(columns=["mc"])  # remove measurement code (not needed anymore)
        .assign(
            park_depth=lambda x: np.where(
                x["depth"] < 350, 200, np.where(x["depth"] > 750, 1000, 500)
            )
        )
        .query('juld > "2021-01-01" and juld < "2027-01-01"') # because some crazy dates were found at some time ...
        .astype({"cycle": int})
        .reset_index()
    )

    # Reorder columns
    cols = ["depth", "park_depth", "cycle", "juld", "wmo"] + lpm_classes
    df = df[cols]

    ds.close()

    return df

# WMO list: override with WMO_TEST env var for quick single-float testing
# Usage: WMO_TEST=1902578 npm run build
_wmo_test = os.environ.get("WMO_TEST")
if _wmo_test:
    WMO = [int(w) for w in _wmo_test.split(",")]
    print(f"TEST MODE: using {len(WMO)} WMO(s): {WMO}", file=sys.stderr)
else:
    WMO = get_wmo_list()
    print(f"Found {len(WMO)} WMO floats with LPM data", file=sys.stderr)
