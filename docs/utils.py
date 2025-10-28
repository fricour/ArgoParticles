import pandas as pd
import numpy as np

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
    part_spectra = ds["NB_SIZE_SPECTRA_PARTICLES"].values
    image_number = ds["NB_IMAGE_PARTICLES"].values

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
    df = pd.DataFrame(part_spectra, columns=lpm_classes)

    # Divide by image volume (0.7L * number of images)
    for col in lpm_classes:
        df[col] = df[col] / (0.7 * image_number)

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

# Define WMO floats
WMO = [1902578, 1902593, 1902601, 1902637, 1902685, 2903783, 2903787, 2903794, 
        3902471, 3902498, 4903634, 4903657, 4903658, 4903660, 4903739, 4903740, 
        5906970, 6904240, 6904241, 6990503, 6990514, 7901028]

# Used for tests
#WMO = [1902578, 6990503]
#WMO=[1902578]
