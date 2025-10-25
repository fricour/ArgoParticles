import pandas as pd

def remove_outliers(series):
    """Remove outliers based on IQR method"""
    q1 = series.quantile(0.25)
    q3 = series.quantile(0.75)
    iqr = q3 - q1
    lower_bound = q1 - 1.5 * iqr
    upper_bound = q3 + 1.5 * iqr
    return series.where((series >= lower_bound) & (series <= upper_bound))

# Define WMO floats
WMO = [1902578, 1902593, 1902601, 1902637, 1902685, 2903783, 2903787, 2903794, 
       3902471, 3902498, 4903634, 4903657, 4903658, 4903660, 4903739, 4903740, 
       5906970, 6904240, 6904241, 6990503, 6990514, 7901028]