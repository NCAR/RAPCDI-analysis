import cftime

def linear_drift_remove(da, ver_time, y1, y2):
    '''function takes in a raw DP DataArray and returns anomalies with lead-time dependent removed'''
    d1 = cftime.DatetimeNoLeap(y1,1,1,0,0,0)
    d2 = cftime.DatetimeNoLeap(y2,12,31,23,59,59)

    fordrift = da.where((ver_time.mean('d2')>d1) & (ver_time.mean('d2')<d2))
    climodrift = fordrift.mean('M').mean('Y')

    da_anom = da - climodrift

    return da_anom, climodrift
