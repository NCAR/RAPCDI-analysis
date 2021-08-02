import xarray as xr 
import numpy as np  
import cftime
import glob


def file_dict(filetempl, mem, stmon):
    ''' returns a dictionary of filepaths keyed by initialization year, 
    for a given experiment, field, ensemble member, and initialization month '''
    memstr = '{0:03d}'.format(mem)
    monstr = '{0:02d}'.format(stmon)
    filepaths = {}
    
    filetemp = filetempl.replace('MM',monstr).replace('EEE',memstr)

    #find all the relevant files
    files = glob.glob(filetemp)
        
    for file in files:
        #isolate initialization year from the file name
        ystr = file.split('.pop.h.')[0] #currently hardcoded for POP monthly
        y0 = int(ystr[-11:-7])
        filepaths[y0]=file
        
    return filepaths





def nested_file_list_by_year(filetemplate, ens, field, firstyear, lastyear, stmon):
    ''' retrieve a nested list of files for these start years and ensemble members'''
    ens = np.array(ens)+1
    yrs = np.arange(firstyear,lastyear+1)
    files = []    # a list of lists, dim0=start_year, dim1=ens
    ix = np.zeros(yrs.shape)+1
    
    for yy,i in zip(yrs,range(len(yrs))):
        ffs = []  # a list of files for this yy
        file0 = ''
        first = True
        for ee in ens:
            filepaths = file_dict(filetemplate, ee, stmon)
            #append file if it is new
            if yy in filepaths.keys():
                file = filepaths[yy]
                if file != file0:
                    ffs.append(file)
                    file0 = file
        
        #append this ensemble member to files
        if ffs:  #only append if you found files
            files.append(ffs)
        else:
            ix[i] = 0
    return files,yrs[ix==1]


def open_members(in_obj):
    ffs = in_obj[0]  #unwrap the list
    field = in_obj[1]
    ens = in_obj[2]
    lm = in_obj[3]
    chunks = in_obj[4]
    preprocess = in_obj[5]
 
    d0 = xr.open_mfdataset(ffs,combine='nested',parallel=True,concat_dim='M',data_vars=[field],\
                           chunks=chunks,compat='override', coords='minimal', preprocess=preprocess)
    
    #ADD CODE HERE THAT DETECTS L AND M FROM d0
    
    # quick fix to adjust time vector for monthly data  
    nmonths = len(d0.time)
    yr0 = d0['time.year'][0].values
    d0['time'] =xr.cftime_range(str(yr0),periods=nmonths,freq='MS')

    d0 = d0.assign_coords(M=("M",ens))
    d0 = d0.assign_coords(L=("time",lm))
    d0 = d0.swap_dims({'time': 'L'})
    d0 = d0.reset_coords(["time"])
    
    return d0



def get_monthly_data(filetemplate, ens, leads, field, firstyear, lastyear, stmon, preprocess, chunks={}, client=[]):
    ''' returns dask array containing the requested hindcast ensemble '''

    ds = xr.Dataset()    #instantiate Dataset #is this necessary?
    lm = np.array(leads)+1
    files,yrs = nested_file_list_by_year(filetemplate, ens, field, firstyear, lastyear, stmon)
    ens = np.array(ens)+1
    
    # all members should have the same number of files, otherwise abort
    nfs = np.array([len(ffs) for ffs in files])
    if np.sum(nfs==nfs[0])==len(nfs):
        complete_set=True   # same number of files
    else:
        raise ValueError('ERROR: Incomplete set of files')
        
    if complete_set: #read all data using map/gather
        dsets = []
        in_obj = [[ffs, field, ens, lm, chunks, preprocess] for ffs in files]
        if not client:
            dsets = [ open_members(one_hindcast) for one_hindcast in in_obj ]
        else:  
            dsets = client.map(open_members, in_obj)
            dsets = client.gather(dsets)
        tmp = xr.concat(dsets,dim='Y',data_vars=[field,'time','time_bound'], coords='minimal', compat='override')
        tmp = tmp.assign_coords(Y=("Y",yrs))

    print(tmp)
    ds[field] = tmp[field]
    ds['time'] = tmp['time']
    ds['time_bound'] = tmp['time_bound']
    ds['TAREA'] = tmp['TAREA']
    ds['UAREA'] = tmp['UAREA']
    ds['dz'] = tmp['dz']
    ds['HT'] = tmp['HT']

    return ds






