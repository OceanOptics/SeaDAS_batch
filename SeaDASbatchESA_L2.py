#!/usr/bin/env python
"""
Process OLCI L1 and MSI L1C image to L2 using default algorithm from SeaDAS
to reproduce Ocean Color Rrs processing and compute additional products
Input files must be in the format downloaded with getOC.py

Need to be run through a SeaDAS Virtual Env and ocssw_runner:
   /home/gbourdin/ocssw/scripts/ocssw_runner --ocsswroot /home/gbourdin/ocssw /home/gbourdin/.conda/envs/SeaDAS/bin/python2.7 SeaDASbatchL2.py

MIT License

Copyright (c) 2021 Guillaume Bourdin
"""

__version__ = "0.1.0"
verbose = False

import os
from SD_ESAtools import *
# from multiprocessing import Pool
import sys
import glob
# import subprocess
from multiprocessing.pool import ThreadPool
import multiprocessing as mp
from itertools import repeat
# import _strptime # to solve multithreading bug

IM_PREFIX = {
    'OLCI': 'S3*_OL_1_',
    'SLSTR': 'S3*_SL_1_',
    'MSI': 'S2*_MSIL1C_'
    }
    
IM_SUFFIX = {
    'OLCI': 'SEN3',
    'SLSTR': 'SEN3',
    'MSI': 'SAFE'
    }

IM_EXT = {
    'OLCI': '.zip',
    'SLSTR': '.zip',
    'MSI': '.zip'
    }

# List L2 files to process #
def list_file(instrument):
    references = [s.split('.')[0] for s in glob.glob(os.path.join(PATH_TO_DATA, '*' + IM_PREFIX[instrument] + '*' + IM_SUFFIX[instrument] + IM_EXT[instrument]))]
    return references

# get ancillary data for each image and build list to inpu in L2process
def getancil(references, instrument):
    print('### Ancillary data recovery ###')
    if instrument == 'VIIRSN' or instrument == 'VIIRSJ1':
      instru = 'VIIRS'
    elif instrument == 'MODISA' or instrument == 'MODIST':
      instru = 'MODIS'
    else:
      instru = instrument
    os.chdir(PATH_TO_ANC)
    anc_list = list()
    for singlref in references:
      # if not os.path.isfile(os.path.join(PATH_TO_DATA, singlref + L2_SUFFIX[instrument])) and not glob.glob(os.path.join(PATH_TO_ANC, singlref + IM_SUFFIX[instrument] + '.anc')):
      if not glob.glob(os.path.join(PATH_TO_ANC, os.path.split(singlref)[1] + IM_SUFFIX[instrument] + '.anc')) or options.force_process:
        # create ancillary directory specific to that image to avoid conflict between threads
        if not os.path.isdir(os.path.join(PATH_TO_ANC, os.path.split(singlref)[1] + IM_SUFFIX[instrument])):
          os.mkdir(os.path.join(PATH_TO_ANC, os.path.split(singlref)[1] + IM_SUFFIX[instrument]))
        if instru == 'MSI':
          foo = singlref.split('_MSIL1C_')
          foo2 = foo[1].split('_')
          start_dt = datetime.strptime(foo2[0], '%Y%m%dT%H%M%S')
          stop_dt = datetime.strptime(foo2[0], '%Y%m%dT%H%M%S') + timedelta(minutes=5)
          anc = get_ancillaries('MSI', os.path.split(singlref)[1] + IM_SUFFIX[instrument], PATH_TO_DATA, PATH_TO_ANC, start_dt=start_dt.strftime('%Y%j%H%M%S'), stop_dt=stop_dt.strftime('%Y%j%H%M%S'))
        elif instru == 'OLCI' or instru == 'SLSTR' :
          foo = singlref.split('____')
          foo2 = foo[1].split('_')
          start_dt = datetime.strptime(foo2[0], '%Y%m%dT%H%M%S').strftime('%Y%j%H%M%S')
          stop_dt = datetime.strptime(foo2[1], '%Y%m%dT%H%M%S').strftime('%Y%j%H%M%S')
          anc = get_ancillaries(instru, os.path.split(singlref)[1] + IM_SUFFIX[instrument], PATH_TO_DATA, PATH_TO_ANC, start_dt=start_dt, stop_dt=stop_dt)
        else:
          return -1
        anc_key = ''
        # for key in sorted(anc.iterkeys()): # for seadas version < 8.00
        for key, value in anc.items():
          anc_key = '<>'.join([anc_key, '='.join([key, value])])
        anc_list.append(anc_key)
      else:
        print('Get ancillary ' + os.path.split(singlref)[1] + IM_SUFFIX[instrument] + ' skip')
        anc_list = open(os.path.join(PATH_TO_ANC, os.path.split(singlref)[1] + IM_SUFFIX[instrument] + IM_SUFFIX[instrument] + '*.anc'), "w").read()
        anc_list = anc_list.replace('\n', '<>')
    return anc_list

# # Process #
# def L2processP2((ref, anc_list, instrument, suite, product, force_process)): ### Python2
#     if instrument == 'OLCI' or instrument == 'SLSTR': ########## OLCI
#       process_SENT3_L1_to_L2(PATH_TO_DATA, ref, anc_list, instrument=instrument, suite=suite, l2_prod=product, get_anc=True, path_to_anc=PATH_TO_ANC, force=force_process)
#     elif instrument == 'MSI': ########## MSI
#       process_MSI_L1_to_L2(PATH_TO_DATA, ref, anc_list, suite=suite, l2_prod=product, get_anc=True, path_to_anc=PATH_TO_ANC, force=force_process)
#     print('### Done processing  ' + ref)

def L2processP3(ref, anc_list, instrument, suite, product, force_process): ### Python3
    if instrument == 'OLCI' or instrument == 'SLSTR': ########## OLCI
      process_SENT3_L1_to_L2(PATH_TO_DATA, ref, anc_list, instrument=instrument, suite=suite, l2_prod=product, get_anc=True, path_to_anc=PATH_TO_ANC, force=force_process)
    elif instrument == 'MSI': ########## MSI
      process_MSI_L1_to_L2(PATH_TO_DATA, ref, anc_list, suite=suite, l2_prod=product, get_anc=True, path_to_anc=PATH_TO_ANC, force=force_process)
    print('### Done processing  ' + ref)


if __name__ == "__main__":
    from optparse import OptionParser

    parser = OptionParser(usage="Usage: SeaDASbatchESA_L2.py [options] [instrument]", version="SeaDASbatchESA_L2 " + __version__)
    parser.add_option("-i", "--instrument", action="store", dest="instrument",
                      help="specify instrument, available options are: OLCI, SLSTR, MSI")
    parser.add_option("--path", "--path-to-root", action="store", dest="PATH_TO_ROOT",
                      help="specify path to root directory")
    parser.add_option("--project", "--project-name", action="store", dest="project_name",
                      help="specify project name. Data directory under '/root_directory/data/' must be named after project name")
    parser.add_option("--suite", "--suite", action="store", dest="suite", default='OC',
                      help="specify suite 'OC' or 'SST'. ")
    # Level 2 specific option
    parser.add_option("--prod", "--product", action="store", dest="product", default=None,
                      help="specify product identifier, check available product for specific sensor, default = 'all default products'")
    # processing options
    parser.add_option("-p", "--parallel-computing", action="store", dest="parallel_process", type='int', default=0,
                      help="specify number of process: 0 = disable parallel processing, -1 = any number of worker available, # = # workers")
    parser.add_option("-f", "--force-process", action="store_true", dest="force_process", default=False,
                      help="specify force process option (boolean)")
    # Other options
    parser.add_option("-q", "--quiet", action="store_false", dest="verbose", default=True)
    (options, args) = parser.parse_args()

    verbose = options.verbose
    if options.instrument is None:
        print(parser.usage)
        print('SeaDASbatchL2.py: error: argument -i, --instrument is required')
        sys.exit(-1)
    if options.PATH_TO_ROOT is None:
        print(parser.usage)
        print('SeaDASbatchL2.py: error: argument --path is required')
        sys.exit(-1)
    if options.project_name is None:
        print(parser.usage)
        print('SeaDASbatchL2.py: error: argument --project_name is required')
        sys.exit(-1)
    if len(args) > 2:
        print(parser.usage)
        print('SeaDASbatchL2.py: error: too many arguments')
        sys.exit(-1)
    if options.product is None:
        print(parser.usage)
        print('SeaDASbatchL2.py: warning: option --prod, --product option not specified, set to default "all default products"')
    if options.suite is None:
        print(parser.usage)
        print('SeaDASbatchL2.py: warning: option --suite, --suite option not specified, set to default "OC"')
    if options.parallel_process is None:
        print(parser.usage)
        print('SeaDASbatchL2.py: warning: option -p, --parallel-computing option not specified, set to default=0')
    elif options.parallel_process == -1:
        ntask = mp.cpu_count()
    else:
        ntask = options.parallel_process
    if options.force_process is None:
        print(parser.usage)
        print('SeaDASbatchL2.py: warning: option -f, --force-process option not specified, set to default=False')

    PATH_TO_ANC = os.path.join(options.PATH_TO_ROOT, 'anc')
    if not os.path.isdir(PATH_TO_ANC):
      os.mkdir(PATH_TO_ANC)
    PATH_TO_DATA = os.path.join(options.PATH_TO_ROOT, options.project_name)

    # list images
    references = list_file(options.instrument)
    # get ancillary data one by one to avoid multithread path conflict
    anc_list = getancil(references, options.instrument)

    if options.parallel_process == 0: # Process images one by one #
      n = len(references)
      for singlref, i in zip(references, range(n)):
        print('Start process one by one')
        print('########################################')
        print('[' + str(i+1) + '/' + str(n) + ']  ' + singlref)
        print('########################################')
        # L2processP2((singlref, anc_list[i], options.instrument, options.suite, options.product, options.force_process)) ####### Python2
        L2processP3(singlref, anc_list[i], options.instrument, options.suite, options.product, options.force_process) ######### Python3

    else: # Process to images in parallel
      # Start pool (with the number of thread available on node)
      print('Start parallel process')
      pool = ThreadPool(processes=ntask)
      ######### Python2 #########
      # pool.map(L2processP2, zip(references, anc_list, repeat(options.instrument), repeat(options.suite), repeat(options.product), repeat(options.force_process)))
      # pool.close()
      # pool.join()

      ######### Python3 #########
      arg_list = list()
      # for ref in references:
      for ref, i in zip(references, range(len(references))):
        arg_list.append([ref, anc_list[i], options.instrument, options.suite, options.product, options.force_process])
      pool = ThreadPool(processes=ntask)
      print('Start parallel process')
      pool.starmap(L2processP3, arg_list)



