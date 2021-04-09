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

from SD_ESAtools import *
import os
# from multiprocessing import Pool
import sys
# import subprocess
from multiprocessing.pool import ThreadPool
import multiprocessing as mp
from itertools import repeat
# import _strptime # to solve multithreading bug

IM_PREFIX = {
    'OLCI': ['S3A_OL_1_', 'S3B_OL_1_'],
    'SLSTR': ['S3A_SL_1_', 'S3B_SL_1_'],
    'MSI': ['S2A_MSIL1C_', 'S2B_MSIL1C_']
    }

IM_SUFFIX = {
    'OLCI': ['SEN3.zip'],
    'SLSTR': ['SEN3.zip'],
    'MSI': ['.zip']
    }

# Process #
def L2process((ref, instrument, suite, product, force_process)):
    if instrument == 'OLCI' or instrument == 'SLSTR': ########## OLCI
      process_SENT3_L1_to_L2(PATH_TO_DATA, ref, instrument=instrument, suite=suite, l2_prod=product, get_anc=True, path_to_anc=PATH_TO_ANC, force=force_process)
    elif instrument == 'MSI': ########## MSI
      process_MSI_L1_to_L2(PATH_TO_DATA, ref, suite=suite, l2_prod=product, get_anc=True, path_to_anc=PATH_TO_ANC, force=force_process)
    print('### Done processing  ' + ref)

# List L2 files to process #
def list_file(instrument):
    sen = options.instrument.split('-')
    prefix = list()
    suffix = list()
    for x in sen:
      for y in IM_PREFIX[x]:
        prefix.append(y)
      for y in IM_SUFFIX[x]:
        suffix.append(y)
    references = [s.split('.')[0] for s in os.listdir(PATH_TO_DATA) if s.startswith(tuple(prefix)) and s.endswith(tuple(suffix))]
    return references


if __name__ == "__main__":
    from optparse import OptionParser

    parser = OptionParser(usage="Usage: SeaDASbatchL2.py [options] [instrument]", version="SeaDASbatchL2 " + __version__)
    parser.add_option("-i", "--instrument", action="store", dest="instrument",
                      help="specify instrument, available options are: VIIRS, MODISA, MODIST, OLCI, SLSTR, MSI")
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
    PATH_TO_DATA = os.path.join(options.PATH_TO_ROOT, 'data', options.project_name)

    # list images
    references = list_file(options.instrument)

    if options.parallel_process == 0: # Process images one by one #
      n = len(references)
      for singlref, i in zip(references, range(n)):
         print('Start process one by one')
         print('########################################')
         print('[' + str(i+1) + '/' + str(n) + ']  ' + singlref)
         print('########################################')
         L2process((singlref, options.instrument, options.suite, options.product, options.force_process))

    else: # Process to images in parallel
      # Start pool (with the number of thread available on node)
      pool = ThreadPool(processes=ntask)
      print('Start parallel process')
      pool.map(L2process, zip(references, repeat(options.instrument), repeat(options.suite), repeat(options.product), repeat(options.force_process)))
      pool.close()
      pool.join()