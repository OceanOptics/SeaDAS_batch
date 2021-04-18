#!/usr/bin/env python
"""
Tools to batch process OLCI and MSI with SeaDAS default atmospheric correction.
Input files must be in the format downloaded with getOC.py

MIT License

Copyright (c) 2021 Guillaume Bourdin
"""

from datetime import datetime, timedelta
from time import sleep
import random
from subprocess import check_call
import os
import shutil
import zipfile
import sys

# Setup path to OCSSW / OCSSW_RUNNER / modules directory in OCSSW
PATH_OCSSW = '/home/bjiang/ocssw'
OCSSW_RUNNER = PATH_OCSSW + '/bin/ocssw_runner' ## Seadas8.00 architecture
sys.path.insert(0, os.path.join(PATH_OCSSW, 'bin')) ## Seadas8.00 architecture
import seadasutils.anc_utils as ga ## Seadas8.00 architecture
from seadasutils.setupenv import env## Seadas8.00 architecture

# OCSSW_RUNNER = PATH_OCSSW + '/scripts/ocssw_runner' ## Seadas version < 8.00 architecture
# sys.path.insert(0, os.path.join(PATH_OCSSW, 'scripts')) ## Seadas version < 8.00 architecture
# import modules.anc_utils as ga ## Seadas version < 8.00 architecture
# from modules.setupenv import env ## Seadas version < 8.00 architecture


def unzipS(ref, suffix):
    # Decompress zip
    if not os.path.isdir(ref + suffix):
        print('Decompress zip ' + ref + suffix + ' ...')
        if os.path.isfile(ref + suffix + '.zip'):
            os.mkdir(ref + suffix)
            zf = zipfile.ZipFile(ref + suffix + '.zip')
            zf.extractall(path=os.path.dirname(ref + suffix))
        else:
            print(ref + suffix + '.zip not found')
            return -1
    else:
        print('Decompress zip: Skip')
    return None


def check_unzip(ref, suffix):
    # check unzip size
    MAX_RETRIES = 30
    j = 0
    while len(os.listdir(ref + suffix)) == 0 and j < MAX_RETRIES:
        print("Empty directory, unzip attempt [" + str(j+1) + "/" + str(MAX_RETRIES) + "] failed, restarting")
        shutil.rmtree(ref + suffix)
        unzipS(ref, suffix)
        j += 1
    if j+1 == MAX_RETRIES:
        print(str(MAX_RETRIES) + ' unzip attempts failed, process aborted.')
        return -1


def get_ancillaries(sensor, reference, path_to_data, path_to_anc, start_dt=None, stop_dt=None):
    # based on SeaDAS/Ocssw/getanc.py
    # Need to be run through a SeaDAS Virtual Env and ocssw_runner:
    # ./ocssw/scripts/ocssw_runner --ocsswroot /home/gbourdin/ocssw/ /home/gbourdin/.conda/envs/SeaDAS/bin/python process.py

    # add random sleep time to avoid overload OBPG server
    # sleep(random.uniform(0, 10))
    if sensor == 'OLCI' or sensor == 'SLSTR':
        ref = os.path.join(path_to_data, reference) + '.SEN3'
    elif sensor == 'MSI':
        ref = os.path.join(path_to_data, reference) + '.SAFE'

    # Start and Stop dt must be YYYYDDDHHMMSS strings
    if reference is not None and start_dt is None and stop_dt is None:
        # g = ga.getanc(file=ref, # for seadas version < 8.00
        g = ga.getanc(filename=ref, # for seadas version = 8.00
                      sensor=sensor,
                      ancdir=os.path.join(path_to_anc, reference),
                      ancdb=os.path.join(path_to_anc, 'ancillary_data_' + reference + '.db'),
                      opt_flag=5,
                      timeout=60,
                      verbose=True)
    elif start_dt is not None and stop_dt is not None:
        # g = ga.getanc(file=ref, # for seadas version < 8.00
        g = ga.getanc(filename=ref, # for seadas version = 8.00
                      start=start_dt,
                      stop=stop_dt,
                      sensor=sensor,
                      ancdir=os.path.join(path_to_anc, reference),
                      ancdb=os.path.join(path_to_anc, 'ancillary_data_' + reference + '.db'),
                      opt_flag=5,
                      timeout=60,
                      verbose=True)
    env(g) # This line check for environment variables
    g.chk()
    if g.finddb():
        g.setup()
    else:
        g.setup()
        g.findweb()
    g.locate()
    g.write_anc_par()
    g.cleanup()
    return g.files


def process_SENT3_L1_to_L2(path_to_data, reference, ancil_list, instrument='OLCI', suite='OC', l2_prod=None, get_anc=True, path_to_anc=None, force=False):
    # Process OLCI Image from compressed L1 to L2_SEN3 using default Nasa Ocean Color parameters
    #   the function will change the current working directory
    ref = os.path.join(path_to_data, reference)
    if force:
        if os.path.isdir(ref + '.SEN3'):
            shutil.rmtree(ref + '.SEN3')
        if os.path.isfile(ref + '.L2_SEN3.nc'):
            os.remove(ref + '.L2_SEN3.nc')
        if os.path.isfile(ref + '.L2_SEN3_temp.nc'):
            os.remove(ref + '.L2_SEN3_temp.nc')

    # Decompress zip
    if not os.path.isdir(ref + '.SEN3'):
        unzipS(ref, '.SEN3')
    # check unzip
    check_unzip(ref, '.SEN3')

    # Process L2
    if not os.path.isfile(ref + '.L2_SEN3.nc'):
        print('Process L2 ...')
        #os.chdir(ref + '.SEN3/')
        cmd = [OCSSW_RUNNER, '--ocsswroot', PATH_OCSSW, 'l2gen',
                'suite=' + suite,
                'ifile=' + ref + '.SEN3/Oa01_radiance.nc',
                'ofile=' + ref + '.L2_SEN3_temp.nc',
                'maskland=0',
                'maskhilt=0']
        if get_anc:
            foo = filter(lambda x: x != "", ancil_list.split('<>')) # remove empty string
            anc_key = list(foo)
            for key in anc_key:
                cmd.append(key) # append each ancillary file to cmd
        if l2_prod is not None:
            cmd.append('l2prod=' + l2_prod)
        check_call(cmd)
        os.rename(ref + '.L2_SEN3_temp.nc', ref + '.L2_SEN3.nc')
    else:
        print('Process L2: Skip')


def process_MSI_L1_to_L2(path_to_data, reference, ancil_list, suite='OC', l2_prod=None, get_anc=True, path_to_anc=None, force=False):
    # Process OLCI Image from compressed L1 to L2_SEN3 using default Nasa Ocean Color parameters
    #   the function will change the current working directory
    ref = os.path.join(path_to_data, reference)
    if force:
        if os.path.isdir(ref + '.SAFE'):
            shutil.rmtree(ref + '.SAFE')
        if os.path.isfile(ref + '.L2_SEN2.nc'):
            os.remove(ref + '.L2_SEN2.nc')
        if os.path.isfile(ref + '.L2_SEN2_temp.nc'):
            os.remove(ref + '.L2_SEN2_temp.nc')

    # Decompress zip
    if not os.path.isdir(ref + '.SAFE'):
        unzipS(ref, '.SAFE')
    # check unzip
    check_unzip(ref, '.SAFE')

    # Process L2
    if not os.path.isfile(ref + '.L2_SEN2.nc'):
        print('Process L2 ...')
        #os.chdir(ref + '.SAFE/')
        cmd = [OCSSW_RUNNER, '--ocsswroot', PATH_OCSSW, 'l2gen',
                'suite=OC',
                'ifile=' + ref + '.SAFE/manifest.safe',
                'ofile=' + ref + '.L2_SEN2_temp.nc',
                'brdf_opt=1',
                'aer_opt=-2',
                'cirrus_opt=false',
                'cloud_thresh=0.018',
                'cloud_wave=2130.0',
                'maskland=0',
                'maskhilt=0']
        if get_anc:
            foo = filter(lambda x: x != "", ancil_list.split('<>')) # remove empty string
            anc_key = list(foo)
            for key in anc_key:
                cmd.append(key) # append each ancillary file to cmd
        if l2_prod is not None:
            cmd.append('l2prod=' + l2_prod)
        check_call(cmd)
        os.rename(ref + '.L2_SEN2_temp.nc', ref + '.L2_SEN2.nc')
    else:
        print('Process L2: Skip')
