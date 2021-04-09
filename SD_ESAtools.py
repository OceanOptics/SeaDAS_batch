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

# PATH TO OCSSW
PATH_OCSSW = '/home/gbourdin/ocssw'
OCSSW_RUNNER = PATH_OCSSW + '/scripts/ocssw_runner'


# Import get_ancillaries modules
import sys
sys.path.insert(0, os.path.join(PATH_OCSSW, 'scripts'))
import modules.anc_utils as ga
from modules.setupenv import env


def unzipS(ref, suffix):
    # Decompress zip
    if not os.path.isdir(ref + suffix):
        print('Decompress zip ' + ref + suffix + ' ...')
        os.mkdir(ref + suffix)
        zf = zipfile.ZipFile(ref + suffix + '.zip')
        zf.extractall(path=os.path.dirname(ref + suffix))
    else:
        print('Decompress zip: Skip')
    return None


def check_unzip(ref, suffix):
    # check unzip size
    if len(os.listdir(ref + suffix)) == 0:
        print("Empty directory, zipfile failed, restarting")
        shutil.rmtree(ref + suffix)
        unzipS(ref, suffix)
        if len(os.listdir(ref + suffix)) == 0:
            print("Empty directory, zipfile failed, restarting")
            shutil.rmtree(ref + suffix)
            unzipS(ref, suffix)
            if len(os.listdir(ref + suffix)) == 0:
                print("Empty directory, zipfile failed 3 times")
                return -1


def get_ancillaries(sensor, reference, path_to_data, path_to_anc, start_dt=None, stop_dt=None):
    # based on SeaDAS/Ocssw/getanc.py
    # Need to be run through a SeaDAS Virtual Env and ocssw_runner:
    # ./ocssw/scripts/ocssw_runner --ocsswroot /home/gbourdin/ocssw/ /home/gbourdin/.conda/envs/SeaDAS/bin/python process.py

    # add random sleep time to avoid overload OBPG server
    sleep(random.uniform(0, 10))

    if sensor == 'MODIS':
        ref = os.path.join(path_to_data, reference) + '.L1A_LAC'
    elif sensor == 'VIIRS':
        ref = os.path.join(path_to_data, reference) + '.L1A_SNPP.nc'
    elif sensor == 'OLCI' or sensor == 'SLSTR':
        ref = os.path.join(path_to_data, reference) + '.SEN3'
    elif sensor == 'MSI':
        ref = os.path.join(path_to_data, reference) + '.SAFE'

    # Start and Stop dt must be YYYYDDDHHMMSS strings
    if reference is not None and start_dt is None and stop_dt is None:
        g = ga.getanc(file=ref,
                      sensor=sensor,
                      ancdir=os.path.join(path_to_anc, reference),
                      ancdb=os.path.join(path_to_anc, 'ancillary_data_' + reference + '.db'),
                      opt_flag=5,
                      timeout=60,
                      verbose=True)
    elif start_dt is not None and stop_dt is not None:
        g = ga.getanc(file=ref,
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


    def process_SENT3_L1_to_L2(path_to_data, reference, instrument='OLCI', suite='OC', l2_prod=None, get_anc=True, path_to_anc=None, force=False):
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
    unzipS(ref, '.SEN3')
    # check unzip
    check_unzip(ref, '.SEN3')

    if get_anc and not os.path.isfile(ref + '.L2_SEN3.nc'):
        print('Get Ancillaries ...')
        os.chdir(path_to_anc)
        foo = ref.split('____')
        foo2 = foo[1].split('_')
        start_dt = datetime.strptime(foo2[0], '%Y%m%dT%H%M%S').strftime('%Y%j%H%M%S')
        stop_dt = datetime.strptime(foo2[1], '%Y%m%dT%H%M%S').strftime('%Y%j%H%M%S')
        # create ancillary directory specific to that image to avoid conflict between threads
        if not os.path.isdir(os.path.join(path_to_anc, reference)):
            os.mkdir(os.path.join(path_to_anc, reference))
        anc = get_ancillaries(instrument, reference, path_to_data, path_to_anc, start_dt=start_dt, stop_dt=stop_dt)

    # Process L2
    if not os.path.isfile(ref + '.L2_SEN3.nc'):
        print('Process L2 ...')
        os.chdir(ref + '.SEN3/')
        cmd = [OCSSW_RUNNER, '--ocsswroot', PATH_OCSSW, 'l2gen',
                'suite=' + suite,
                'ifile=' + ref + '.SEN3/Oa01_radiance.nc',
                'ofile=' + ref + '.L2_SEN3_temp.nc',
                'maskland=0',
                'maskhilt=0']
        if get_anc:
            for key in sorted(anc.iterkeys()):
                cmd.append('='.join([key, anc[key]]))
        if l2_prod is not None:
            cmd.append('l2prod=' + l2_prod)
        check_call(cmd)
        os.rename(ref + '.L2_SEN3_temp.nc', ref + '.L2_SEN3.nc')
    else:
        print('Process L2: Skip')


def process_MSI_L1_to_L2(path_to_data, reference, suite='OC', l2_prod=None, get_anc=True, path_to_anc=None, force=False):
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
    unzipS(ref, '.SAFE')
    # check unzip
    check_unzip(ref, '.SAFE')

    if get_anc and not os.path.isfile(ref + '.L2_SEN2.nc'):
        print('Get Ancillaries ...')
        os.chdir(path_to_anc)
        foo = ref.split('_MSIL1C_')
        foo2 = foo[1].split('_')
        start_dt = datetime.strptime(foo2[0], '%Y%m%dT%H%M%S')
        stop_dt = datetime.strptime(foo2[0], '%Y%m%dT%H%M%S') + timedelta(minutes=5)
        # create ancillary directory specific to that image to avoid conflict between threads
        if not os.path.isdir(os.path.join(path_to_anc, ref)):
            os.mkdir(os.path.join(path_to_anc, ref))
        anc = get_ancillaries('MSI', reference, path_to_data, path_to_anc, start_dt=start_dt.strftime('%Y%j%H%M%S'), stop_dt=stop_dt.strftime('%Y%j%H%M%S'))

    # Process L2
    if not os.path.isfile(ref + '.L2_SEN2.nc'):
        print('Process L2 ...')
        os.chdir(ref + '.SAFE/')
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
            for key in sorted(anc.iterkeys()):
                cmd.append('='.join([key, anc[key]]))
        if l2_prod is not None:
            cmd.append('l2prod=' + l2_prod)
        check_call(cmd)
        os.rename(ref + '.L2_SEN2_temp.nc', ref + '.L2_SEN2.nc')
    else:
        print('Process L2: Skip')