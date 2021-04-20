SeaDASbatch
=====

_Batch processing of Ocean Color images using SeaDAS using default algorithm from SeaDAS
to reproduce Ocean Color Rrs processing and compute additional products._

Only OLCI and MSI for now.
Input files must be in the format downloaded with getOC.py
Need to be run through a SeaDAS Virtual Env and ocssw_runner:
   /home/username/ocssw/bin/ocssw_runner --ocsswroot /home/username/ocssw /home/username/.conda/envs/SeaDAS/bin/python3.9 SeaDASbatchESA_L2.py

With:
- path to image processor runner script: /home/username/ocssw/bin/ocssw_runner
- path to ocssw root directory: --ocsswroot /home/username/ocssw
- path to python in virtual environment: /home/username/.conda/envs/SeaDAS/bin/python3.9
- name of python script to run: SeaDASbatchESA_L2.py

### Argument description:
- **`--path`** **path to root directory**  

- **`--project`** **project name: working directory containing images (subdirectory of root directory)**  

- **`-i`** **instrument**  
     - **`MSI`**  
     - **`OLCI`**

- **`--suite`** **suite**  
     - **`OC`**  
     - **`SST`**  [not tested]  

- **`--prod`** **list of product to compute specific to each instrument**  

- **`-p`** **parallel computing**  
    Specify number of process:
    - 0 = disable parallel processing
    - -1 = any number of worker available
    - 12 = 12 workers

**`-f`** **force process**  
   - overwrite files with same name

**`-q`** **quiet**  
   - Quiet please !

### Usage examples:
    /home/username/ocssw/bin/ocssw_runner --ocsswroot /home/username/ocssw /home/username/.conda/envs/SeaDAS/bin/python3.9 SeaDASbatchESA_L2.py --path /home/username/SeaDASbatch --project test01 -i MSI -p 12
    /home/username/ocssw/bin/ocssw_runner --ocsswroot /home/username/ocssw /home/username/.conda/envs/SeaDAS/bin/python3.9 SeaDASbatchESA_L2.py --path /home/username/SeaDASbatch --project test01 -i OLCI -p 12
    /home/username/ocssw/bin/ocssw_runner --ocsswroot /home/username/ocssw /home/username/.conda/envs/SeaDAS/bin/python3.9 SeaDASbatchESA_L2.py --path /home/username/SeaDASbatch --project test01 -i MSI -p 0 -f
    /home/username/ocssw/bin/ocssw_runner --ocsswroot /home/username/ocssw /home/username/.conda/envs/SeaDAS/bin/python3.9 SeaDASbatchESA_L2.py --path /home/username/SeaDASbatch --project test01 -i MSI -p -1 -f