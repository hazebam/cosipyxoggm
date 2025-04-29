#!/bin/bash 

# The batch system should use the current directory as working directory.
#SBATCH --job-name=AWS2nc
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=20
#SBATCH --time=03:00:00

module load intel64 netcdf 
Module load 

python3 aws2cosipy.py -i ../../data/input/Zhadang/Zhadang_ERA5_200901_short.csv -o ../../data/input/Zhadang/Zhadang_ERA5_2009.nc -s ../../data/static/Zhadang_static.nc -b 20090101 -e 20091231 
