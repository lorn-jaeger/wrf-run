#PBS -q develop
#PBS -l select=1:ncpus=4:mpiprocs=4
#PBS -l walltime=00:05:00

module load openmpi
module list
/glade/u/home/ljaeger/wrf-run/bin/WPS-4.6-dmpar/geogrid.exe

