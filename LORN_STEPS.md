> [!NOTE]
> Documentation of the steps I followed to set up wps_wrf_workflow both for me and for anyone helping me debug this.


### Obtaining Code

Clone an up to date fork of wps_wrf_workflow into my home directory.

`git clone https://github.com/lorn-jaeger/wrf-run.git`

### Jeremy Files

I am using the config files and compiled WRF and WPS version available in Jeremy's For_Um directory. 

I put the the templates and configs in the template and config directories and am keeping WRF and WPS in a /bin directory I created.

I changed each of the marked paths in the configs and templates to the actual locations. You can see the diff for more information.

### Jared Changes

Previous runs crashed MPI, Jared told us that we were using too many CPU cores for this domain size and that we should reduce it to 128.
