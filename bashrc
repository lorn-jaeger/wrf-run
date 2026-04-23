set -o vi

module load conda
module load neovim
module load nco
module load ncl
module load gdal
module load cdo
module load wgrib2

alias qburn='qhist -u ljaeger -d 50 | awk '"'"'NR>2 { sum += $NF } END { print sum * 128 * 1.5 - 300000}'"'"''
alias qrn='qstat -u ljaeger | awk '"'"'NR>2 && $(NF-1)=="R" { n++ } END { print n * 128 * 1.5 }'"'"''

conda activate workflow

export NVM_DIR="$HOME/.nvm"
[ -s "$NVM_DIR/nvm.sh" ] && \. "$NVM_DIR/nvm.sh"  # This loads nvm
[ -s "$NVM_DIR/bash_completion" ] && \. "$NVM_DIR/bash_completion"  # This loads nvm bash_completion
