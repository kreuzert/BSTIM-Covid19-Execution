#!/bin/bash

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
module --force purge
module use $OTHERSTAGES
module load Stages/Devel-2019a GCCcore/.8.3.0 JupyterCollection/2019a.2.2

MAIL="" # These adresses are notified if full_routine.py throws an error. Seperate multiple adresses with whitespace
BASEDIR=${DIR}
GITDIR="BSTIM-Covid19"
DOWNLOADURL="https://www.arcgis.com/sharing/rest/content/items/f10774f1c63e40168479a1feb6c7ca74/data"
JOBDIR="jobs"
SLURMACCOUNT="covid19dynstat"
SLURMMAIL="" # These adresses are notified if a slurm job failed. Comma seperated
EXPORTDIR="" # export results to this base directory
EXPORTOFFSET="25"

# If there's an argument we want to run a Job for old days. If there's no argument we will use yesterday in the python script full_routine.
if [[ -n ${1} ]]; then
    echo "$(date) start full_routine with ${1//_/-}"
    /p/project/covid19dynstat/.local/share/venvs/covid19dynstat_jusuf/bin/python3 ${BASEDIR}/python_code/full_routine.py -m ${MAIL} --basedir ${BASEDIR} --gitdir ${GITDIR} --downloadurl ${DOWNLOADURL} --jobdir ${JOBDIR} --slurmaccount ${SLURMACCOUNT} --slurmmail ${SLURMMAIL} -d ${1//_/-}
else
    echo "$(date) start full_routine without date argument"
    /p/project/covid19dynstat/.local/share/venvs/covid19dynstat_jusuf/bin/python3 ${BASEDIR}/python_code/full_routine.py -m ${MAIL} --basedir ${BASEDIR} --gitdir ${GITDIR} --downloadurl ${DOWNLOADURL} --jobdir ${JOBDIR} --slurmaccount ${SLURMACCOUNT} --slurmmail ${SLURMMAIL} 
fi


EXIT_CODE=$(echo $?)
echo "$(date) full_routine.py finished with exit code: ${EXIT_CODE}"
if [[ ${EXIT_CODE} -eq 0 ]]; then
  exit 0
else
  exit 255
fi
