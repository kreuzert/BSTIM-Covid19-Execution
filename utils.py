import argparse
import datetime
import json
import logging.config
import numpy as np
import pandas as pd
import re
import requests
import os
import sys

from collections import OrderedDict
from logging.handlers import SMTPHandler
from subprocess import (Popen, PIPE)


# Override logging.config.file_config, so that the logfilename will be
# send to the parser, each time the logging.conf will be updated
def covid_file_config(fname, lname, defaults=None, disable_existing_loggers=True):
    if not defaults:
        defaults = {'logfilename': lname}
    import configparser

    if isinstance(fname, configparser.RawConfigParser):
        cp = fname
    else:
        cp = configparser.ConfigParser(defaults)
        if hasattr(fname, 'readline'):
            cp.read_file(fname)
        else:
            cp.read(fname)

    formatters = logging.config._create_formatters(cp)

    # critical section
    logging._acquireLock()
    try:
        logging._handlers.clear()
        del logging._handlerList[:]
        # Handlers add themselves to logging._handlers
        handlers = logging.config._install_handlers(cp, formatters)
        logging.config._install_loggers(cp, handlers, disable_existing_loggers)
    finally:
        logging._releaseLock()


def setup_logger(mail_receiver, logging_conf, logfile):
    logger = logging.getLogger('COVID')
    logging.addLevelName(9, "TRACE")

    def trace_func(self, message, *args, **kws):
        if self.isEnabledFor(9):
            # Yes, logger takes its '*args' as 'args'.
            self._log(9, message, args, **kws)

    logging.Logger.trace = trace_func
    mail_handler = SMTPHandler(mailhost='mail.fz-juelich.de',
                               fromaddr='covid19dynstat@fz-juelich.de',
                               toaddrs=mail_receiver,
                               subject='Covid19dynstat Error')
    mail_handler.setLevel(logging.ERROR)
    mail_handler.setFormatter(logging.Formatter(
        '[%(asctime)s] %(levelname)s in %(filename)s ( Line=%(lineno)d ): %(message)s'
    ))

    logging.config.fileConfig = covid_file_config
    logging.config.fileConfig(logging_conf, logfile)
    log = logging.getLogger('COVID')
    log.addHandler(mail_handler)
    return log

def str2bool(v):
    if isinstance(v, bool):
       return v
    if v.lower() in ('yes', 'true', 't', 'y', '1'):
        return True
    elif v.lower() in ('no', 'false', 'f', 'n', '0'):
        return False
    else:
        raise argparse.ArgumentTypeError('Boolean value expected.')

def parse_arguments(args):
    if args.date is not None:
        job_date = datetime.datetime.strptime(args.date[0], "%Y-%m-%d").date()
    else:
        job_date = datetime.date.today() - datetime.timedelta(days=1)

    if args.mailreceiver is not None:
        mail_receiver = args.mailreceiver
    else:
        mail_receiver = []

    if args.basedir is not None:
        base_dir = args.basedir[0]
    else:
        # Default: one above this script path
        base_dir = "{}".format(os.path.dirname(os.path.dirname(os.path.abspath(sys.argv[0]))))

    if args.downloadurl is not None:
        download_url = args.downloadurl[0]
    else:
        download_url = "https://www.arcgis.com/sharing/rest/content/items/f10774f1c63e40168479a1feb6c7ca74/data"

    if args.gitdir is not None:
        git_dir = os.path.join(base_dir, args.gitdir[0])
    else:
        git_dir = os.path.join(base_dir, "BSTIM-Covid19")

    if args.jobdir is not None:
        job_dir = os.path.join(base_dir, args.jobdir[0], job_date.strftime("%Y_%m_%d"))
    else:
        job_dir = os.path.join(base_dir, 'jobs', job_date.strftime("%Y_%m_%d"))

    if args.loggingconf is not None:
        logging_conf = args.loggingconf[0]
    else:
        logging_conf = os.path.join(os.path.dirname(os.path.abspath(sys.argv[0])), "logging.conf")

    if args.slurmaccount is not None:
        slurm_account = args.slurmaccount[0]
    else:
        slurm_account = "covid19dynstat"

    if args.slurmlogdir is not None:
        slurm_log_dir = os.path.join(job_dir, args.slurmlogdir[0])
    else:
        slurm_log_dir = os.path.join(job_dir, "slurm_logs")

    if args.slurmmail is not None:
        slurm_mail = args.slurmmail[0]
    else:
        slurm_mail = ""

    if args.exportdir is not None:
        export_dir = args.exportdir[0]
    else:
        export_dir = ""

    if args.exportoffset is not None:
        export_offset = args.exportoffset[0]
    else:
        export_offset = "25"

    if args.usetaskid is not None:
        use_task_id = args.usetaskid
    else:
        use_task_id = False

    if args.cores is not None:
        cores = args.cores[0]
    else:
        cores = 128

    if args.sampletasks is not None:
        sample_tasks = args.sampletasks[0]
    else:
        sample_tasks = 100

    if args.samplenodes is not None:
        sample_nodes = args.samplenodes[0]
    else:
        sample_nodes = 2

    if args.sampleoverloadfactor is not None:
        sample_overload_factor = args.sampleoverloadfactor[0]
    else:
        sample_overload_factor = 4

    return job_date, mail_receiver, base_dir, download_url, git_dir, job_dir, logging_conf, slurm_account, slurm_log_dir, slurm_mail, export_dir, export_offset, use_task_id, cores, sample_tasks, sample_nodes, sample_overload_factor


def download_csv(log, download_url, raw_csv_fpath):
    # download csv data
    log.info("Download COVID-19 data")
    log.trace("Download from: {}".format(download_url))
    log.trace("Download to: {}".format(raw_csv_fpath))
    raw_csv_red = requests.get(download_url,
                               allow_redirects=True)
    with open(raw_csv_fpath, 'wb') as f:
        f.write(raw_csv_red.content)


def preprocess_table(log, raw_csv_fpath, data_csv_fpath, git_dir_data, job_dir):
    log.trace("Converts Covid-19 Data.Tabels provided by the RKI to a simpler format to fit the model")
    log.trace("Input csv: {}".format(raw_csv_fpath))
    log.trace("Output csv: {}".format(data_csv_fpath))
    counties = OrderedDict()
    county_shapes = os.path.join(git_dir_data, "raw", "germany_county_shapes.json")
    log.trace("Load county shapes from: {}".format(county_shapes))
    with open(county_shapes, "r") as f:
        shape_data = json.load(f)

    for idx, val in enumerate(shape_data["features"]):
        id_current = val["properties"]["RKI_ID"]
        name_current = val["properties"]["RKI_NameDE"]
        counties[name_current] = id_current

    covid19_data = pd.read_csv(raw_csv_fpath, sep=',')
    # this complicated procedure removes timezone information.
    regex = re.compile(r"([0-9]+)/([0-9]+)/([0-9]+).*")
    start_year, start_month, start_day = regex.search(covid19_data['Meldedatum'].min()).groups()
    end_year, end_month, end_day = regex.search(covid19_data['Meldedatum'].max()).groups()
    start_date = pd.Timestamp(int(start_year), int(start_month), int(start_day))
    end_date = pd.Timestamp(int(end_year), int(end_month), int(end_day))
    log.trace("Start_date in Meldedaten: {}".format(start_date))
    log.trace("End_date in Meldedaten: {}".format(end_date))

    dates = [day for day in pd.date_range(start_date, end_date)]
    df = pd.DataFrame(index=dates)
    for (county_name, county_id) in counties.items():
        print('.', end='')
        series = np.zeros(len(df), dtype=np.int32)
        lk_data = covid19_data[covid19_data['IdLandkreis'] == int(county_id)]
        for (d_id, day) in enumerate(dates):
            day_string = "{:04d}/{:02d}/{:02d} 00:00:00".format(day.year, day.month, day.day)
            cases = np.sum(lk_data[lk_data['Meldedatum'] == day_string]['AnzahlFall'])
            if cases > 0:
                series[d_id] = cases
        df.insert(len(df.columns), counties[county_name], series)

    if '05334' not in df.columns:
        raise RuntimeError("Aachen was excluded from the final data")
    if sum(df.sum() == 0):
        raise RuntimeError("A county without any Covid cases in dataframe: CSV Parsing Error")
    if not len(df.columns) == 412:
        raise RuntimeError("Wrong number of counties in dataframe columns")

    df_total = df.sum(axis=1)
    df_total.to_csv(os.path.join(job_dir, "data", "diseases", "covid19_total.csv"))
    df.to_csv(data_csv_fpath, sep=",")


def create_sample_slurm(log, slurm_file, slurm_sh_file, sample_id, account, slurm_log_dir, slurm_mail, use_task_id=False, tasks_per_node=50, nodes=2):
    s = ""
    s += "#!/bin/bash -x\n"
    s += "#SBATCH --job-name={}_SM_COVID\n".format(sample_id)
    s += "#SBATCH --account={}\n".format(account)
    s += "#SBATCH --partition=batch\n"
    if use_task_id:
        s += "#SBATCH --array=1-100:{tasks_per_node}\n".format(tasks_per_node=int(tasks_per_node))
        s += "#SBATCH --ntasks-per-node={tasks_per_node}\n".format(tasks_per_node=int(tasks_per_node))
        s += "#SBATCH --nodes={nodes}\n".format(nodes=int(nodes))
    else:
        s += "#SBATCH --array=1\n"
        s += "#SBATCH --ntasks-per-node=1\n"
        s += "#SBATCH --nodes=1\n"
    s += "#SBATCH --output={}/%A_o.txt\n".format(slurm_log_dir)
    s += "#SBATCH --error={}/%A_e.txt\n".format(slurm_log_dir)
    s += "#SBATCH --time=3:00:00\n"
    s += "# #SBATCH --mail-type=FAIL\n"
    s += "# #SBATCH --mail-user={}\n".format(slurm_mail)
    s += "# select project\n"
    s += "jutil env activate -p {}\n".format(account)
    s += "# ensure SLURM output/errors directory exists\n"
    s += "# run tasks\n"
    s += "srun --exclusive -n ${{SLURM_NTASKS}} {}\n".format(slurm_sh_file)
    log.trace("Create {} Input:\n{}".format(slurm_file, s))
    with open(slurm_file, 'w') as f:
        f.write(s)

def create_results_slurm(log, slurm_file, slurm_sh_file, sample_id, account, slurm_log_dir, slurm_mail, sample_slurm_job_id):
    s = ""
    s += "#!/bin/bash -x\n"
    s += "#SBATCH --job-name={}_R_COVID\n".format(sample_id)
    s += "#SBATCH --account={}\n".format(account)
    s += "#SBATCH --partition=batch\n"
    s += "#SBATCH --array=1\n"
    s += "#SBATCH --ntasks-per-node=1\n"
    s += "#SBATCH --nodes=1\n"
    s += "#SBATCH --output={}/%A_o.txt\n".format(slurm_log_dir)
    s += "#SBATCH --error={}/%A_e.txt\n".format(slurm_log_dir)
    s += "#SBATCH --dependency=afterok:{}\n".format(sample_slurm_job_id)
    s += "#SBATCH --time=3:00:00\n"
    s += "# #SBATCH --mail-type=FAIL\n"
    s += "# #SBATCH --mail-user={}\n".format(slurm_mail)
    s += "# select project\n"
    s += "jutil env activate -p {}\n".format(account)
    s += "# ensure SLURM output/errors directory exists\n"
    s += "# run tasks\n"
    s += "srun --exclusive -n ${{SLURM_NTASKS}} {}\n".format(slurm_sh_file)
    log.trace("Create {} Input:\n{}".format(slurm_file, s))
    with open(slurm_file, 'w') as f:
        f.write(s)


def make_executable(path):
    mode = os.stat(path).st_mode
    mode |= (mode & 0o444) >> 2    # copy R bits to X
    os.chmod(path, mode)


def create_results_slurm_sh(log, slurm_sh_file, csv_input_file, git_dir_src, output_root_dir, sample_id, export_dir, export_offset):
    s = ""
    s += "#!/bin/bash\n"
    s += "TASK_ID=$((${SLURM_ARRAY_TASK_ID}+${SLURM_LOCALID}))\n" # 1+[0..49] | 51+[0..49]
    s += "TASK_DIR=${SCRATCH}/run_${SLURM_ARRAY_JOB_ID}/task_${TASK_ID}\n"
    s += "mkdir -p ${TASK_DIR}\n"
    s += "echo \"TASK ${TASK_ID}: Running in job-array ${SLURM_ARRAY_JOB_ID} on \
          `hostname` and dump output to ${TASK_DIR}\"\n"
    s += "source ${PROJECT}/.local/share/venvs/covid19dynstat_jusuf/bin/activate\n"
    results_to_csv_py = "results_to_csv.py"
    s += "cd {}\n".format(git_dir_src)
    if export_dir:
        s += "#THEANO_FLAGS=\"base_compiledir=${{TASK_DIR}}/,floatX=float32,device=cpu,openmp=True,mode=FAST_RUN,warn_float64=warn\" python3 {results_to_csv_py} {sample_id} --csvinputfile {csv_input_file} --outputrootdir {output_root_dir} --exportdir {export_dir} --exportoffset {export_offset} &>> ${{TASK_DIR}}/log.txt\n".format(results_to_csv_py=results_to_csv_py, sample_id=sample_id, csv_input_file=csv_input_file, output_root_dir=output_root_dir, export_dir=export_dir, export_offset=export_offset) 
    else:
        s += "#THEANO_FLAGS=\"base_compiledir=${{TASK_DIR}}/,floatX=float32,device=cpu,openmp=True,mode=FAST_RUN,warn_float64=warn\" python3 {results_to_csv_py} {sample_id} --csvinputfile {csv_input_file} --outputrootdir {output_root_dir} &>> ${{TASK_DIR}}/log.txt\n".format(results_to_csv_py=results_to_csv_py, sample_id=sample_id, csv_input_file=csv_input_file, output_root_dir=output_root_dir) 
    log.trace("Create {} Input:\n{}".format(slurm_sh_file, s))
    with open(slurm_sh_file, 'w') as f:
        f.write(s)
    make_executable(slurm_sh_file)

def create_sample_slurm_sh(log, slurm_sh_file, csv_input_file, git_dir_src, output_root_dir, sample_id, omp_num_threads):
    s = ""
    s += "#!/bin/bash\n"
    s += "TASK_ID=$((${SLURM_ARRAY_TASK_ID}+${SLURM_LOCALID}))\n" # 1+[0..49] | 51+[0..49]
    s += "TASK_DIR=${SCRATCH}/run_${SLURM_ARRAY_JOB_ID}/task_${TASK_ID}\n"
    s += "mkdir -p ${TASK_DIR}\n"
    s += "echo \"TASK ${TASK_ID}: Running in job-array ${SLURM_ARRAY_JOB_ID} on \
          `hostname` and dump output to ${TASK_DIR}\"\n"
    s += "source ${PROJECT}/.local/share/venvs/covid19dynstat_jusuf/bin/activate\n"
    sample_model_py = "sample_model.py"
    s += "cd {}\n".format(git_dir_src)
    s += "#THEANO_FLAGS=\"base_compiledir=${{TASK_DIR}}/,floatX=float32,device=cpu,openmp=True,mode=FAST_RUN,warn_float64=warn\" OMP_NUM_THREADS={omp_num_threads} python3 {sample_model_py} {sample_id} --csvinputfile {csv_input_file} &>> ${{TASK_DIR}}/log.txt\n".format(sample_model_py=sample_model_py, sample_id=sample_id, csv_input_file=csv_input_file, omp_num_threads=omp_num_threads)
    log.trace("Create {} Input:\n{}".format(slurm_sh_file, s))
    with open(slurm_sh_file, 'w') as f:
        f.write(s)
    make_executable(slurm_sh_file)


def submit_job(log, slurm_jobfile, submit_dir, sbatch_addargs=''):
    log.info("Submit slurm job: {}".format(slurm_jobfile))
    log.info("Submit slurm dir: {}".format(submit_dir))
    # build sbatch command
    sbatch_args = sbatch_addargs + " " + slurm_jobfile
    sbatch_cmd = ['sbatch'] + sbatch_args.split()
    log.trace("sbatch_cmd: {}".format(sbatch_cmd))

    # submit SLURM job
    process = Popen(sbatch_cmd, stdin=PIPE, stdout=PIPE,
                    stderr=PIPE, cwd=submit_dir)

    # block until finished and output stdout, stderr
    stdout, stderr = process.communicate()
    sbatch_out = stdout.decode("utf-8")
    sbatch_err = stderr.decode("utf-8")

    log.debug("Slurm stdout: {}".format(sbatch_out))
    log.debug("Slurm stderr: {}".format(sbatch_err))

    if process.returncode != 0:
        raise Exception("Exit code is not 0. Exit\
                        code: {}".format(process.returncode))

    # get SLURM job id
    slurm_jobid = ''
    if sbatch_out:
        slurm_jobid = sbatch_out.split()[-1]
    log.info("Slurm jobid: {}".format(slurm_jobid))

    # save SLURM job id to file
    if slurm_jobid:
        with open(os.path.join(submit_dir,
                               slurm_jobfile + ".jobid"), "w") as f:
            f.write("jobid: {}".format(slurm_jobid))
    return slurm_jobid


def status_job(log, slurm_jobid):
    # build squeue command
    log.debug("Get Status of Job: {}".format(slurm_jobid))
    squeue_cmd = ['squeue', '-l', '-j', slurm_jobid]
    log.trace("squeue_cmd: {}".format(squeue_cmd))
    # show status
    squeue_out = ''
    process = Popen(squeue_cmd, stdin=PIPE, stdout=PIPE, stderr=PIPE)
    stdout, stderr = process.communicate()
    if stderr:
        log.trace("Stdout: {}".format(stdout.decode("utf-8")))
        log.trace("Stderr: {}".format(stderr.decode("utf-8")))
        # raise IpyExit
    return stdout.decode("utf-8")
