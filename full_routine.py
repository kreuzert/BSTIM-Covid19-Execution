import datetime
import os
import sys
from pathlib import Path

import argparse

import utils


def main(
    log,
    job_date,
    mail_receiver,
    base_dir,
    download_url,
    git_dir,
    job_dir,
    logging_conf,
    slurm_account,
    slurm_log_dir,
    slurm_mail,
    export_dir,
    export_offset,
    use_task_id,
    cores,
    sample_tasks,
    sample_nodes,
    sample_overload_factor,
):
    log.trace(
        "Arguments(updated paths): \ndate: {}\nmailreceiver: {}\n"
        "basedir: {}\ngitdir: {}\njobdir: {}\nloggingconf: {}\n"
        "slurmaccount: {}\nslurmlogdir: {}\nslurmmail: {}".format(
            job_date,
            mail_receiver,
            base_dir,
            git_dir,
            job_dir,
            logging_conf,
            slurm_account,
            slurm_log_dir,
            slurm_mail,
        )
    )

    # Define sample_id
    log.info("Date for this Job: {}".format(job_date))
    start_date = datetime.date(2020, 1, 28)
    sample_id = ((job_date - datetime.timedelta(days=25)) - start_date).days
    log.info("Sample_ID for this Job: {}".format(sample_id))

    # Setup directories
    log.info("Store temporary files for this job in {}".format(job_dir))
    log.trace("Create {}".format(job_dir))
    Path(job_dir).mkdir(parents=True, exist_ok=True)
    log.trace("Create {}".format(slurm_log_dir))
    Path(slurm_log_dir).mkdir(parents=True, exist_ok=True)
    git_dir_data = os.path.join(git_dir, "data")
    git_dir_src = os.path.join(git_dir, "src")
    log.trace("Create {}".format(os.path.join(job_dir, "data", "raw")))
    Path(os.path.join(job_dir, "data", "raw")).mkdir(parents=True, exist_ok=True)
    log.trace("Create {}".format(os.path.join(job_dir, "data", "diseases")))
    Path(os.path.join(job_dir, "data", "diseases")).mkdir(parents=True, exist_ok=True)
    raw_csv_fpath = os.path.join(job_dir, "data", "raw", "covid19.csv")
    data_csv_fpath = os.path.join(job_dir, "data", "diseases", "covid19.csv")
    output_dpath = os.path.join(job_dir, "csv")
    Path(output_dpath).mkdir(parents=True, exist_ok=True)

    # download csv data
    utils.download_csv(log, download_url, raw_csv_fpath)
    log.debug("Download completed")
    log.debug("Start Preprocessing")
    utils.preprocess_table(log, raw_csv_fpath, data_csv_fpath, git_dir_data, job_dir)
    log.debug("Preprocess completed")

    # Create files for sample Slurm Job
    slurm_dir = os.path.join(job_dir, "slurm")
    log.trace("Create {}".format(slurm_dir))
    Path(slurm_dir).mkdir(parents=True, exist_ok=True)
    if use_task_id:
        if sample_tasks % sample_nodes != 0:
            raise Exception(
                "sampletask % samplenodes != 0  -  Define sampletasks as a multiple of samplenodes"
            )
        tasks_per_node = sample_tasks / sample_nodes
        omp_num_threads = int(cores / tasks_per_node * sample_overload_factor)
    else:
        # placeholder
        tasks_per_node = 0
        omp_num_threads = 0

    sample_slurm_sh_file = os.path.join(slurm_dir, "sample_window.slurm.sh")
    sample_slurm_file = os.path.join(slurm_dir, "sample_window.slurm")

    utils.create_sample_slurm_sh(
        log,
        sample_slurm_sh_file,
        data_csv_fpath,
        git_dir_src,
        output_dpath,
        sample_id,
        omp_num_threads,
    )
    log.debug("Sample Slurm_sh file created")
    utils.create_sample_slurm(
        log,
        sample_slurm_file,
        sample_slurm_sh_file,
        sample_id,
        slurm_account,
        slurm_log_dir,
        slurm_mail,
        use_task_id,
        sample_tasks,
        tasks_per_node,
        sample_nodes,
    )

    log.debug("Sample Slurm file created")

    sample_slurm_jobid = utils.submit_job(log, sample_slurm_file, slurm_dir, "-vv")
    log.debug("Sample Slurm job submitted. JobId: {}".format(sample_slurm_jobid))

    slurm_status = utils.status_job(log, sample_slurm_jobid)
    log.debug("Sample Slurm job status: {}".format(slurm_status))

    results_slurm_sh_file = os.path.join(slurm_dir, "results_to_csv.slurm.sh")
    results_slurm_file = os.path.join(slurm_dir, "results_to_csv.slurm")

    utils.create_results_slurm_sh(
        log,
        results_slurm_sh_file,
        data_csv_fpath,
        git_dir_src,
        output_dpath,
        sample_id,
        export_dir,
        export_offset,
    )
    log.debug("Results Slurm_sh file created")
    utils.create_results_slurm(
        log,
        results_slurm_file,
        results_slurm_sh_file,
        sample_id,
        slurm_account,
        slurm_log_dir,
        slurm_mail,
        sample_slurm_jobid,
    )
    log.debug("Results Slurm file created")

    results_slurm_jobid = utils.submit_job(log, results_slurm_file, slurm_dir, "-vv")
    log.debug("Results Slurm job submitted. JobId: {}".format(results_slurm_jobid))


if __name__ == "__main__":
    # Parse arguments
    parser = argparse.ArgumentParser(
        description="Start slurm Jobs\
                                     for Covid19Dynstat Project."
    )
    parser.add_argument(
        "-d",
        "--date",
        type=str,
        nargs=1,
        help="Date for the Job. Format: YYYY-MM-DD (%Y-%m-%d). Default: yesterday",
    )
    parser.add_argument(
        "-m",
        "--mailreceiver",
        type=str,
        nargs="+",
        help="Receiver of the mails for this python script. Default: None",
    )
    parser.add_argument(
        "--basedir",
        type=str,
        nargs=1,
        help="Base directory of this project. Default: {}".format(
            os.path.dirname(os.path.dirname(os.path.abspath(sys.argv[0])))
        ),
    )
    parser.add_argument(
        "--gitdir",
        type=str,
        nargs=1,
        help="Git directory of this project. Has to be part of basedir. Default: BSTIM-Covid19",
    )
    parser.add_argument(
        "--downloadurl", type=str, nargs=1, help="Download RKI data from this url"
    )
    parser.add_argument(
        "--jobdir",
        type=str,
        nargs=1,
        help="Directory to store date specific files. Default: jobs",
    )
    parser.add_argument(
        "--loggingconf",
        type=str,
        nargs=1,
        help="Path to logging.conf file. Default: {}".format(
            os.path.join(os.path.dirname(os.path.abspath(sys.argv[0])), "logging.conf")
        ),
    )
    parser.add_argument(
        "--slurmaccount",
        type=str,
        nargs=1,
        help="SBATCH account defined in the slurm jobs. Default: covid19dynstat",
    )
    parser.add_argument(
        "--slurmlogdir",
        type=str,
        nargs=1,
        help="Directory for slurm log files. Has to be part of basedir. Default: slurm_logs",
    )
    parser.add_argument(
        "--slurmmail", type=str, nargs=1, help="Receiver of the slurm job mails"
    )
    parser.add_argument(
        "--exportdir", type=str, nargs=1, help="Copy the csv results to this path"
    )
    parser.add_argument(
        "--exportoffset",
        type=str,
        nargs=1,
        help="Get rid of any offsets when exporting",
    )
    parser.add_argument(
        "--usetaskid",
        type=utils.str2bool,
        nargs="?",
        const=True,
        help="Run sample_ia with multiple tasks",
    )
    parser.add_argument(
        "--cores", type=int, nargs=1, help="Number of cores on the hpc system"
    )
    parser.add_argument(
        "--sampletasks", type=int, nargs=1, help="Number of sample tasks"
    )
    parser.add_argument(
        "--samplenodes", type=int, nargs=1, help="Number of nodes for the sample job"
    )
    parser.add_argument(
        "--sampleoverloadfactor",
        type=int,
        nargs=1,
        help="Overload factor to calucalte omp num threads",
    )
    args = parser.parse_args()

    # ensure that everything is accessable by all project members
    new_umask = 0o002
    old_umask = os.umask(new_umask)
    # parse arguments
    (
        job_date,
        mail_receiver,
        base_dir,
        download_url,
        git_dir,
        job_dir,
        logging_conf,
        slurm_account,
        slurm_log_dir,
        slurm_mail,
        export_dir,
        export_offset,
        use_task_id,
        cores,
        sample_tasks,
        sample_nodes,
        sample_overload_factor,
    ) = utils.parse_arguments(args)
    # Setup Logger
    Path(job_dir).mkdir(parents=True, exist_ok=True)
    log = utils.setup_logger(
        mail_receiver, logging_conf, os.path.join(job_dir, "full_routine.log")
    )
    try:
        main(
            log,
            job_date,
            mail_receiver,
            base_dir,
            download_url,
            git_dir,
            job_dir,
            logging_conf,
            slurm_account,
            slurm_log_dir,
            slurm_mail,
            export_dir,
            export_offset,
            use_task_id,
            cores,
            sample_tasks,
            sample_nodes,
            sample_overload_factor,
        )
        os.umask(old_umask)
    except Exception:
        log.exception("Covid19 Dynstat full_routine failed. Bugfix required")
        os.umask(old_umask)
