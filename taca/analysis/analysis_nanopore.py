"""Nanopore analysis methods for TACA."""
import os
import logging
import glob
import csv
import subprocess
import shutil
import smtplib

from datetime import datetime
from taca.utils.config import CONFIG
from taca.utils.minion_barcodes import BARCODES
from taca.utils.transfer import RsyncAgent, RsyncError
from taca.utils.misc import send_mail

logger = logging.getLogger(__name__)

def find_runs_to_process():
    nanopore_data_dir = CONFIG.get('nanopore_analysis').get('data_dir')[0]
    found_run_dirs = []
    try:
        found_top_dirs = [os.path.join(nanopore_data_dir, top_dir) for top_dir in os.listdir(nanopore_data_dir)
                 if os.path.isdir(os.path.join(nanopore_data_dir, top_dir))
                 and top_dir != 'nosync']
    except OSError:
        logger.warn('There was an issue locating the following directory: {}. \
        Please check that it exists and try again.'.format(nanopore_data_dir))
    # Get the actual location of the run directories in /var/lib/MinKnow/data/USERDETERMINEDNAME/USERDETSAMPLENAME/run
    if found_top_dirs:
        for top_dir in found_top_dirs:
            for sample_dir in os.listdir(top_dir):
                for run_dir in os.listdir(os.path.join(top_dir, sample_dir)):
                    found_run_dirs.append(os.path.join(top_dir, sample_dir, run_dir))
    else:
        logger.warn('Could not find any run directories in {}'.format(nanopore_data_dir))
    return found_run_dirs

def process_run(run_dir):
    logger.info('Processing run: {}'.format(run_dir))
    summary_file = os.path.join(run_dir, 'final_summary.txt')
    demux_dir = os.path.join(run_dir, 'nanoseq_output')
    analysis_exit_status_file = os.path.join(run_dir, '.exitcode_for_nanoseq')
    email_recipients = CONFIG.get('mail').get('recipients')
    if os.path.isfile(summary_file) and not os.path.isdir(demux_dir):
        logger.info('Sequencing done for run {}. Attempting to start analysis.'.format(run_dir))
        found_sample_sheets = glob.glob(run_dir + '/*sample_sheet.csv')
        if len(found_sample_sheets) == 0:
            sample_sheet = parse_lims_sample_sheet(run_dir)
        elif len(found_sample_sheets) == 1:
            sample_sheet = found_sample_sheets[0]
        else:
            sample_sheet = ''
        if os.path.isfile(sample_sheet):
            start_analysis_pipeline(run_dir, sample_sheet)
        else:
            logger.warn('Samplesheet not found for run {}. Operator notified. Skipping.'.format(run_dir))
            email_subject = ('Samplesheet missing for run {}'.format(os.path.basename(run_dir)))
            email_message = 'There was an issue locating the samplesheet for run {}.'.format(run_dir)
            send_mail(email_subject, email_message, email_recipients)
    elif os.path.isdir(demux_dir) and not os.path.isfile(analysis_exit_status_file):
        logger.info('Analysis has started for run {} but is not yet done. Skipping.'.format(run_dir))
    elif os.path.isdir(demux_dir) and os.path.isfile(analysis_exit_status_file):
        analysis_successful = check_exit_status(analysis_exit_status_file)
        if analysis_successful:
            run_id = os.path.basename(run_dir)
            transfer_log = CONFIG.get('nanopore_analysis').get('transfer').get('transfer_file')
            if is_not_transferred(run_id, transfer_log):
                if transfer_run(run_dir):
                    update_transfer_log(run_id, transfer_log)
                    logger.info('Run {} has been synced to the analysis cluster.'.format(run_dir))
                    archive_run(run_dir)
                    logger.info('Run {} is finished and has been archived. Notifying operator.'.format(run_dir))
                    email_subject = ('Run successfully processed: {}'.format(os.path.basename(run_dir)))
                    email_message = 'Run {} has been analysed, transferred and archived \
                    successfully.'.format(run_dir)
                    send_mail(email_subject, email_message, email_recipients)
                else:
                    email_subject = ('Run processed with errors: {}'.format(os.path.basename(run_dir)))
                    email_message = 'Run {} has been analysed, but an error occurred during \
                    transfer.'.format(run_dir)
                    send_mail(email_subject, email_message, email_recipients)
            else:
                logger.warn('The following run has already been transferred, \
                skipping: {}'.format(run_dir))
        else:
            logger.warn('Analysis pipeline exited with a non-zero exit status for run {}. \
            Notifying operator.'.format(run_dir))
            email_subject = ('Analysis failed for run {}'.format(os.path.basename(run_dir)))
            email_message = 'The analysis failed for run {run}. \
            Please review the logfiles in {run}.'.format(run=run_dir)
            send_mail(email_subject, email_message, email_recipients)
    else:
        logger.info('Run {} not finished yet. Skipping.'.format(run_dir))
    return

def parse_lims_sample_sheet(run_dir):
    # Generate nanoseq samplesheet based on Lims original
    run_id = os.path.basename(run_dir)
    lims_samplesheet = get_original_samplesheet(run_id)
    if lims_samplesheet:
        parsed_samplesheet_location = parse_samplesheet(run_dir, lims_samplesheet)
    else:
        parsed_samplesheet_location = ''
    return parsed_samplesheet_location

def get_original_samplesheet(run_id):
    year_processed = run_id[0:4]
    flowcell_id = run_id.split('_')[3]
    lims_samplesheet_dir = os.path.join(CONFIG.get('nanopore_analysis').get('samplesheets_dir'),
                                            year_processed)
    found_samplesheets = glob.glob(lims_samplesheet_dir + '/*'+ flowcell_id + '*')
    if not found_samplesheets:
        logger.warn('No Lims sample sheets found for run {}'.format(run_id))
        return
    elif len(found_samplesheets) > 1:
        logger.warn('Found more than one Lims sample sheets for run {}'.format(run_id))
        return
    return found_samplesheets[0]

def parse_samplesheet(run_dir, lims_samplesheet):
    # Parse Lims samplesheet into one suitable for nanoseq
    nanopore_kit = os.path.basename(lims_samplesheet).split('_')[0]
    nanoseq_samplesheet = os.path.join(run_dir, nanopore_kit + '_sample_sheet.csv')
    content = 'sample,fastq,barcode,genome,transcriptome'
    with open(lims_samplesheet, 'r') as f:
        for line in sorted(f.readlines()):
            sample_name = line.split(',')[0]
            nanoseq_barcode = line.split(',')[1]
            if nanoseq_barcode and nanoseq_barcode in BARCODES:
                barcode = BARCODES[nanoseq_barcode]
            else:
                barcode = '0'
            content += '\n' + sample_name + ',,' + barcode + ',,' # Only need sample and barcode for now.
    with open(nanoseq_samplesheet, 'w') as f:
        f.write(content)
    return nanoseq_samplesheet

def start_analysis_pipeline(run_dir, sample_sheet):
    # start analysis detatched
    flowcell_id = get_flowcell_id(run_dir)
    kit_id = os.path.basename(sample_sheet).split('_')[0]
    if is_multiplexed(sample_sheet):
        logger.info('Run {} is multiplexed. Starting nanoseq with --barcode_kit option'.format(run_dir))
        barcode_kit = get_barcode_kit(sample_sheet)
        analysis_command = 'nextflow run nf-core/nanoseq --input ' + sample_sheet + \
            ' --input_path ' + run_dir + '/fast5/ \
            --outdir ' + run_dir + '/nanoseq_output \
            --flowcell ' + flowcell_id + \
            ' --guppy_gpu \
            --skip_alignment \
            --kit ' + kit_id + \
            ' --max_cpus 6 \
            --max_memory 20.GB \
            --barcode_kit ' + barcode_kit + \
            ' -profile singularity; echo $? > .exitcode_for_nanoseq'
    else:
        logger.info('Run {} is not multiplexed. Starting nanoseq without --barcode_kit option'.format(run_dir))
        analysis_command = 'nextflow run nf-core/nanoseq --input ' + sample_sheet + \
        ' --input_path ' + run_dir + '/fast5/ \
        --outdir ' + run_dir + '/nanoseq_output \
        --flowcell ' + flowcell_id + \
        ' --guppy_gpu \
        --skip_alignment \
        --kit ' + kit_id + \
        ' --max_cpus 6 \
        --max_memory 20.GB \
        -profile singularity; echo $? > .exitcode_for_nanoseq'

    try:
        p_handle = subprocess.Popen(analysis_command, stdout=subprocess.PIPE, shell=True, cwd=run_dir)
        logger.info('Started analysis for run {}'.format(run_dir))
    except subprocess.CalledProcessError:
        logger.warn('An error occurred while starting the analysis for run {}. Please check the logfile for info.'.format(run_dir))
    return

def get_flowcell_id(run_dir):
    # Look for flow_cell_product_code in report.md and return the corresponding value
    report_file = os.path.join(run_dir, 'report.md')
    with open(report_file, 'r') as f:
        for line in f.readlines():
            if 'flow_cell_product_code' in line:
                return line.split('"')[3]

def is_multiplexed(sample_sheet):
    # Look in the sample_sheet and return True if the run was multiplexed, else False.
    # Assumes that a run that has not been multiplexed has the barcode 0
    with open(sample_sheet, 'r') as f:
        for i, line in enumerate(f):
            if i == 1: # Only need to check first non-header line
                line_entries = line.split(',')
    if line_entries[2] == '0':
        return False
    else:
        return True

def get_barcode_kit(sample_sheet):
    # Figure out which barcode kit was used. Assumes only one kit is ever used.
    with open(sample_sheet, 'r') as f:
        for i, line in enumerate(f):
            if i == 1: # Only need to check first non-header line
                line_entries = line.split(',')
    if int(line_entries[2]) <= 12:
        return 'EXP-NBD104'
    elif int(line_entries[2]) >= 13:
        return 'EXP-NBD114'
    barcode_kit = get_barcode_kit(sample_sheet)

def check_exit_status(status_file):
    # Read pipeline exit status file and return True if 0, False if anything else
    with open(status_file, 'r') as f:
        exit_status = f.readline().strip()
    return exit_status == '0'

def is_not_transferred(run_id, transfer_log):
    # Return True if run id not in transfer.tsv, else False
        with open(transfer_log, 'r') as f:
            return run_id not in f.read()

def transfer_run(run_dir):
    #rsync dir to irma
    logger.info('Transferring run {} to analysis cluster'.format(run_dir))
    destination = CONFIG.get('nanopore_analysis').get('transfer').get('destination')
    rsync_opts = {'-Lav': None,
                  '--chown': ':ngi2016003',
                  '--chmod' : 'Dg+s,g+rw',
                  '-r' : None,
                  '--exclude' : 'work'}
    connection_details = CONFIG.get('nanopore_analysis').get('transfer').get('analysis_server')
    transfer_object = RsyncAgent(run_dir,
                                 dest_path=destination,
                                 remote_host=connection_details['host'],
                                 remote_user=connection_details['user'],
                                 validate=False,
                                 opts=rsync_opts)
    try:
        transfer_object.transfer()
    except RsyncError:
        logger.warn('An error occurred while transferring {} to the \
        ananlysis server. Please check the logfiles'.format(run_dir))
        return False
    return True

def update_transfer_log(run_id, transfer_log):
    try:
        with open(transfer_log, 'a') as f:
            tsv_writer = csv.writer(f, delimiter='\t')
            tsv_writer.writerow([run_id, str(datetime.now())])
    except IOError:
        logger.warn('Could not update the transfer logfile for run {}. \
        Please make sure gets updated.'.format(run_id, transfer_log))
    return

def archive_run(run_dir):
    # mv dir to nosync
    logger.info('Archiving run ' + run_dir)
    archive_dir = CONFIG.get('nanopore_analysis').get('finished_dir')
    top_dir = '/'.join(run_dir.split('/')[0:-2]) # Try pathlib (pathlib.Path(run_dir).parent.parent) when running completely on python3
    try:
        shutil.move(top_dir, archive_dir)
        logger.info('Successfully archived {}'.format(run_dir))
    except shutil.Error:
        logger.warn('An error occurred when archiving {}. Please check the logfile for more info.'.format(run_dir))
    return

def run_preprocessing(run):
    if run:
        process_run(os.path.normpath(run))
    else:
        runs_to_process = find_runs_to_process()
        for run_dir in runs_to_process:
            process_run(run_dir)
