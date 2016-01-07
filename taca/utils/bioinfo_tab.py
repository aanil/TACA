import socket
import os
import couchdb
import glob
import re
import logging
import datetime

from csv import DictReader
from taca.utils.config import CONFIG
from flowcell_parser.classes import SampleSheetParser
from collections import defaultdict
from taca.utils.misc import send_mail

logger = logging.getLogger(__name__)

def setupServer(conf):
    db_conf = conf['statusdb']
    url="http://{0}:{1}@{2}:{3}".format(db_conf['username'], db_conf['password'], db_conf['url'], db_conf['port'])
    return couchdb.Server(url)

"""Constructor for a search tree
"""
class Tree(defaultdict):
    def __init__(self, value=None):
        super(Tree, self).__init__(Tree)
        self.value = value

def merge(d1, d2):
    """ Will merge dictionary d2 into dictionary d1.
    On the case of finding the same key, the one in d1 will be used.
    :param d1: Dictionary object
    :param d2: Dictionary object
    """
    for key in d2:
        if key in d1:
            if isinstance(d1[key], dict) and isinstance(d2[key], dict):
                merge(d1[key], d2[key])
        else:
            d1[key] = d2[key]
    return d1

"""Update command
"""
def collect_runs():
    found_runs=[]
    rundir_re=re.compile("^[0-9]{6}_[A-Z0-9\-]+_[0-9]{4}_[A-Z0-9\-]{10,16}$")
    for data_dir in CONFIG['bioinfo_tab']['data_dirs']:
        if os.path.exists(data_dir):
            potential_run_dirs=glob.glob(os.path.join(data_dir, '*'))
            for run_dir in potential_run_dirs:
                if rundir_re.match(os.path.basename(os.path.abspath(run_dir))) and os.path.isdir(run_dir):
                    found_runs.append(os.path.basename(run_dir))
                    logger.info("Working on {}".format(run_dir))   
                    #updates run status     
                    update_statusdb(run_dir)
        nosync_data_dir = os.path.join(data_dir, "nosync")
        potential_nosync_run_dirs=glob.glob(os.path.join(nosync_data_dir, '*'))
        #wades through nosync directories
        for run_dir in potential_nosync_run_dirs:
             if rundir_re.match(os.path.basename(os.path.abspath(run_dir))) and os.path.isdir(run_dir):
                #update the run status
                update_statusdb(run_dir)
    

""" Gets status for a project
"""
def update_statusdb(run_dir):
    #fetch individual fields
    project_info=get_ss_projects(run_dir)
    run_id = os.path.basename(os.path.abspath(run_dir))
    
    couch=setupServer(CONFIG)
    valueskey=datetime.datetime.now().isoformat()
    db=couch['bioinfo_analysis']
    view = db.view('full_doc/pj_run_to_doc')
    #Construction and sending of individual records
    for flowcell in project_info:
        if flowcell == 'UNKNOWN':
            #At some point, remove this and rely only on the email function
            obj={'run_id':run_id, 'project_id':'ERROR_Samplesheet'}
            logger.info("INVALID SAMPLESHEET, CHECK {} FORMED AT {}".format(run_id, valueskey))
            error_emailer('no_samplesheet', run_id)
            db.save(obj)
        else:
            for lane in project_info[flowcell]:
                for sample in project_info[flowcell][lane]:
                    for project in project_info[flowcell][lane][sample]:
                        project_info[flowcell][lane][sample].value = get_status(run_dir)
                        sample_status = project_info[flowcell][lane][sample].value
                        
                        obj={'run_id':run_id, 'project_id':project, 'flowcell': flowcell, 'lane': lane, 
                             'sample':sample, 'status':sample_status, 'values':{valueskey:{'user':'taca','sample_status':sample_status}} }
                        #If entry exists, append to existing
                        if len(view[[project, flowcell, lane, sample]].rows) >= 1:
                            remote_doc= view[[project, flowcell, lane, sample]].rows[0].value
                            remote_status=remote_doc["sample_status"]
                            #Only updates the listed statuses
                            if remote_status in ['Sequencing', 'Demultiplexing', 'QC-Failed', 'BP-Failed', 'Failed']:
                                final_obj=merge(obj, remote_doc)
                                logger.info("saving {} {} {} {} {} as  {}".format(run_id, project, 
                                flowcell, lane, sample, sample_status))
                                #updates record
                                db.save(final_obj)
                        #Creates new entry
                        else:
                            logger.info("saving {} {} {} {} {} as  {}".format(run_id, project, 
                            flowcell, lane, sample, sample_status))
                            #creates record
                            db.save(obj)
                        #Sets FC error flag
                        if not project_info[flowcell].value == None:
                            if (("Failed" in project_info[flowcell].value and "Failed" not in sample_status)
                             or ("Failed" in sample_status and "Failed" not in project_info[flowcell].value)): 
                                project_info[flowcell].value = 'Ambiguous' 
                            else:
                                project_info[flowcell].value = sample_status
            #Checks if a flowcell needs partial re-doing
            #Email error per flowcell
            if not project_info[flowcell].value == None:
                if 'Ambiguous' in project_info[flowcell].value:    
                    error_emailer('failed_run', run_name) 
""" Gets status of a sample run, based on flowcell info (folder structure)
"""
def get_status(run_dir):    
    #default state, should never occur
    status = 'ERROR'
    run_name = os.path.basename(os.path.abspath(run_dir))
    xten_dmux_folder=os.path.join(run_dir, 'Demultiplexing')
    unaligned_folder=glob.glob(os.path.join(run_dir, 'Unaligned_*'))
    nosync_pattern = re.compile("nosync")
    
    #If we're in a nosync folder
    if nosync_pattern.search(run_dir):
        status = 'New'
    #If demux folder exist (or similar)
    elif (os.path.exists(xten_dmux_folder) or unaligned_folder):
        status = 'Demultiplexing'
    #If RTAcomplete doesn't exist
    elif not (os.path.exists(os.path.join(run_dir, 'RTAComplete.txt'))):
        status = 'Sequencing'
    return status

"""Fetches project, FC, lane & sample (sample-run) status for a given folder
"""
def get_ss_projects(run_dir):
    proj_tree = Tree()
    proj_pattern=re.compile("(P[0-9]{3,5})_[0-9]{3,5}")
    lane_pattern=re.compile("^[A-H]([1-8]{1,2})$")
    sample_pattern=re.compile("(P[0-9]{3,5}_[0-9]{3,5})")
    run_name = os.path.basename(os.path.abspath(run_dir))
    current_year = '20' + run_name[0:2]
    run_name_components = run_name.split("_")
    FCID = run_name_components[3][1:]
    newData = False
    
    xten_samplesheets_dir = os.path.join(CONFIG['bioinfo_tab']['xten_samplesheets'],
                                    current_year)
    hiseq_samplesheets_dir = os.path.join(CONFIG['bioinfo_tab']['hiseq_samplesheets'],
                                    current_year)
    FCID_samplesheet_origin = os.path.join(hiseq_samplesheets_dir, '{}.csv'.format(FCID))
    #if it is not hiseq
    if not os.path.exists(FCID_samplesheet_origin):
        FCID_samplesheet_origin = os.path.join(xten_samplesheets_dir, '{}.csv'.format(FCID))
        #if it is not xten
        if not os.path.exists(FCID_samplesheet_origin):
            #if it is miseq
            FCID_samplesheet_origin = os.path.join(run_dir,'Data','Intensities','BaseCalls', 'SampleSheet.csv')
            if not os.path.exists(FCID_samplesheet_origin):
                FCID_samplesheet_origin = os.path.join(run_dir,'SampleSheet.csv')
                if not os.path.exists(FCID_samplesheet_origin):
                    logger.warn("Cannot locate the samplesheet for run {}".format(run_dir))
                    return ['UNKNOWN']

        ss_reader=SampleSheetParser(FCID_samplesheet_origin)
        if 'Description' in ss_reader.header and ss_reader.header['Description'] not in ['Production', 'Application']:
            #This is a non platform MiSeq run. Disregard it.
            return []
        data=ss_reader.data

    else:
        csvf=open(FCID_samplesheet_origin, 'rU')
        data=DictReader(csvf)

    proj_n_sample = False
    lane = False
    for d in data:
        for v in d.values():
            #if project is found
            if proj_pattern.search(v):
                projects = proj_pattern.search(v).group(1)
                #sample is also found
                samples = sample_pattern.search(v).group(1)
                proj_n_sample = True
                
            #if a lane is found
            elif lane_pattern.search(v):
                #In miseq case, writes off a well hit as lane 1
                lane_inner = re.compile("[A-H]")
                if lane_inner.search(v):
                    lanes = 1
                else:
                    lanes = lane_pattern.search(v).group(1)
                lane = True
         
        #Populates structure
        if proj_n_sample and lane:
            proj_tree[FCID][lanes][samples][projects]
            proj_n_sample = False
            lane = False
    return proj_tree

"""Sends a custom error e-mail
    :param flag e-mail state
    :param info variable that describes the record in some way
"""
def error_emailer(flag, info):
    recipients = CONFIG['mail']['recipients']
    
    #no_samplesheet: A run was moved back due to QC/BP-Fail. Some samples still passed
    #failed_run: Samplesheet for a given project couldn't be found
    
    body='TACA has encountered an issue that might be worth investigating\n'
    body+='The offending entry is: '
    body+= info
    body+='\n\nSincerely, TACA'

    if (flag == 'no_samplesheet'):
        subject='ERROR, Samplesheet error'
    elif (flag == "failed_run"):
        subject='WARNING, Reinitialization of partially failed FC'
       
    hourNow = datetime.datetime.now().hour 
    if hourNow == 7 or hourNow == 12 or hourNow == 16:
        send_mail(subject, body, recipients)
    

    
    