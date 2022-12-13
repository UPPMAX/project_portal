#!/bin/env python
# -*- coding: utf-8 -*-

import requests
import os
import json
import sys
import pdb
import logging
from pprint import pprint
from datetime import datetime, timedelta
import sqlite3
import math
import subprocess
import re


logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] %(levelname)s [%(name)s.%(funcName)s:%(lineno)d] %(message)s",
    datefmt="%d/%b/%Y %H:%M:%S",
    stream=sys.stdout)


def print_progress_bar(index, total, label):
    n_bar = 50  # Progress bar width
    progress = index / total
    sys.stdout.write('\r')
    sys.stdout.write(f"[{'=' * int(n_bar * progress):{n_bar}s}] {int(100 * progress)}%  {label}")
    sys.stdout.flush()



state = {}


#  ____  _   _ ____       _    ____ ___
# / ___|| | | |  _ \     / \  |  _ \_ _|
# \___ \| | | | |_) |   / _ \ | |_) | |
#  ___) | |_| |  __/   / ___ \|  __/| |
# |____/ \___/|_|     /_/   \_\_|  |___|
#
# SUP API

if True:
    # fetch projects from api
    logging.info("Fetching projects from SUP API.")
    url = 'http://api.uppmax.uu.se:5000/api/v1/projects'
    logging.debug(f"Querying SUP: {url}")
    response = requests.get(url)
    
    # load the json
    # got unicodeEncodeErrors when printing state, solved by export PYTHONIOENCODING="UTF-8" in bash
    state['projects'] = response.json()

    # prune after all api calls are done
    if False:
        # filter out delivery projects
        state['projects'] = {k:v for k,v in state['projects'].items() if not v['Projectname'].startswith("delivery")}
        # filter out old projects
        state['projects'] = {k:v for k,v in state['projects'].items() if int(v['End'][0:4]) >= int((datetime.now()-timedelta(days=2*365)).strftime('%Y'))}
        # filter out delivery projects
        state['projects'] = {k:v for k,v in state['projects'].items() if not v['Projectname'].startswith("sens")}
    
    
    
    
    







#  ____    _    __  __ ____    ____ _____ ___  ____      _    ____ _____
# / ___|  / \  |  \/  / ___|  / ___|_   _/ _ \|  _ \    / \  / ___| ____|
# \___ \ / _ \ | |\/| \___ \  \___ \ | || | | | |_) |  / _ \| |  _|  _|
#  ___) / ___ \| |  | |___) |  ___) || || |_| |  _ <  / ___ \ |_| | |___
# |____/_/   \_\_|  |_|____/  |____/ |_| \___/|_| \_\/_/   \_\____|_____|
#
# SAMS STORAGE

# api documentation: https://sonc.swegrid.se/wiki/SAMS_Verify

script_path     = os.path.dirname(os.path.realpath(__file__))
cert_file_path  = f"{script_path}/cert/compstore_uppmax_uu_se.crt"
key_file_path   = f"{script_path}/cert/key.pem"
cert            = (cert_file_path, key_file_path)
ca_file_path    = f"{script_path}/cert/DigiCertCA.crt"

if True:
    # init
    resources = ["crex1.uppmax.uu.se", "crex2.uppmax.uu.se"] # TODO: can we get this list from SUPR?
    share = "PROJECT"

    # loop over all resources
    for resource in resources:

        # for each day in the date range
        for date_to in [datetime.now() - timedelta(days=n) for n in range(30)]:

            # get day before date_to to give the api a 1 day window
            date_from = date_to - timedelta(days=1)

            # convert to strings for the remainder of the interation
            date_from = date_from.strftime("%Y-%m-%d")
            date_to = date_to.strftime("%Y-%m-%d")


            # average usage on resource per project
            logging.info(f"Fetching SAMS data for {resource} on {date_to}")
            url = f'https://accounting.snic.se:6143/sgas/customquery/storage-snic-average?storage_system={resource}&storage_share={share}&from={date_from}&to={date_to}'
            logging.debug(f"Querying SAMS: {url}")

            # save the response?
            response     = requests.get(url, cert=cert)

            # populate the state object
            for proj in response.json():

                # find projects that are not in the list of projects provided by SUP api above
                if proj['project'] not in state['projects'].keys():
                    logging.warning(f"Project not in SUP list: {proj['project']}")

                try:
                    state['projects'][proj['project']]['storage'][resource][date_to] = {"file_count": proj['file_count'], "bytes_used": proj['resource_capacity_used'] }
                    state['projects'][proj['project']]['storage'][resource]['updated'] = proj['last']
                except KeyError:

                    # it will fail the first time it sees the project
                    if 'storage' not in state['projects'][proj['project']]:
                        state['projects'][proj['project']]['storage'] = {}

                    # as well as first time seeing a recource
                    state['projects'][proj['project']]['storage'][resource] = {}
                    state['projects'][proj['project']]['storage'][resource][date_to] = {"file_count": proj['file_count'], "bytes_used": proj['resource_capacity_used'] }
                    state['projects'][proj['project']]['storage'][resource]['updated'] = proj['last']











#   ____ ___  ____  _____ _   _  ___  _   _ ____  ____
#  / ___/ _ \|  _ \| ____| | | |/ _ \| | | |  _ \/ ___|
# | |  | | | | |_) |  _| | |_| | | | | | | | |_) \___ \
# | |__| |_| |  _ <| |___|  _  | |_| | |_| |  _ < ___) |
#  \____\___/|_| \_\_____|_| |_|\___/ \___/|_| \_\____/
#
# COREHOURS

# set limits
corehour_period = 30
clusters = ["rackham", "snowy"]

period_end = datetime.now()
period_start = period_end - timedelta(days=corehour_period)
period_start_padded = period_start - timedelta(days=30)

# for each cluster
for cluster in clusters:


    # connect to database
    slurmdb = sqlite3.connect(f'/sw/share/compstore/production/statistics/dbs/slurm_accounting/{cluster}.sqlite')
    slurmdb.row_factory = sqlite3.Row
    slurmcur = slurmdb.cursor()
    
    # connect to database
    effdb = sqlite3.connect(f'/sw/share/compstore/production/statistics/dbs/efficiency/{cluster}.sqlite')
    effdb.row_factory = sqlite3.Row
    effcur = effdb.cursor()

    logging.info(f"Fetching {cluster} jobs from database.")

    # fetch all jobs overlapping the time period
    query = f"SELECT proj_id, job_id, user, start, end, cores FROM slurm_accounting WHERE end>={period_start_padded.strftime('%s')}"
    slurmcur.execute(query)
    slurm_jobs_list = slurmcur.fetchall()
    slurm_jobs = { job['job_id']:job for job in slurm_jobs_list}

    # fetch all jobs overlapping the time period
    query = f"SELECT proj_id, job_id, cpu_mean, mem_peak, mem_limit FROM efficiency WHERE date_finished>={period_start_padded.strftime('%Y-%m-%d')}"
    effcur.execute(query)
    eff_jobs_list = slurmcur.fetchall()
    eff_jobs = { job['job_id']:job for job in eff_jobs_list}

    
    logging.info("Fetching running {cluster} jobs from SLURM.")
    # fetch all running jobs and add them to the slurm_jobs
    running_jobs = subprocess.run(['squeue', '-M', cluster, '-t', 'R,CG', '-o', '"%i|%a|%u|%S|%C"'], stdout=subprocess.PIPE) 

    # process each running job
    for line in running_jobs.stdout.decode('utf-8').split("\n"):
        match = re.search("(\d+)\|([\w\-]+)\|(\w+)\|([\w\-\:]+)\|(\d+)", line)
        if match:
            job_id, proj_id, user, start, cores = int(match.groups()[0]), match.groups()[1], match.groups()[2], int(datetime.strptime(match.groups()[3], "%Y-%m-%dT%H:%M:%S").timestamp()), int(match.groups()[4])
            end = int(datetime.now().timestamp())
            # construct a new job dict
            slurm_jobs[job_id] = { 'proj_id':proj_id, 'job_id':job_id, 'user':user, 'start':start, 'end':end, 'cores':cores }


    logging.info(f"Processing all {cluster} jobs.")
    # for each job in time period
    t0 = int(datetime.now().strftime("%s"))
    corehour_plot = dict()
    job_n = len(slurm_jobs)
    counter = 0
    for job_id, job in slurm_jobs.items():

        # get datetime from job start and end epoch times
        try:
            job_start = datetime.fromtimestamp(job['start'])
            job_end = datetime.fromtimestamp(job['end'])
        except ValueError as e:
            print(e)
            continue

        # day span
        delta = job_end - job_start
        for day in [job_start + timedelta(days=i) for i in range(delta.days + 1)]:

            # how many hours of the job overlaps this day?
            overlap_start = max(job['start'],   day.replace(hour=0,  minute=0,  second=0,  microsecond=000000).timestamp()) # max of job start and day start (epoch time)
            overlap_end   = min(job['end'],     day.replace(hour=23, minute=59, second=59, microsecond=999999).timestamp()) # min of job end   and day end   (epoch time)
            overlap_hours = (overlap_end - overlap_start) /60 /60 # number of seconds converted to hours
            day_str = day.strftime('%Y-%m-%d')


            # save job in the project that ran it
            try:
                # add this jobs overlapping corehours to this day
                state['projects'][job['proj_id']]['corehours'][cluster][day_str] += overlap_hours * job['cores'] 
            except KeyError:

                # it will fail the first time it sees the project
                if 'corehours' not in state['projects'][job['proj_id']]:
                    state['projects'][job['proj_id']]['corehours'] = {}
                    state['projects'][job['proj_id']]['corehours'][cluster] = {}

                # as well as first time seeing a recource
                elif cluster not in state['projects'][job['proj_id']]['corehours']:
                    state['projects'][job['proj_id']]['corehours'][cluster] = {}

                # now the infrastructure should be in place to add the daily value
                state['projects'][job['proj_id']]['corehours'][cluster][day_str] = overlap_hours * job['cores'] 

        
        # print progress
        counter += 1
        if counter % 10000 == 0:
            print_progress_bar(counter, job_n, "Creating timeline from jobs")


    t1 = int(datetime.now().strftime("%s"))
    print(f"Per job:\t{t1 -t0}s")
   
    # convert daily corehour usage to a corehour timeline with 30 day memory
    for project in [ proj for proj in state['projects'].values() if 'corehours' in proj ]:
        
        # skip this project if there are no corehours on this cluster
        if cluster not in project['corehours']:
            continue

        daily_usages = project['corehours'][cluster]

        # init timeline
        corehour_timeline = [0] * (period_end - period_start_padded).days

        # go through each day
        for i,day in enumerate([ period_start_padded + timedelta(days=n) for n in range((period_end - period_start_padded).days) ]):

            # create day string
            day_str = day.strftime('%Y-%m-%d')
            try:
                # get daily usage, if any, otherwise 0
                daily_usage = daily_usages.get(day_str, 0)
            except Exception as e:
                print(e)
                pdb.set_trace()

            # if there is any usage
            if daily_usage:

                # add the usage to all affected days
                for j in range(i, min(i+30, len(corehour_timeline))):
                    corehour_timeline[j] += daily_usage

        state['projects'][project['Projectname']]['corehours'][cluster] = corehour_timeline






    if False:
        active_projects = [ proj for proj in state['projects'].values() if 'corehours' in proj ]
        corehour_plot = active_projects[1]['corehours']['rackham']
        # convert to timeline
        y_vals = []
        for day in [ period_start + timedelta(days=n) for n in range(corehour_period) ]:
            daily_corehours = corehour_plot.get(day.strftime('%Y-%m-%d'), 0)
            y_vals.append(daily_corehours)


        import matplotlib.pyplot as plt
        plt.style.use('seaborn-whitegrid')
        import numpy as np

        print(project['Projectname'])
        fig = plt.figure()
        ax = plt.axes()
        ax.plot([ daily_usages.get((period_start_padded + timedelta(days=n)).strftime('%Y-%m-%d'), 0) for n in range((period_end - period_start_padded).days) ], label="daily usages")
        ax.plot(corehour_timeline, label="timeline")
        ax.legend()
        plt.show()

        pdb.set_trace()

