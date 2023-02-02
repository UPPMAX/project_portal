#!/usr/bin/env python
# -*- coding: utf-8 -*-


#  ____  _____ ____   ____ ____  ___ ____ _____ ___ ___  _   _
# |  _ \| ____/ ___| / ___|  _ \|_ _|  _ \_   _|_ _/ _ \| \ | |
# | | | |  _| \___ \| |   | |_) || || |_) || |  | | | | |  \| |
# | |_| | |___ ___) | |___|  _ < | ||  __/ | |  | | |_| | |\  |
# |____/|_____|____/ \____|_| \_\___|_|    |_| |___\___/|_| \_|
#
# DESCRIPTION
# This script will collect data from a multitude of sources and summarize them
# per project and store the data in a database. This will make all data needed
# for generation of the project portal accessible in one single location.


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


# dict of all dicts
state = {}


#  ____  _   _ ____       _    ____ ___
# / ___|| | | |  _ \     / \  |  _ \_ _|
# \___ \| | | | |_) |   / _ \ | |_) | |
#  ___) | |_| |  __/   / ___ \|  __/| |
# |____/ \___/|_|     /_/   \_\_|  |___|
#
# SUP API
# SUP API is UPPMAX internal API that contains information about all projects and users.
if True:
    # fetch projects from api
    logging.info("Fetching projects from SUP API.")
    url = 'http://api.uppmax.uu.se:5000/api/v1/projects'
    logging.debug(f"Querying SUP: {url}")
    response = requests.get(url)

    # load the json
    # got unicodeEncodeErrors when printing state, solved by export PYTHONIOENCODING="UTF-8" in bash
    state['projects'] = response.json()



#  ____    _    __  __ ____    ____ _____ ___  ____      _    ____ _____
# / ___|  / \  |  \/  / ___|  / ___|_   _/ _ \|  _ \    / \  / ___| ____|
# \___ \ / _ \ | |\/| \___ \  \___ \ | || | | | |_) |  / _ \| |  _|  _|
#  ___) / ___ \| |  | |___) |  ___) || || |_| |  _ <  / ___ \ |_| | |___
# |____/_/   \_\_|  |_|____/  |____/ |_| \___/|_| \_\/_/   \_\____|_____|
#
# SAMS STORAGE
# This API is the same API that SUPR uses to display resource usage data about projects.
# api documentation: https://sonc.swegrid.se/wiki/SAMS_Verify

script_path     = os.path.dirname(os.path.realpath(__file__))
cert_file_path  = f"{script_path}/cert/compstore_uppmax_uu_se.crt"
key_file_path   = f"{script_path}/cert/key.pem"
cert            = (cert_file_path, key_file_path)
ca_file_path    = f"{script_path}/cert/DigiCertCA.crt"

if True:
#if False:
    # init
    resources = ["crex1.uppmax.uu.se", "crex2.uppmax.uu.se"] # TODO: can we get this list from SUPR?
    share = "PROJECT"

    # loop over all resources
    for resource in resources:

        # for each day in the date range
        for date_to in [datetime.now() - timedelta(days=n) for n in range(400)]:

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
# Here we collect corehour usage and efficiency data for all jobs run at UPPMAX.
# The will be summarized per project and users.

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
    query = f"SELECT proj_id, job_id, user, start, end, cores FROM slurm_accounting WHERE end>={period_start_padded.timestamp()}"
    slurmcur.execute(query)
    slurm_jobs_list = slurmcur.fetchall()
    slurm_jobs = { job['job_id']:dict(job) for job in slurm_jobs_list}

    # fetch all jobs overlapping the time period
    query = f"SELECT proj_id, job_id, cpu_mean, mem_peak, mem_limit FROM efficiency WHERE date_finished>='{period_start_padded.strftime('%Y-%m-%d')}'"
    effcur.execute(query)
    eff_jobs_list = effcur.fetchall()
    eff_jobs = { job['job_id']:dict(job) for job in eff_jobs_list}


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



    #   ____ _   _   ____  _____ ____    ____  ____   ___      _
    #  / ___| | | | |  _ \| ____|  _ \  |  _ \|  _ \ / _ \    | |
    # | |   | |_| | | |_) |  _| | |_) | | |_) | |_) | | | |_  | |
    # | |___|  _  | |  __/| |___|  _ <  |  __/|  _ <| |_| | |_| |
    #  \____|_| |_| |_|   |_____|_| \_\ |_|   |_| \_\\___/ \___/
    #
    # CH PER PROJ

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
        for day in [job_start + timedelta(days=i) for i in range(delta.days + 2)]: # +2 since we miss some jobs otherwise

            # skip jobs that are completely outside of current day
            if job['start'] > day.replace(hour=23, minute=59, second=59, microsecond=999999).timestamp() or job['end'] < day.replace(hour=0,  minute=0,  second=0,  microsecond=000000).timestamp():
                continue

            # how many hours of the job overlaps this day?
            overlap_start = max(job['start'],   day.replace(hour=0,  minute=0,  second=0,  microsecond=000000).timestamp()) # max of job start and day start (epoch time)
            overlap_end   = min(job['end'],     day.replace(hour=23, minute=59, second=59, microsecond=999999).timestamp()) # min of job end   and day end   (epoch time)
            overlap_hours = (overlap_end - overlap_start) /60 /60 # number of seconds converted to hours
            day_str = day.strftime('%Y-%m-%d')


            # save job in the project that ran it
            try:
                # add this jobs overlapping corehours to this day
                state['projects'][job['proj_id']]['corehours'][cluster]['daily_usage'][day_str] += overlap_hours * job['cores']
            except KeyError:

                # it will fail the first time it sees the project
                if 'corehours' not in state['projects'][job['proj_id']]:
                    state['projects'][job['proj_id']]['corehours'] = {}
                    state['projects'][job['proj_id']]['corehours'][cluster] = {}
                    state['projects'][job['proj_id']]['corehours'][cluster]['daily_usage'] = {}

                # as well as first time seeing a recource
                elif cluster not in state['projects'][job['proj_id']]['corehours']:
                    state['projects'][job['proj_id']]['corehours'][cluster] = {}
                    state['projects'][job['proj_id']]['corehours'][cluster]['daily_usage'] = {}

                # now the infrastructure should be in place to add the daily value
                state['projects'][job['proj_id']]['corehours'][cluster]['daily_usage'][day_str] = overlap_hours * job['cores']


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

        daily_usages = project['corehours'][cluster]['daily_usage']

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

        state['projects'][project['Projectname']]['corehours'][cluster]['timeline'] = corehour_timeline



    #   ____ _   _   ____  _____ ____    _   _ ____  _____ ____
    #  / ___| | | | |  _ \| ____|  _ \  | | | / ___|| ____|  _ \
    # | |   | |_| | | |_) |  _| | |_) | | | | \___ \|  _| | |_) |
    # | |___|  _  | |  __/| |___|  _ <  | |_| |___) | |___|  _ <
    #  \____|_| |_| |_|   |_____|_| \_\  \___/|____/|_____|_| \_\
    #
    # CH PER USER

    logging.info(f"Processing all {cluster} jobs per user.")
    # for each job in time period
    t0 = int(datetime.now().strftime("%s"))
    corehour_plot = dict()
    job_n = len(eff_jobs)
    counter = 0

    for job_id, job in eff_jobs.items():

        # add slurm info to the job
        try:
            job.update(slurm_jobs[job_id])
        except KeyError:
            counter += 1
            continue

        # get datetime from job start and end epoch times
        try:
            job_start = datetime.fromtimestamp(job['start'])
            job_end = datetime.fromtimestamp(job['end'])
        except ValueError as e:
            print(e)
            counter += 1
            continue

        # skip jobs that does not overlap the period
        if job['start'] > period_end.timestamp() or job['end'] < period_start.timestamp():
            counter += 1
            continue

        # how many hours of the job overlaps this period?
        overlap_start = max(job['start'],   period_start.timestamp()) # max of job start and day start (epoch time)
        overlap_end   = min(job['end'],     period_end.timestamp()) # min of job end   and day end   (epoch time)
        overlap_hours = (overlap_end - overlap_start) /60 /60 # number of seconds converted to hours


        # save job in the project and user that ran it
        try:
            # add this jobs overlapping corehours to this day
            state['projects'][job['proj_id']]['corehours'][cluster]['user'][job['user']]['corehours']  += overlap_hours * job['cores']
            state['projects'][job['proj_id']]['corehours'][cluster]['user'][job['user']]['efficiency'] += overlap_hours * job['cores'] * max(job['cpu_mean']/100, job['mem_peak']/job['mem_limit'])
        except KeyError:

            # it will fail the first time it sees the project
            if 'user' not in state['projects'][job['proj_id']]['corehours'][cluster]:
                state['projects'][job['proj_id']]['corehours'][cluster]['user'] = {}
                state['projects'][job['proj_id']]['corehours'][cluster]['user'][job['user']] = {}

            # as well as first time seeing a recource
            elif job['user'] not in state['projects'][job['proj_id']]['corehours'][cluster]['user']:
                state['projects'][job['proj_id']]['corehours'][cluster]['user'][job['user']] = {}

            # now the infrastructure should be in place to add the daily value
            state['projects'][job['proj_id']]['corehours'][cluster]['user'][job['user']]['corehours']  = overlap_hours * job['cores']
            state['projects'][job['proj_id']]['corehours'][cluster]['user'][job['user']]['efficiency'] = overlap_hours * job['cores'] * max(job['cpu_mean']/100, job['mem_peak']/job['mem_limit'])


        # print progress
        counter += 1
        if counter % 10000 == 0:
            print_progress_bar(counter, job_n, "Creating timeline from jobs")

    t1 = int(datetime.now().strftime("%s"))
    print(f"\nUser corehours:\t{t1 -t0}s")

# go through all users and rescale their efficiency now that all jobs have been processed
for project_id in state['projects']:
    if 'corehours' in state['projects'][project_id]:
        for cluster in state['projects'][project_id]['corehours']:
            if 'user' in state['projects'][project_id]['corehours'][cluster]:
                for user in state['projects'][project_id]['corehours'][cluster]['user']:

                    # divice the efficiency by corehours to get the normalized efficiency
                    state['projects'][project_id]['corehours'][cluster]['user'][user]['efficiency'] /= state['projects'][project_id]['corehours'][cluster]['user'][user]['corehours']





#  _____ ___ _     _____ ____ ___ __________
# |  ___|_ _| |   | ____/ ___|_ _|__  / ____|
# | |_   | || |   |  _| \___ \| |  / /|  _|
# |  _|  | || |___| |___ ___) | | / /_| |___
# |_|   |___|_____|_____|____/___/____|_____|
#
# FILESIZE
#pdb.set_trace()
# create a translation table from dirname to proj_id since SUP API will report project ids, not dirnames
dirname_to_projid = { project['Directory_Name']:proj_id for proj_id, project in state['projects'].items() if 'Directory_Name' in project }

# fetch the filesize data
filesize_data_filename = "/crex/proj/staff/bjornv/filesize/out.dirs.included/data_dump.json"
with open(filesize_data_filename, 'r') as filesize_data_file:
    filesize_data = json.load(filesize_data_file)

# add filesize data to state
state['projects'][proj_id]['filesize'] = {}
for directory_name, users_fs_data in filesize_data.items():

    # skip empty data
    if len(users_fs_data) == 0:
        continue

    # if a projects has a custom directory name, we have to translate the directory name to a project id
    proj_id = directory_name
    if directory_name not in state['projects']:
        proj_id = dirname_to_projid[directory_name]

    # add user based data
    state['projects'][proj_id]['filesize'] = {}
    state['projects'][proj_id]['filesize']['user'] = users_fs_data

    # summarize user data to project data
    proj_fs_data = {}
    for user, user_data in users_fs_data.items():

        for stat_type in ['exts', 'locations', 'years']:

            # for all stats in user
            for stat, size_freq in user_data[stat_type].items():

                try:
                    proj_fs_data[stat_type][stat][0] += size_freq[0]
                    proj_fs_data[stat_type][stat][1] += size_freq[1]

                except KeyError:

                    # will fail the first time
                    if stat_type not in proj_fs_data:
                        proj_fs_data[stat_type] = {}

                    # create initial list for this stat
                    proj_fs_data[stat_type][stat] = [size_freq[0], size_freq[1]]



    # add project based data
    state['projects'][proj_id]['filesize']['project'] = proj_fs_data






#  ____    ___     _______   ____    _  _____  _    ____    _    ____  _____
# / ___|  / \ \   / / ____| |  _ \  / \|_   _|/ \  | __ )  / \  / ___|| ____|
# \___ \ / _ \ \ / /|  _|   | | | |/ _ \ | | / _ \ |  _ \ / _ \ \___ \|  _|
#  ___) / ___ \ V / | |___  | |_| / ___ \| |/ ___ \| |_) / ___ \ ___) | |___
# |____/_/   \_\_/  |_____| |____/_/   \_\_/_/   \_\____/_/   \_\____/|_____|
#
# SAVE DATABASE

# connect to db and insert state object
logging.info(f"Inserting state to project portal database.")
ppdb = sqlite3.connect(f'/sw/share/compstore/production/statistics/dbs/project_portal.sqlite')
ppcur = ppdb.cursor()

for proj_id in state['projects']:

    query = f"INSERT OR REPLACE INTO current_state VALUES (?, ?, ?)"
    ppcur.execute(query, [proj_id, json.dumps(state['projects'][proj_id]), datetime.now().strftime('%Y-%m-%d') ])

query = f"INSERT OR REPLACE INTO updated VALUES (?, ?, ?)"
ppcur.execute(query, ['current_state', datetime.now().timestamp(), datetime.now().strftime('%Y-%m-%d') ])
ppdb.commit()
ppdb.close()










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


