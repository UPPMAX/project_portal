#!/bin/env python


import requests
import os
import json
import sys
import pdb
import logging
from pprint import pprint
from datetime import datetime, timedelta

logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] %(levelname)s [%(name)s.%(funcName)s:%(lineno)d] %(message)s",
    datefmt="%d/%b/%Y %H:%M:%S",
    stream=sys.stdout)


state = {}


#  ____  _   _ ____       _    ____ ___
# / ___|| | | |  _ \     / \  |  _ \_ _|
# \___ \| | | | |_) |   / _ \ | |_) | |
#  ___) | |_| |  __/   / ___ \|  __/| |
# |____/ \___/|_|     /_/   \_\_|  |___|
#
# SUP API

if 1:
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
    storage = {}

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

# for each cluster

    # fetch all jobs overlapping the time period

    # fetch efficiency data for all jobs overlapping the time period

    # for each day in time period
    
        # sum up corehours from all jobs running this day

        # save to time line


    # summarize per user the used corehours and their efficiency








#url = "https://accounting.snic.se:6143/sgas/customquery/usage-summary?machine_name=rackham.uppmax.uu.se&from=2019-10-01&to=2019-11-01"
#url = "https://accounting.snic.se:6143/sgas/customquery/usage?machine_name=rackham.uppmax.uu.se&from=2019-10-01&to=2019-11-01"
projid       = "sllstore2017033"
storage_sys  = "crex1.uppmax.uu.se"
share        = "PROJECT"
date_from    = "2022-12-01"
date_to      = "2022-12-02"
interval     = "day"

# summary
# https://accounting.snic.se:6143/sgas/customquery/usage-summary?machine_name=<clustername(fqdn)>&from=<from>&to=<to>
#url = f"https://accounting.snic.se:6143/sgas/customquery/usage-summary?machine_name={storage_sys}&from={date_from}&to={date_to}"

# project
url = f"https://accounting.snic.se:6143/sgas/customquery/storage-snic-max-for-project?project_name=sllstore2017033&storage_system={storage_sys}&storage_share={share}&from={date_from}&to={date_to}&interval={interval}"

# average usage per resource
url = f'https://accounting.snic.se:6143/sgas/customquery/storage-snic-average?storage_system={storage_sys}&storage_share={share}&from={date_from}&to={date_to}'

response     = requests.get(url, cert=cert)
storage_data = response.json()

print(storage_data)
pdb.set_trace()



