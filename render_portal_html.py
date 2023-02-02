import jinja2
import sys
import os
import pdb
import json
import re
from distutils.dir_util import copy_tree
import sqlite3
from pprint import pprint
from datetime import datetime, timedelta
import logging
import glob




# ssh -L 13377:localhost:13377 rackham.uppmax.uu.se
# cd web/
# python3 -m http.server 13377


# configure logging
logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] %(levelname)s [%(name)s.%(funcName)s:%(lineno)d] %(message)s",
    datefmt="%d/%b/%Y %H:%M:%S",
    stream=sys.stdout)

def main():

    # make settings global
    global root_dir
    global web_root
    global portal_root
    global proj_list
    global environment
    global projects
    global project_sizes
    global project_stats
    global main_page_plot_suffixes
    global project_page_plot_suffixes

    # get arguemnts
    web_root = sys.argv[1]


    # init
    script_root = os.path.dirname(os.path.realpath(__file__))
    portal_root = f"{script_root}/portal"
    environment = jinja2.Environment(loader=jinja2.FileSystemLoader(f"{portal_root}/page_templates/"))
    environment.filters['pretty_number'] = pretty_number
    main_page_plot_suffixes = ['total_size', 'total_freq', 'year', 'ext_size', 'ext_freq', 'location']
    project_page_plot_suffixes = ['year', 'ext_size', 'ext_freq', 'location', 'mean_efficiency']

    # create folder structure
    os.makedirs(web_root, exist_ok=True)

    # copy files
    logging.info(f"Copying {portal_root}/site_template to {web_root}")
#    copy_tree(f"{portal_root}/site_template", f"{web_root}")
    logging.info(f"Copying {portal_root}/plots to {web_root}")
#    copy_tree(f"{script_root}/plots/", f"{web_root}/projects/")

    # paths

    # load data from database
    db_file = '/sw/share/compstore/production/statistics/dbs/project_portal.sqlite'
    logging.info(f"Fetching project data from database ({db_file})")
    db = sqlite3.connect(db_file)
    db.row_factory = sqlite3.Row
    cur = db.cursor()
    query = f"SELECT proj_id, data, updated FROM current_state"
    cur.execute(query)

    # convert to dict of dicts
    logging.debug(f"Converting database results to dicts")
    projects = { result['proj_id']:{'data':json.loads(result['data']), 'updated':result['updated']} for result in cur.fetchall() }


    # Sum all exts to get disk usage per project
    logging.info(f"Restructuring data per project")
    project_stats = {}
    too_old_date = (datetime.now() - timedelta(days=365)).strftime("%Y-%m-%d")
    for proj_id, project in projects.items():

#        if proj_id == 'snic2022-6-145':
#            pdb.set_trace()

        # skip projects that are more than 1 year since expiry date
#        if project['data']['End'] < too_old_date:
#            logging.debug(f"Skipping old project {proj_id}, expired {project['data']['End']}.")
#            continue

        # init
        project_stats[proj_id] = {'fs_exts':{}, 'fs_years':{}, 'fs_locations':{}, 'storage_size':0, 'storage_freq':0, 'ch_usage':0, 'ch_eff':0, 'user':{} }
        for user in project['data']['Uppmax_Members']:
            project_stats[proj_id]['user'][user['Username']] = {'fs_exts':{}, 'fs_years':{}, 'fs_locations':{}, 'storage_size':0, 'storage_freq':0, 'ch_usage':0, 'ch_eff':0 } 






#  _____ ___ _     _____ ____ ___ __________
# |  ___|_ _| |   | ____/ ___|_ _|__  / ____|
# | |_   | || |   |  _| \___ \| |  / /|  _|
# |  _|  | || |___| |___ ___) | | / /_| |___
# |_|   |___|_____|_____|____/___/____|_____|
#
# FILESIZE

        # check if project has filesize stats
        if 'filesize' in project['data'] and len(project['data']['filesize']) > 0:

            logging.debug("Processing filesize key")
#            pdb.set_trace()
            # save ext info for project
            project_stats[proj_id]['fs_exts']      = project['data']['filesize']['project']['exts']
            project_stats[proj_id]['fs_years']     = project['data']['filesize']['project']['years']
            project_stats[proj_id]['fs_locations'] = project['data']['filesize']['project']['locations']

            # save ext info per user
            for user, user_fs_data in project['data']['filesize']['user'].items():
                
                # create user default entry if not seen before
                if user not in project_stats[proj_id]['user']:
                    project_stats[proj_id]['user'][user] = {'fs_exts':{}, 'fs_years':{}, 'fs_locations':{}, 'storage_size':0, 'storage_freq':0, 'ch_usage':0, 'ch_eff':0 }

                # save user specific storage data
                project_stats[proj_id]['user'][user]['fs_exts']      = user_fs_data['exts']
                project_stats[proj_id]['user'][user]['fs_years']     = user_fs_data['years']
                project_stats[proj_id]['user'][user]['fs_locations'] = user_fs_data['locations']

                # sum up the storage used and number of files for this user
                for loc in ['backup', 'nobackup']:
                    # if user has zero files the location keys are missing. Default to a [0,0] value for this.
                    project_stats[proj_id]['user'][user]['storage_size'] += user_fs_data['locations'].get(loc, [0,0])[0]
                    project_stats[proj_id]['user'][user]['storage_freq'] += user_fs_data['locations'].get(loc, [0,0])[1]












#  ____ _____ ___  ____      _    ____ _____
# / ___|_   _/ _ \|  _ \    / \  / ___| ____|
# \___ \ | || | | | |_) |  / _ \| |  _|  _|
#  ___) || || |_| |  _ <  / ___ \ |_| | |___
# |____/ |_| \___/|_| \_\/_/   \_\____|_____|
#
# STORAGE

        if 'storage' in project['data']:

            logging.debug("Processing storage key")

            if len(project['data']['storage']) == 1:
                # get storage system name
                storage_system = list(project['data']['storage'].keys())[0]
                
                # get latest date
                latest_date = sorted(project['data']['storage'][storage_system].keys())[-2]
                
                # save size and number of files
                project_stats[proj_id]['storage_size'] = project['data']['storage'][storage_system][latest_date]['bytes_used']
                project_stats[proj_id]['storage_freq'] = project['data']['storage'][storage_system][latest_date]['file_count']

            else:
                logging.warning(f"{proj_id} has more than 1 storage system defined in SAMS data. This is not handled yet, so now is probably a good time to implement it.")







#   ____ ___  ____  _____ _   _  ___  _   _ ____  ____
#  / ___/ _ \|  _ \| ____| | | |/ _ \| | | |  _ \/ ___|
# | |  | | | | |_) |  _| | |_| | | | | | | | |_) \___ \
# | |__| |_| |  _ <| |___|  _  | |_| | |_| |  _ < ___) |
#  \____\___/|_| \_\_____|_| |_|\___/ \___/|_| \_\____/
#
# COREHOURS

        if 'corehours' in project['data']:

            logging.debug("Processing corehours key")

            # init
            total_ch     = 0
            total_eff    = 0
            total_eff_ch = 0

            # loop over and summarize all clusters
            for cluster in project['data']['corehours']:

                # take the latest timeline value as latest usage
                total_ch += project['data']['corehours'][cluster]['timeline'][-1]

                # check if user data exists. If there are no jobs run in the last 30 days, this will be missing.
                if 'user' in project['data']['corehours'][cluster]:

                    # go through user efficiency data for this cluster and summarize to a single number
                    for username, user in project['data']['corehours'][cluster]['user'].items():

                        # add efficiency ch and unnormalized efficiency value
                        total_eff_ch += user['corehours']
                        total_eff    += user['corehours'] * user['efficiency']

                        # create user default entry if not seen before
                        if username not in project_stats[proj_id]['user']:
                            project_stats[proj_id]['user'][user] = {'fs_exts':{}, 'fs_years':{}, 'fs_locations':{}, 'storage_size':0, 'storage_freq':0, 'ch_usage':0, 'ch_eff':0 }

                        # save the user specific corehour usage and efficiency in the user specific data
                        project_stats[proj_id]['user'][username]['ch_usage'] = user['corehours']
                        project_stats[proj_id]['user'][username]['ch_eff']   = user['efficiency']
                        

            # only if some cluster had job run in the last 30 days
            if total_eff_ch:
                # normalize efficiency
                total_eff /= total_eff_ch

            # if no jobs were run in the last 30 days anywhere
            else:
                total_eff = 0


            project_stats[proj_id]['ch_usage'] = total_ch
            project_stats[proj_id]['ch_eff']   = total_eff 

#        if proj_id == 'uppmax2021-2-2':
#            pdb.set_trace()







#  ____  _____ _   _ ____  _____ ____
# |  _ \| ____| \ | |  _ \| ____|  _ \
# | |_) |  _| |  \| | | | |  _| | |_) |
# |  _ <| |___| |\  | |_| | |___|  _ <
# |_| \_\_____|_| \_|____/|_____|_| \_\
#
# RENDER

    # render the main page
    logging.info(f"Rendering main page")
    render_main_page()

    # render all projects
    logging.info("Rendering project pages")
    for proj in project_stats:
        render_project_page(proj)

        for user in project_stats[proj]['user']:
            render_project_user_page(proj, user)



def pretty_number(value, sep=" "):
    """Applies thousands separator to numbers"""
    
    # check if it's a float or int
    if isinstance(value, int):
        return '{:,d}'.format(value).replace(',', sep)

    # guess it's a float
    else:
        return '{:,.2f}'.format(float(value)).replace(',', sep)


def render_page(template_name, output_file, data=None):
    """
    Function to render html and write to an output file.
    """
    # create template
    try:
        template = environment.get_template(template_name)
    except:
        pdb.set_trace()

    # vars

    html = template.render(data)

    # write to file
    with open(output_file, 'w') as html_file:
        html_file.write(html)





def get_projids():
    """
    Get a list of all project ids from the root dir
    """

    # get all unique projids
    projids = set()
    for csv_file in os.listdir(f"{root_dir}/tmp/"): # TODO change to json dir
        projids.add(csv_file.split(".")[0])

    # Remove summary csvs. E.g. all_extensions_size.csv
    projids = [a for a in projids if not re.search('^all', a)]

    return list(projids)


def render_main_page():
    """
    Function to render the main page.
    """
    
    # fetch all png files in folder
    plot_files = list(map(os.path.basename, glob.glob(f"{web_root}/*.png")))
    
    # create data object
    data = {'title' : "UPPMAX Project Portal",
            'web_root' : ".",
            'image_prefix' : "projects/all_projects",
            'project_stats' : project_stats,
            'plot_files' : plot_files,
           }

    render_page("main_page.html", f"{web_root}/index.html", data)





def render_project_page(proj_id):
    """
    Function to render a project's page.
    """

    logging.debug(f"Rendering {proj_id} page")

    # create project folder if not existing
    os.makedirs(f"{web_root}/projects/{proj_id}", exist_ok=True)
    
    # fetch all png files in folder
    plot_files = list(map(os.path.basename, glob.glob(f"{web_root}/projects/{proj_id}/*.png")))

    # create data object
    data = {'title' : "UPPMAX Project Portal",
            'proj_id' : proj_id,
            'web_root' : "../../",
            'subtitle' : f' - {proj_id}',
            'project_data' : project_stats[proj_id],
            'plot_files' : plot_files,
           }


    render_page("project_page.html", f"{web_root}/projects/{proj_id}/index.html", data)





def render_project_user_page(proj_id, user):
    """
    Function to render a project's user page.
    """

    logging.debug(f"Rendering {proj_id}-{user} page")

    # create project folder if not existing
    os.makedirs(f"{web_root}/projects/{proj_id}/{user}", exist_ok=True)
    
    # fetch all png files in folder
    plot_files = list(map(os.path.basename, glob.glob(f"{web_root}/projects/{proj_id}/{user}/*.png")))

    # create data object
    data = {'title' : "UPPMAX Project Portal",
            'proj_id' : proj_id,
            'web_root' : "../../../",
            'subtitle' : f' - {proj_id} - {user}',
            'user_data' : project_stats[proj_id]['user'][user],
            'plot_files' : plot_files,
           }


    render_page("project_user_page.html", f"{web_root}/projects/{proj_id}/{user}/index.html", data)


def human_readable_size(num, suffix="B"):
    for unit in ["", "Ki", "Mi", "Gi", "Ti", "Pi", "Ei", "Zi"]:
        if abs(num) < 1024.0:
            return f"{num:3.1f}{unit}{suffix}"
        num /= 1024.0
    return f"{num:.1f}Yi{suffix}"


if __name__ == "__main__":
    main()
