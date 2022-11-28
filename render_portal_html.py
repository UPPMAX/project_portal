import jinja2
import sys
import os
import pdb
import json
import re
from distutils.dir_util import copy_tree



# ssh -L 13377:localhost:13377 rackham.uppmax.uu.se
# cd web/
# python3 -m http.server 13377



def main():

    # make settings global
    global root_dir, web_root, portal_root, proj_list, environment, projects, project_sizes, all_projects_piechart

    # get arguemnts
    root_dir = sys.argv[1]
    web_root = sys.argv[2]


    # init
    portal_root = f"{os.path.dirname(os.path.realpath(__file__))}/portal"
    environment = jinja2.Environment(loader=jinja2.FileSystemLoader(f"{portal_root}/page_templates/"))

    # create folder structure
    copy_tree(f"{portal_root}/site_template", f"{web_root}")
    copy_tree(f"{root_dir}/plots/", f"{web_root}/plots/")

    # paths
    all_projects_piechart = "/plots/all_projects_piechart.png"

    # get proj list TODO: use this list or use projects.keys() for project ids? the differ in length
    proj_list = get_projids()

    # import users json object
    with open(f"{root_dir}/data_dump.json") as json_file:
        projects = json.load(json_file)

    #pdb.set_trace()

    # Sum all exts to get disk usage per project
    project_sizes = {}
    for project, users in projects.items():

        # init
        project_sizes[project] = 1 # to avoid division by zero later on

        for user in users:

            # get all ext sizes TODO: will location give the same answer? less work to add 2 numbers :)
            for size, freq in users[user]['locations'].values():
                project_sizes[project] += size

    # render the main page
    render_main_page()

    # render all projects
    for proj in proj_list:
        render_project_page(proj)






def render_page(template_name, output_file, data=None):
    """
    Function to render html and write to an output file.
    """
    # create template
    template = environment.get_template(template_name)

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
    for csv_file in os.listdir(f"{root_dir}/csv/"): # TODO change to json dir
        projids.add(csv_file.split(".")[0])

    # Remove summary csvs. E.g. all_extensions_size.csv
    projids = [a for a in projids if not re.search('^all_', a)]

    return list(projids)


def render_main_page():
    """
    Function to render the main page.
    """
    # create data object
    data = {'title' : "UPPMAX Project Portal",
            'projid' : "snic2022-6-147",
            'web_root' : ".",
            'project_sizes' : project_sizes,
            'all_projects_piechart' : all_projects_piechart,
           }

    render_page("main_page.html", f"{web_root}/index.html", data)





def render_project_page(proj_id):
    """
    Function to render a project's page.
    """
    # Sum all exts to get disk usage per user and total project size
    user_size = {}
    project_size = 1 # to avoid division by zero later on
    for user in projects[proj_id]:

        # init
        user_size[user] = 0

        # get all ext sizes TODO: will location give the same answer? less work to add 2 numbers :)
        for size, freq in projects[proj_id][user]['exts'].values():
            user_size[user] += size
            project_size += size

    # create data object
    data = {'title' : "UPPMAX Project Portal",
            'projid' : proj_id,
            'web_root' : "../../",
            'subtitle' : f' - {proj_id}',
            'user_size' : user_size,
            'project_size' : project_size,
           }

    os.makedirs(f"{web_root}/projects/{proj_id}", exist_ok=True)
    render_page("project_page.html", f"{web_root}/projects/{proj_id}/index.html", data)


def human_readable_size(num, suffix="B"):
    for unit in ["", "Ki", "Mi", "Gi", "Ti", "Pi", "Ei", "Zi"]:
        if abs(num) < 1024.0:
            return f"{num:3.1f}{unit}{suffix}"
        num /= 1024.0
    return f"{num:.1f}Yi{suffix}"


if __name__ == "__main__":
    main()
