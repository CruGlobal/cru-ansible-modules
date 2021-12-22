#!/opt/rh/python27/root/usr/bin/python
# -*- coding: utf-8 -*-
from ansible.module_utils.basic import *
from ansible.module_utils.facts import *
from ansible.module_utils._text import to_native
from ansible.module_utils._text import to_text
# from ansible.error import AnsibleError
# import commands
import subprocess
from subprocess import (PIPE, Popen)
import sys
import os

ANSIBLE_METADATA = {'status': ['stableinterface'],
                    'supported_by': 'Cru DBA team',
                    'version': '0.1'}

DOCUMENTATION = '''
---
module: rmtnsentry - (rm)remove (tns)tnsnames (entry)
short_description: Given a database name without domain this module will cleanly
remove it from the tnsnames.ora file.

notes: Success or Failure will be returned once the module completes.

requirements: [ python2.* ]
author: "DBA Oracle module Team"
'''

EXAMPLES = '''

    - name: remove tnsnames entry
      rmtnsentry:
        remove_db: "JFT"
        tns_loc: "{{ tns_names_location }}"
        backup_first: True
        debugging: True

    "ansible_facts": {
    },
    "msg": "JFT was successfully removed from tnsnames.ora"

    Notes:
      - remove_db    - Give the db name. Not the SID. Don't add the domain.
      - backup_first - True/False Create a backup of the tnsnames.ora file before editing.
      - debugging    - True/False Turn debugging on.
                       debugging output can be found here: /tmp/mod_debug.log
                       on the remote host.

'''

affirm = [ 'True', 'TRUE', True, 'T', 't', 'true', 'Yes', 'YES', 'Y', 'y']
debugme = True
host_debug_log = os.path.expanduser("~/.mod_debug.log")
tns_names_filename = "tnsnames.ora"
tns_names_location = ""
tns_abs_path = ""
tns_dir = ""
var_tns_file = ""
all_items = []
msg = ""
backup_first_default = True

# sys.path.append(r'./library/pymods/')
# from crumods import *


def debugg(dbug_msg):
    """
    Append this dbug_msg string to the debug.log file.
    """
    global host_debug_log
    global debugme
    global affirm

    if debugme not in affirm:
        return()

    if not host_debug_log:
        # set_debug_log()
        if not debug_log:
            add_to_msg("Error setting debug log. No debugging will be available.")
        return()

    try:
        with open(host_debug_log, 'a') as f:
            f.write(dbug_msg + "\n")
    except:
        pass

    return()


def add_to_msg(info):
    """
    add string to return message for Ansible
    """
    global msg

    if msg:
        msg = msg + " " + info
    else:
        msg = info

    return()


def load_tnsnames_file():
    """
    Load the tnsnames.ora file on the host
    """
    global tns_names_filename
    global tns_dir
    global tns_abs_path
    global var_tns_file

    debugg("rmtnsentry :: load_tnsnames_file() :: start...tns_names_filename={}".format(tns_names_filename, tns_names_location))

    # Read a tnsnames.ora file into 'a' for testing
    tns_abs_path = "{}/{}".format(tns_dir, tns_names_filename)
    debugg("full path to tnsnames.ora = {}".format(tns_abs_path))

    with open(tns_abs_path, 'r') as f:
        var_tns_file = f.read()

    return()


def split_tns_to_list():
    """
    global var_tns_file is the entire tnsnames.ora file loaded into a variable as string
    """
    global var_tns_file
    global all_items
    an_item = ""

    debugg("rmtnsentry :: split_tns_to_list() :: start...var_tns_file={} tns_abs_path={}".format(var_tns_file, tns_abs_path))

    for item in var_tns_file.split("\n"):
        # if the line has content keep appending else go to next item
        if item:
            if an_item:
                an_item = an_item + item + "\n"
            else:
                an_item = item + "\n"
        else:
            debugg("rmtnsentry :: split_tns_to_list() :: saving this item: {}".format(an_item))
            all_items.append(an_item)
            an_item = ""

    return()


def backup_tns_file(rm_db):
    """
    if designated backup the tnsnames.ora file before writing the new one.
    """
    global tns_abs_path
    global tns_dir
    global msg

    debugg("rmtnsentry :: backup_tns_file() :: tns_abs_path={}".format(tns_abs_path))

    tstamp = "{:%Y-%m-%d_%H:%M:%S}".format(datetime.datetime.now())
    bkp_abs_path = "{dir}/{file}_rm_{db}_{ts}.bkp".format(dir=tns_dir, file="tnsnames",db=rm_db, ts=tstamp)
    cmd_str = "cp {} {}".format(tns_abs_path, bkp_abs_path)

    output = run_cmd(cmd_str)

    debugg("rmtnsentry :: backup_tns_file() ::\ntns_abs_path={}\nbkp_abs_path={}\noutput={}\ncmd_str={}\n....exiting....\n".format(tns_abs_path,bkp_abs_path,output,cmd_str))

    return(bkp_abs_path)


def write_tns_rm_entry(rm_entry):
    """
    Remove the designated database while re-creating the tnsames.ora file.
    """
    global all_items
    global tns_abs_path

    debugg("rmtnsentry :: write_tns_rm_entry() :: ...starting....rm entry=>{} tns_abs_path={}".format(rm_entry, tns_abs_path))

    with open(tns_abs_path, 'w') as f:
        for item in all_items:
            if not item.strip():
                continue
            else:
                debugg("Processing:\n" + item)
                if rm_entry.lower() not in item.lower():
                    f.write( item + "\n" )

    debugg("rmtnsentry :: write_tns_rm_entry() :: ...Exiting....".format(tns_abs_path))
    return()


def run_cmd(cmd_str):
    """
    Encapsulate all error handline in one fx. Run cmds here.
    """
    global msg

    # get the pmon process id for the running database.
    # 10189  tstdb1

    try:
        p = subprocess.Popen([cmd_str], stdout=PIPE, stderr=PIPE, shell=True)
        output, code = p.communicate()
    except subprocess.CalledProcessError as e:
       debugg("Error running: {}\n".format(cmd_str))
       debugg("Error info: {}\n".format(str(e.output)))
       add_to_msg("Error running cmd_str={} actual error: {}".format(cmd_str, e.output))
       raise Exception (msg)

    return output.strip()


def main ():
    """
    This module will read the entire tnsnames.ora file, remove all entries
    for a specified database name, then rewrite it with those entries removed.
    """
    global debugme
    global tns_loc
    global msg
    global tns_dir
    global backup_first_default
    global affirm
    ansible_facts={}

    module = AnsibleModule(
      argument_spec = dict(
        remove_db        = dict(required=True),
        tns_loc          = dict(required=False),
        backup_first     = dict(required=False),
        debugging        = dict(required=False)
      ),
      supports_check_mode=False,
    )

    # Get arguements passed from Ansible playbook
    vdb               = module.params["remove_db"]
    vtns_loc          = module.params["tns_loc"]
    vbackup_first     = module.params["backup_first"]
    vdebug            = module.params["debugging"]

    if vdebug:
        debugme = vdebug
    debugg("rmtnsentry:: main()....debugme={}".format(debugme))

    if not vdb:
        module.fail_json(msg="ERROR: No database specified to remove",ansible_facts={},changed=False)

    # If tnsnames directory location was not passed in, find it.
    if vtns_loc:
        tns_dir = vtns_loc
    else:
        cmd_str = "cat /etc/oratab | grep ASM | grep -v '^#' | awk '{print $1}' | cut -d ':' -f 2"
        output = run_cmd(cmd_str)
        tns_dir = output.strip()
    debugg("rmtnsentry:: main()....tns_dir={}".format(tns_dir))

    if not vbackup_first:
        vbackup_first = backup_first_default
    debugg("backup_first_default={}".format(vbackup_first))

    # Read tnsnames.ora file
    debugg("rmtnsentry:: main()....calling => load_tnsnames_file()")
    load_tnsnames_file()

    # Split the file into a list of strings
    debugg("rmtnsentry:: main()....calling => split_tns_to_list()")
    split_tns_to_list()

    # Backup the original tnsnames.ora file prior to overwriting.
    if vbackup_first in affirm:
        debugg("rmtnsentry:: main()....calling => backup_tns_file()")
        bkp_file = backup_tns_file(vdb)
        if bkp_file:
            bkp_msg = "tnsnames.ora backup successful, backup file: {}".format(bkp_file)
            bkp_results = "Success"
        else:
            bkp_msg = "An Error may have occurred backing up the tnsnames.ora file."
            bkp_results = "Fail"
        debugg("bkp results: {}".format(bkp_msg))
        ansible_facts.update({ "tns backup msg": bkp_msg, "tns backup results" : bkp_results })

    debugg("rmtnsentry:: main()....calling => write_tns_rm_entry() with entry to remove: {}".format(vdb))
    write_tns_rm_entry(vdb)
    ansible_facts.update( { "tnsnames_write" : "Success" } )

    module.exit_json( msg=msg, ansible_facts=ansible_facts , changed=True)

# code to execute if this program is called directly
if __name__ == "__main__":
    main()
