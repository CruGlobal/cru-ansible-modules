#!/usr/bin/env python3

from ansible.module_utils.basic import *
from ansible.module_utils.facts import *
import subprocess
import sys
import os
import json
import re
import math
# import commands
from subprocess import (PIPE, Popen)

try:
    import cx_Oracle
    cx_Oracle_found = True
except ImportError:
    cx_Oracle_found = False

# Reference links
# http://www.oracle.com/technetwork/articles/dsl/prez-python-queries-101587.html

# Notes: IAW this doc : http://docs.ansible.com/ansible/latest/dev_guide/developing_modules_general.html
# This module was setup to return a dictionary called "ansible_facts" which then makes those facts usable
# in the ansible playbook, and roles. The facts in this module are referenced by using the format:
#                    sourcefacts['key'] which returns associated value - the ref name : "sourcefacts" was created in this module
#     example:    {{ sourcefacts['oracle_version'] }} => 11.2.0.4

ANSIBLE_METADATA = {'status': ['stableinterface'],
                    'supported_by': 'Cru DBA team',
                    'version': '0.1'}

DOCUMENTATION = '''
---
module: rundbcmd
short_description: Run any sql against the database as sys, system, or any user
                   as long as user name and password are provided. This will
                   run on the local host ( users computer ) and requires cx_Oracle
                   to be installed and working.

notes: Returned values are then available to use in Ansible.
requirements: [ python3 ]
author: "DBA Oracle module Team"
'''

EXAMPLES = '''

    # To run a sql command in a database during ansible play.
    - local_action:
        module: rundbcmd
        user_id: sys or system or other user (1)
        user_password: "{{ database_passwords[db_name].sys }} or {{ database_passwords[db_name].system }} "
        db_name: "{{ db_name }}"
        host: "{{ host }}" (2)
        is_rac: "{{ orafacts['is_rac'] }}" or "{{ sourcefacts['is_rac'] }}"
        cmd: "{{ cmd_str }}"
        expect_results: "{{ expect_records }}" (3) True / False or Yes / No
        refname: "{{ refname_str }} (4)"
        ignore: False (5)
        debugging: False
      whith_items:
        - { expect_records: "Yes", cmd_str: "select systdate from dual" }
        - { expect_records: "No", cmd_str: "drop restore point rst1" }
      when: master_node|bool

      (1) user_id / user_password - sys, system or any user / password combo

      (2) host - including instance number. With domain.
          ( i.e. tlrac2.ccci.org or ploradr.dr.cru.org etc. )

      (3) expect_results - REQUIRED. If the cmd should produce an output: True, else False
          if querying a table for records : True
          if dropping restore points: False

      (4) refname - name used during Ansible play to reference the returned
          records. ( default: cmdfacts )

      (5) ignore - ignore errors. Optional. Default: False
          If you know the database may be down set ignore: True.
          If connection to the database fails the module will not throw a
          fatal error and allow the play to continue.
          If set to False this module will fail if an error occurs.

'''

# Debugging will go to: cru-ansible-oracle/bin/.utils/debug.log
msg = ""
default_ignore = False
debugme = True
default_refname = "cmdfacts"
affirm = ['True','TRUE', True, 'true', 'T', 't', 'Yes', 'YES', 'yes', 'y', 'Y']
db_home_name = "dbhome_1"
debug_log = "" # os.path.expanduser("~/.debug.log")
utils_settings_file = os.path.expanduser("~/.utils")
cru_domain = ".ccci.org"
dr_domain = ".dr.cru.org"
non_rac_dbs = [ "dw", "dr" ]
cur = None
nasty_list = [
            "truncate",
            "drop"
            "drop database"
            ]

exemption_list = [ "drop restore point" ]

def set_debug_log():
    """
    Set the debug_log value to write debugging messages
    Debugging will go to: cru-ansible-oracle/bin/.utils/debug.log
    """
    global utils_settings_file
    global debug_log
    global debugme

    if not debugme or debug_log:
        return()

    try:
        with open(utils_settings_file, 'r') as f1:
            line = f1.readline()
            while line:
                if 'ans_dir' in line:
                    tmp = line.strip().split("=")[1]
                    debug_log = tmp + "/bin/.utils/debug.log"
                    return()

                line = f1.readline()
    except:
        print("ans_dir not set in ~/.utils unable to write debug info to file")
        pass


def add_to_msg(a_msg):
    """Add the arguement to the msg to be passed out"""
    global msg

    if msg:
        msg = msg + " " + a_msg
    else:
        msg = a_msg


def debugg(db_msg):
    """if debugging is on add this to msg"""
    global debug_log
    global debugme
    global affirm
    print("debugging log: {}".format(debug_log or "Empty!"))
    if debugme not in affirm:
        return()

    add_to_msg(db_msg)

    if not debug_log:
        set_debug_log()
        if not debug_log:
            add_to_msg("Error setting debug log. No debugging will be available.")
        return()

    try:
        with open(debug_log, 'a') as f:
            f.write(db_msg + "\n")
    except:
        pass

    return()


def convert_size(arg_size_bytes, vunit):
    """Given bytes and units ( K, M, G, T)
       convert input bytes to that unit:
             vtemp = convert_size(float(vtemp),"M")
    """

    size_bytes = arg_size_bytes

    if size_bytes == 0:
       return "0B"

    vunit = vunit.upper()

    size_name = ("B", "K", "M", "G", "T", "P", "E", "Z", "Y")

    vidx = int(size_name.index(vunit))

    p = math.pow(1024, vidx)
    s = round(size_bytes / p, 2)
    # i = int(math.floor(math.log(size_bytes, 1024)))
    # p = math.pow(1024, i)
    # s = round(size_bytes / p, 2)
    return "%s%s" % (int(round(s)), size_name[vidx])


def israc(host_str=None):
    """
    Determine if a host is running RAC or Single Instance
    """
    global err_msg
    global cru_domain
    global dr_domain

    if host_str is None:
        return()

    if "org" in host_str:
        host_str = host_str.replace(cru_domain,"")
        host_str = host_str.replace(dr_domain, "")

    if "dr" in host_str or "dw" :
        return(False)

    # if the last digits is 1 or 2 ( something less than 10) and not 0 (60) return True
    if host_str[-1:].isdigit() and int(host_str[-1:]) < 10 and int(host_str[-1:]) != 0:
        return(True)
    else:
        return(False)


def run_cmd(cmd_str):
    """
    Encapsulate all error handline in one fx. Run cmds on the local machine.
    """

    try:
        process = subprocess.Popen([cmd_str], stdout=PIPE, stderr=PIPE, shell=True)
        output, code = process.communicate()
    except:
        pt1 = "Error :: run_cmd() :: running cmd_str: {}".format(cmd_str)
        add_to_msg(pt1)
        pt2 = " {}, {}, {}".format(sys.exc_info()[0], sys.exc_info()[1], sys.exc_info()[2])
        add_to_msg(pt2)
        return("")

    if output:
        return(output)
    else:
        return("")


def ck_cmd(cmd_str):
    """
    Check this command string for nasty stuff
    if nasty stuff exists:
        return True
        else False
    """
    global nasty_list

    for item in nasty_list:
        if item in cmd_str:
            key_word = item
            debugg("comparing {} with {}".format(item, cmd_str))
            if key_word == "drop" and "drop restore" in cmd_str:
                return(False)
            else:
                return(True, key_word)

    return(False, None)


# ==============================================================================
# =================================== MAIN =====================================
# ==============================================================================

def main ():
    """
    Run a sql command string
    """
    global msg
    global default_refname
    global debugme
    global db_home_name
    global cru_domain
    global dr_domain
    global affirm
    global cur
    global default_ignore
    is_rac = None
    ignore_err_flag = False
    return_values = []

    # Name to call facts dictionary being passed back to Ansible
    # This will be the name you reference in Ansible. i.e. source_facts['sga_target'] (source_facts)

    # os.system("/usr/bin/scl enable python27 bash")
    # os.system("scl enable python27 bash")
    print("here")

    module = AnsibleModule(
      argument_spec = dict(
        user_id         =dict(required=True),
        user_password   =dict(required=True),
        db_name         =dict(required=True),
        host            =dict(required=True),
        is_rac          =dict(required=False),
        cmd             =dict(required=True),
        expect_results  =dict(required=True),
        refname         =dict(required=False),
        ignore          =dict(required=False),
        debugging       =dict(required=False)
      ),
      supports_check_mode=True,
    )

    # Get arguements passed from Ansible playbook
    vuid       = module.params.get('user_id')
    vpass      = module.params.get('user_password')
    vdb        = module.params.get('db_name')
    vhost      = module.params.get('host')
    visrac     = module.params.get('is_rac')
    vcmd_str   = module.params.get('cmd')
    vxpect     = module.params.get('expect_results')
    vrefname   = module.params.get('refname')
    vignore    = module.params.get('ignore')
    vdebug     = module.params.get('debugging')

    if vdebug in affirm:
        debugme = True
        set_debug_log()
    else:
        debugme = False

    debugg("DEBUGGING SET: {}".format(debugme))

    if vignore is None:
        vignore = default_ignore

    if ".org" in vhost:
        abbr_host = vhost.replace(".ccci.org","").replace(dr_domain, "")
    else:
        abbr_host = vhost

    if not visrac:
        visrac = israc(abbr_host)

    if not cx_Oracle_found:
        module.fail_json(msg="Error: cx_Oracle module not found. Unable to proceed.")

    if not vrefname:
        refname = default_refname
    else:
        refname = vrefname
    ansible_facts = { refname : {}}

    debugg("vignore={} abbr_host={} host={} israc={}".format(vignore or "None", abbr_host or "None", vhost or "None", visrac or "None"))

    # check required vars passed in are not NULL.
    if ( vpass is None) or (vdb is None) or (vhost is None) or (vuid is None) or (vcmd_str is None):
        temp_msg = "password: {} db name: {} host: {} user name: {} cmd = {}".format(vpass or "Empty!", vdb  or "Empty!", vhost  or "Empty!", vuid  or "Empty!", vcmd_str or "Empty!")
        ansible_facts[refname].update({ 'results': {'cmd': vcmd_str, 'success': False, 'output': None } })
        debugg("required parameter missing...exiting....{}".format(temp_msg))
        module.fail_json(msg="Error: Expected paramter missing. Unable to proceed. {}", ansible_facts=ansible_facts, changed=False )

    # This will check vcmd_str for hazardous words. Drop, Drop database, truncate.
    results, key_word = ck_cmd(vcmd_str)
    if results in affirm:
        ansible_facts[refname].update({ 'results': {'cmd': vcmd_str, 'success': False, 'output': None } })
        debugg("MALICIOUS KEYWORD FOUND: {} {}...exiting..".format(results, key_word))
        module.fail_json(msg="{} Are you kidding? Unable to proceed.".format(key_word or "Error: No Key"), ansible_facts=ansible_facts, changed=False )

    # Make a cx_Oracle connection:
    # create dsn_tns
    try:
        dsn_tns = cx_Oracle.makedsn(vhost, '1521', vdb)
    except cx_Oracle.DatabaseError as exc:
        # try special case where single instance on rac:
        error, = exc.args
        if vignore:
            add_to_msg("Failed to create dns_tns: {}".format(error.message))
        else:
            err_msg="cx_Oracle dsn_tns generation error: {}, db name: {} host: {}".format(error.message or "None", vdb or "None", vhost or "None")
            add_to_msg(err_msg)
            debugg("DEBUG[01] :: ERROR creating dsn_tns {}".format(err_msg))
            ansible_facts[refname].update({ 'results': {'cmd': vcmd_str, 'success': False, 'output': None } })
            module.fail_json(msg=msg, ansible_facts=ansible_facts, changed=False)

    debugg("dsn_tns created : {}".format(str(dsn_tns)))

    # create cx_Oracle connection
    try:
        if vuid == "sys":
            con = cx_Oracle.connect(dsn=dsn_tns,user='sys',password=vpass,mode=cx_Oracle.SYSDBA)
        else:
            con = cx_Oracle.connect(vuid, vpass, dsn_tns)
    except cx_Oracle.DatabaseError as exc:
        error, = exc.args
        if vignore:
            temp_msg = "DB CONNECTION FAILED : {}".format(error.message)
            add_to_msg(temp_msg)
            debugg(" vignore: {} Error: {}".format(vignore,temp_msg))
            module.exit_json(msg=msg, ansible_facts=ansible_facts, changed="False")
        else:
            module.fail_json(msg='Database connection error: {}, tnsname: {} host: {}' % (error.message, vdb, vhost), changed=False)

    cur = con.cursor()
    debugg("Cursor successfully created.")

    # Execute the command passed in and return output if results are expected
    # It causes an error to try and retrieve results when none are produced by the command.
    try:
        cur.execute(vcmd_str)
    except cx_Oracle.DatabaseError as exc:
        try:
            cur.close()
        except:
            pass
        error, = exc.args
        ansible_facts[refname].update({ 'results': {'cmd': vcmd_str, 'success': False, 'output': None } })
        temp_msg = "Error executing command: {}".format(error.message)
        add_to_msg(temp_msg)
        debugg(temp_msg)
        debugg("ansible_facts={}".format(str(ansible_facts)))
        # Fail hard or soft
        if not vignore:
            module.exit_json(msg=msg, ansible_facts=ansible_facts, changed=False)
        else:
            module.fail_json(msg=msg, ansible_facts=ansible_facts, changed=False)

    # if no results are expected return
    if vxpect not in affirm:
        add_to_msg("Success")
        ansible_facts[refname].update({ 'results': {'cmd': vcmd_str, 'success': True, 'output': None } })
        module.exit_json( msg=msg, ansible_facts=ansible_facts , changed=True)

    # if results expected fetch and return
    try:
        vtemp = cur.fetchall()
    except cx_Oracle.DatabaseError as exc:
        error, = exc.args
        err_msg = "Error fetching results from database: {} cmd_str={}".format(error.message, vcmd_str)
        debugg("ERROR DURING FETCH: {}".format(err_msg))
        full_msg = "DEBUGGING: {} OUTPUT: {}".format(err_msg, msg)
        ansible_facts[refname].update({ 'results': {'cmd': vcmd_str, 'success': False, 'output': None } })
        module.fail_json(msg=full_msg, ansible_facts=ansible_facts, changed=False)

    for item in vtemp:
        return_values.append(item[0])
    try:
        cur.close()
    except:
        pass
    debugg("cmd={} output={} results={}".format(vcmd_str,vtemp, results))

    ansible_facts[refname].update({ 'results': {'cmd': vcmd_str, 'success': True, 'output': return_values } })

    module.exit_json( msg=msg, ansible_facts=ansible_facts , changed=True)

# code to execute if this program is called directly
if __name__ == "__main__":
    main()
