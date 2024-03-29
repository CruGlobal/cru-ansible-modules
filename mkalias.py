#!/usr/bin/env python2
# -*- coding: utf-8 -*-
from ansible.module_utils.basic import *
from ansible.module_utils.facts import *
from ansible.module_utils._text import to_native
from ansible.module_utils._text import to_text
# from ansible.error import AnsibleError
# import commands
import subprocess
import sys
import os
import json
import re                           # regular expression
import ast
# import math
# import time
# import pexpect
# from datetime import datetime, date, time, timedelta
from subprocess import (PIPE, Popen)
# from __builtin__ import any as exists_in  # exist_in(word in x for x in mylist)

# sys.path.append(r'./pymods/crumods.py')
# from crumods import *
#Global variables
# This is global in that it calls library/pymods/crumods.py msg variable
affirm = [ 'True', 'TRUE', True, 'T', 't', 'true', 'Yes', 'YES', 'Y', 'y']
oracle_home=""
err_msg = ""
msg = ""
debugme = True
sleep_time = 2
default_ttw = 2
default_expected_num_reg_lsnrs = 1
grid_home = ""
node_number = ""
env_path = "/opt/rh/python27/root/usr/bin:/app/oracle/agent12c/core/12.1.0.3.0/bin:/app/oracle/agent12c/agent_inst/bin:/app/oracle/11.2.0.4/dbhome_1/OPatch:/app/12.1.0.2/grid/bin:/usr/lib64/qt-3.3/bin:/usr/local/bin:/bin:/usr/bin:/usr/local/sbin:/usr/sbin:/sbin:/opt/dell/srvadmin/bin:/u01/oracle/bin:/app/12.1.0.2/grid/tfa/tlorad01/tfa_home/bin"
# number of registered listeners: currently 2 ( UNKNOWN and BLOCKED )
# [oracle@tlorad01]:tstdb1:/u01/oracle/ansible_stage/utils/tstdb/dup/2018-08-12> lsnrctl status | grep tstdb
# Service "tstdb.ccci.org" has 2 instance(s).
#   Instance "tstdb1", status UNKNOWN, has 1 handler(s) for this service...
#   Instance "tstdb1", status BLOCKED, has 1 handler(s) for this service...
host_debug_log = os.path.expanduser("~/.debug.log")


ANSIBLE_METADATA = {'status': ['stableinterface'],
                    'supported_by': 'Cru DBA team',
                    'version': '0.1'}

DOCUMENTATION = '''
---
module: mkalias
short_description: Given ASM diskgroup and database name it looks for an spfile in ASM,
                   and creates an alias.
'''

EXAMPLES = '''
    # when standing up a new database using restore, or clone etc.
    # this will look in asm for a new spfile and create an alias to it.
    - name: Map new alias to spfile
      mkalias:
        db_name: "{{ db_name }}"
        asm_dg: "{{ asm_dg_name }}" (1)
      when: master_node
    Notes:
        A database name ( db_name ) can be entered with or without the instance number ( tstdb or tstdb1 )
        The ASM diskgroup ( asm_dg ) The asm diskgroup the database is located in on ASM.
            ** this can be obtained dynamically from dbfacts.py module output if run prior to this module.
'''


def add_to_msg(mytext):
    """
    Add a snippet of information to attach to the msg string being passed back to the Ansible play.
    """
    global msg

    if not msg:
        msg = mytext
    else:
        msg = msg + " " + mytext


def debugg(debug_str):
    """
    If debugging is on add debugging string to global msg and write it to the debug log file
    """
    global debugme

    if debugme:
        add_to_msg(debug_str)
        write_to_file(debug_str)


def write_to_file(info_str):
    """
    write this string to debug log
    """
    global host_debug_log

    f =  open(host_debug_log, 'a')
    for aline in info_str.split("\n"):
        f.write(aline + "\n")
    f.close()


def get_grid_home():
    """
    Determine the Grid Home directory
       using ps -eo args
       returns string

    """
    global grid_home

    output = run_sub("/bin/ps -eo args | /bin/grep ocssd.bin | /bin/grep -v grep | /bin/awk '{print $1}'")
    grid_home = (output.strip()).replace('/bin/ocssd.bin', '')

    return(grid_home)


def get_node_num():
    """
    Return current node number to ensure that srvctl is only executed on one node (1)
    """
    global grid_home
    global node_number
    global msg

    if not grid_home:
        grid_home = get_grid_home()

    output = run_sub("%s/bin/olsnodes -l -n | awk '{ print $2 }'" % (grid_home))
    node_number = output.strip()

    if israc():
        return(node_number)


def get_dbhome(vdb):
    """
    Return database home as recorded in /etc/oratab
    """

    output = run_sub("/bin/cat /etc/oratab | /bin/grep -m 1 %s | /bin/grep -o -P '(?<=:).*(?<=:)' |  /bin/sed 's/\:$//g'" % (vdb))

    ora_home = output.strip()

    debugg("get_dbhome(%s) output: %s returning: %s" % (vdb, output, ora_home))

    return(ora_home)


def israc():
    """
    Determine if a host is running RAC or Single Instance
    """
    global err_msg

    # Determine if a host is Oracle RAC ( return 1 ) or Single Instance ( return 0 )
    vproc = run_cmd("ps -ef | grep lck | grep -v grep | wc -l")
    debugg("israc()...run_cmd() returning vproc = %s" % (vproc))
    if int(vproc) > 0:
        # if > 0 "lck" processes running, it's RAC
        debugg("israc() returning True")
        return(True)
    else:
        debugg("israc() returning False")
        return(False)


def run_sub(cmd_str):
    """
    Encapsulate error handling and run subprocess cmds here
    """
    global msg

    try:
        process = subprocess.Popen([cmd_str], stdout=PIPE, stderr=PIPE, shell=True)
        output, code = process.communicate()
    except:
        add_to_msg(' Error: srvctl module get_node_num() error - retrieving node_number excpetion: %s' % (sys.exc_info()[0]))
        add_to_msg("%s, %s, %s %s" % (sys.exc_info()[0], sys.exc_info()[1], err_msg, sys.exc_info()[2]))
        raise Exception (msg)

    if output:
        return(output)
    else:
        return("")


def run_sub_env(cmd_str, env=None):
    """
     Run a subprocess with environmental vars
     passed in as dictionary: {'ORACLE_HOME': value, ORACLE_SID: value }
    """
    global msg

    try:
        # passed in python dictionary is 'env'
        os.environ['ORACLE_HOME'] = env['oracle_home']
        os.environ['ORACLE_SID'] = env['oracle_sid']
        debugg("Running cmd_str=%s with ORACLE_HOME: %s and ORACLE_SID: %s" % (cmd_str, env['oracle_home'], env['oracle_sid']))
        process = subprocess.Popen([cmd_str], stdout=PIPE, stderr=PIPE, shell=True)
        output, code = process.communicate()
    except:
        add_to_msg('Error run_sub_env cmd_str=%s env=%s' % (cmd_str,str(env)) )
        add_to_msg('%s, %s, %s' % (sys.exc_info()[0], sys.exc_info()[1], sys.exc_info()[2]) )
        raise Exception (msg)
    debugg("run_sub_env()...returning {} from cmd_str={}".format(output, cmd_str))
    if not output:
        return("")
    else:
        return(output)


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
    except:
       add_to_msg("Error run_cmd: {}".format(cmd_str))
       add_to_msg("{}, {}, {}".format(sys.exc_info()[0], sys.exc_info()[1], sys.exc_info()[2]))
       raise Exception (msg)

    return output.strip()


def get_orahome_procid(vdb):
    """
    Get database Oracle Home from the running process.
    """
    global msg

    # get the pmon process id for the running database.
    # 10189  tstdb1

    vproc = run_cmd("pgrep -lf _pmon_%s | /bin/sed 's/ora_pmon_/ /; s/asm_pmon_/ /' | /bin/grep -v sed" % (vdb))

    # if the database isnt running (no process id)
    # try getting oracle_home from /etc/oratab
    if not vproc:
        tmp_home = get_dbhome(vdb)
        if tmp_home:
            return tmp_home
        else:
            exit_msg = "Error determining oracle_home for database: %s all attempts failed! (proc id, srvctl, /etc/oratab)"
            sys.exit(exit_msg)

    # ['10189', 'tstdb1']
    vprocid = vproc.split()[0]

    # get Oracle home the db process is running out of
    # (0, ' /app/oracle/12.1.0.2/dbhome_1/')

    vhome = run_cmd(str(commands.getstatusoutput("sudo ls -l /proc/" + vprocid + "/exe | awk -F'>' '{ print $2 }' | sed 's/\/bin\/oracle$//' ")[1]))

    ora_home = vhome.strip()

    # msg = msg + "exiting get_orahome_procid(%s) returning: ora_home: %s" % (vdb,ora_home)
    debugg("get_orahome_procid() returning oracle_home=%s for db=%s",(vdb, ora_home))
    return(ora_home)


def get_asm_db():
    """
    Retrieve the ASM DB name
    """
    cmd_str = "/bin/ps -ef | grep _pmon_ | grep -v grep | grep '+'"
    output = run_cmd(cmd_str)
    tmp = output.split()
    tmp = [ i for i in tmp if '+' in i ]
    tmp = tmp[0].split('_')
    tmp = tmp[2]

    debugg("get_asm_db() returning %s" % (tmp) )
    return(tmp)


# ==============================================================================
# =================================== MAIN =====================================
# ==============================================================================
def main ():
    """
    This module will delete any old spfiles in the +ASM diskgroup for the given db
    from previous runs, if they exist and create a new alias pointing to the current
    spfile.

    This module works in conjuntion with a task which passes a registered variable
    as input to the existing_spfiles parameter of this module which contains the output to the command:
        echo ls -l +{{ database_parameters[dest_db_name].asm_dg_name }}/{{ dest_db_name }}/parameterfile | {{ grid_home }}/bin/asmcmd | awk '{ print $8}'
    that command captures existing spfiles on the ASM diskgroup
    Another task runs to create a current spfile in the ASM diskgroup before calling this module.
    This module will list the contents of the +ASM/db/parameterfile directory and take the difference between
    the previously captured spfiles and the spfiles which currently exist. The difference will be the one spfile
    created just prior to calling this module. That being the new spfile which should be used by this module (mkalias)
    """
    global msg
    global err_msg
    global grid_home
    global debugme
    global affirm
    cur_aliased_spfile = ""
    vasm_sid = "+ASM"
    voracle_user = "oracle"

    ansible_facts={}

    module = AnsibleModule(
      argument_spec = dict(
        db_name          = dict(required=True),
        asm_dg           = dict(required=True),
        existing_spfiles = dict(required=False),
        debugging        = dict(required=False)
      ),
      supports_check_mode=False,
    )

    # Get arguements passed from Ansible playbook
    vdb               = module.params["db_name"]
    vasm_dg           = module.params["asm_dg"]
    vexisting_spfiles = module.params["existing_spfiles"]
    vdebug            = module.params["debugging"]

    # vexisting_spfiles = ast.literal_eval(vexisting_spfiles)

    if vasm_dg[0] != "+":
        vasm_dg = "+%s" % (vasm_dg)

    if vdebug in affirm:
        debugme = True

    asm_db = get_asm_db()
    visrac = israc()

    if visrac:
        vnode_num = get_node_num()
        if not asm_db[-1:].isdigit():
            vasm_sid = asm_db + str(vnode_num)
        else:
            vasm_sid = asm_db

    if visrac in affirm:
        if not vdb[-1].isdigit():
            voracle_sid = vdb + vnode_num
        else:
            if vdb[-1] != "1":
                voracle_sid = vdb[:-1] + vnode_num
            vdb = vdb[:-1]
        debugg("visrac is %s, instance name: %s" % (visrac, vdb))

    vasm_home = get_dbhome(vasm_sid)

    debugg("main: called get_dbhome(%s) returned: %s" %(vasm_sid,vasm_home))
    debugg("vexisting_spfiles={} typ={}".format(vexisting_spfiles, type(vexisting_spfiles)))

    # format existing spfile input. Input will be like this: "\nspfile.271.1089906513\nspfile.681.1088866513"
    # {"changed": True, "end": "2021-11-29 15:58:44.202582", "stdout": "\nspfile.271.1089906513", "cmd": "echo ls -l +DATA3/tst -
    # db/parameterfile | /app/19.0.0/grid/bin/asmcmd | awk '{ print $8}\n", "rc": 0, "start": "2021-11-29 15:58:43.373553", "stderr": -
    # "", "delta": "0:00:00.829029", "stdout_lines": ["", "spfile.271.1089906513"], "stderr_lines": [], "failed": False}
    existing_spfiles_list = []
    debugg("list of pre-existing spfiles passed in: type {} ==>> {} vexisting_spfiles={}".format( type(vexisting_spfiles), str(vexisting_spfiles) or "None", str(vexisting_spfiles) or "vexisting_spfiles empty!"))
    if vexisting_spfiles:
        for item in vexisting_spfiles.split("\n"):
            item = item.strip()
            debugg("processing item {}".format(item.strip()))
            if "spfile" in item:
                debugg("adding item {} to the list: {}".format(item.strip(), str(existing_spfiles_list)))
                existing_spfiles_list.append(item.strip())
        # if the existing_spfiles_list isn't empty remove any duplicate entries by casting the new list to a set and then back to a list
        if existing_spfiles_list:
            existing_spfiles_list = list(set(existing_spfiles_list))
        debugg("Existing spfiles => {}".format(str(existing_spfiles_list) or "None!"))

    # Make sure an alias doesn't already exist. If it does, delete it.
    debugg("[1] Checking an alias doesnt already exist")
    cmd_str = "echo ls -l {dg}/{db}/spfile{db}.ora | {home}/bin/asmcmd".format(dg=vasm_dg.upper(),db=vdb,home=vasm_home)
    output = run_sub_env(cmd_str, {'oracle_home': vasm_home, 'oracle_sid': vasm_sid })
    debugg("main: spfile output={} from cmd_str={}".format(str(output), cmd_str))

    # if an existing alias does exist
    old_spfile = []
    if not ('does not exist' in output) and ('spfile' in output.lower()):
        debugg("[1a] existing alias found %s" % (output))
        old_spfile = [ item for item in output.split() if "spfile" in item.lower() ][0]
        debugg("old_spfile: {}".format(str(old_spfile)))
        # old_spfile would contain this format if an alias exists
        # +DATA3/DB_UNKNOWN/PARAMETERFILE/SPFILE.659.1087834493 or +DATA3/TSTDB/PARAMETERFILE/SPFILE.659.1087834493
        for item in old_spfile.split("/"):
            debugg("processing item: {}".format(item or "Empty!"))
            if "spfile" in item.lower() and ".ora" not in item:
                # If the cur_aliased_spfile is in the existing_spfiles_list, remove it.
                # It will be deleted separately below
                try:
                    existing_spfiles_list.remove(item)
                    debugg("Existing spfiles after removing alias => {}".format(str(existing_spfiles_list)))
                except:
                    debugg("Removing alias from list failed! Existing spfiles list {} removing alias => {}".format(str(existing_spfiles_list)), str(item))
                    pass

        debugg("[1a] old_spfile results {}".format(str(old_spfile)))

        # These two checks will help us not accidentally delete the wrong existing spfile
        if "+" in old_spfile and ( vdb.lower() in old_spfile.lower() or "DB_UNKNOWN" in old_spfile):
            debugg("[1b] existing spfile %s" % (old_spfile))
            add_to_msg("existing spfile: %s deleted." % (old_spfile))
            # Remove the alias
            cmd_str = "echo rmalias {asm_dg}/{db}/spfile{db}.ora | {asm_home}/bin/asmcmd".format(asm_dg=vasm_dg.upper(), db=vdb, asm_home=vasm_home)
            debugg("removing alias spfile{}.ora using cmd_str = {}".format(vdb, cmd_str))
            output = run_sub_env(cmd_str, {'oracle_home': vasm_home, 'oracle_sid': vasm_sid })
            debugg("alias removed, results {}".format(output))
            # remove the spfile
            debugg("deleted alias...removing old spfile")
            cmd_str = "echo rm {spfile} | {asm_home}/bin/asmcmd".format(spfile=old_spfile, asm_home=vasm_home)
            output = run_sub_env(cmd_str, {'oracle_home': vasm_home, 'oracle_sid': vasm_sid })
            debugg("removed old spfile using cmd {} with output {}".format(cmd_str, ouptut))

    # sometimes old spfiles can accumulate in the parameterfile directory due to previous runs. If they exist, remove them.
    debugg("Removing any previously existing spfiles from parameterfile directory existing_spfiles_list = {}".format(str(existing_spfiles_list)))
    if len(existing_spfiles_list) > 0:
        for an_spfile in existing_spfiles_list:
            debugg("REMOVING EXISTING SPFILES: {}".format(an_spfile or "None!"))
            cmd_str = "echo rm {vasm_dg}/{db}/PARAMETERFILE/{spfile} | {asm_home}/bin/asmcmd".format(vasm_dg=vasm_dg.upper(), db=vdb, spfile=an_spfile, asm_home=vasm_home)
            output = run_sub_env(cmd_str, {'oracle_home': vasm_home, 'oracle_sid': vasm_sid })
            debugg("removed {} with cmd_str={} results {}".format(an_spfile, cmd_str, output))

    # Get a list of existing system parameter files (spfile) after the list of pre-existeing system parameter files was deleted. Only the current spfile should remain.
    cmd_str = "echo ls -l {dg}/{db}/parameterfile/ | {asm_home}/bin/asmcmd".format(dg=vasm_dg.upper(), db=vdb.upper(),asm_home=vasm_home)
    output = run_sub_env(cmd_str, {'oracle_home': vasm_home, 'oracle_sid': vasm_sid })
    debugg("This is the output of {dg}/{db}/parameterfile and should only contain the new spfile = {out}".format(dg=vasm_dg.upper(), db=vdb.upper(), out=output))
    if output:
        # turn the output of the ls command into a python list: spfile_orig ( the original spfiles )
        spfile_orig = [ item for item in output.split() if "spfile" in item ][0]
        debugg("[2] get the parameterfile name: %s" % (spfile_orig))

    # Create the alias
    output = run_sub_env("echo mkalias %s/%s/parameterfile/%s %s/%s/spfile%s.ora | %s/bin/asmcmd" % (vasm_dg.upper(),vdb,spfile_orig,vasm_dg.upper(),vdb.upper(),vdb,vasm_home), {'oracle_home': vasm_home, 'oracle_sid': vasm_sid })

    debugg("[3] Alias created output %s" % (output))

    # Check the alias and return the results
    output = run_sub_env("echo ls -l %s/%s/spfile%s.ora | %s/bin/asmcmd" % (vasm_dg.upper(),vdb,vdb,vasm_home), {'oracle_home': vasm_home, 'oracle_sid': vasm_sid } )

    spfile = [ item for item in output.split() if "spfile" in item]

    debugg("[4] Check Alias spfile={}".format(str(spfile)))

    add_to_msg("Module mkalias exiting successfully. Created alias: %s/%s/%s => %s ".format(vasm_dg, vdb, spfile[0]))

    # print json.dumps( ansible_facts_dict )
    module.exit_json( msg=msg, ansible_facts={} , changed=True)

# code to execute if this program is called directly
if __name__ == "__main__":
    main()
