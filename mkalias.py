#!/opt/rh/python27/root/usr/bin/python
# -*- coding: utf-8 -*-
from ansible.module_utils.basic import *
from ansible.module_utils.facts import *
from ansible.module_utils._text import to_native
from ansible.module_utils._text import to_text
# from ansible.error import AnsibleError
import commands
import subprocess
import sys
import os
import json
import re                           # regular expression
# import math
# import time
# import pexpect
# from datetime import datetime, date, time, timedelta
from subprocess import (PIPE, Popen)
from __builtin__ import any as exists_in  # exist_in(word in x for x in mylist)

#Global variables
oracle_home=""
err_msg = ""
msg = ""
DebugMe = False
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
        asm_dg: "{{ asm_dg_name }}"
      when: master_node

    Notes:
        The ASM diskgroup ( asm_dg_name ) the database is in can be entered with or without the + ( +DATA3 or DATA3 )

        The database name ( db_name ) can be entered with or without the instance number ( tstdb or tstdb1 )

'''


def get_grid_home():
    """Determine the Grid Home directory
       using ps -eo args
       returns string."""

    global grid_home
    # global module

    try:
        cmd_str = "/bin/ps -eo args | /bin/grep ocssd.bin | /bin/grep -v grep | /bin/awk '{print $1}'"
        process = subprocess.Popen([cmd_str], stdout=PIPE, stderr=PIPE, shell=True)
        output, code = process.communicate()
    except:
        custom_err_msg = 'Error [ get_gihome() ]: retrieving GRID_HOME. Error running cmd: %s' % (cmd_str)
        custom_err_msg = custom_err_msg + " %s, %s, %s" % (sys.exc_info()[0], sys.exc_info()[1], sys.exc_info()[2])
        raise Exception (custom_err_msg)

    grid_home = (output.strip()).replace('/bin/ocssd.bin', '')

    if not grid_home:
         custom_err_msg = ' Error[ get_gihome() ]: No output returned after running cmd : %s' % (cmd_str)
         custom_err_msg = custom_err_msg + "%s, %s, %s" % (sys.exc_info()[0], sys.exc_info()[1], sys.exc_info()[2])
         raise Exception (custom_err_msg)

    return(grid_home)


def get_node_num():
    """Return current node number to ensure that srvctl is only executed on one node (1)"""
    global grid_home
    global node_number

    if not grid_home:
        grid_home = get_grid_home()

    try:
        cmd_str = grid_home + "/bin/olsnodes -l -n | awk '{ print $2 }'"
        process = subprocess.Popen([cmd_str], stdout=PIPE, stderr=PIPE, shell=True)
        output, code = process.communicate()
    except:
        err_msg = ""
        err_msg = err_msg + ' Error: srvctl module get_node_num() error - retrieving node_number excpetion: %s' % (sys.exc_info()[0])
        err_msg = err_msg + "%s, %s, %s %s" % (sys.exc_info()[0], sys.exc_info()[1], err_msg, sys.exc_info()[2])
        raise Exception (err_msg)

    node_number = output.strip()

    return(node_number)


def get_dbhome(vdb):
    """Return database home as recorded in /etc/oratab"""
    global ora_home

    cmd_str = "cat /etc/oratab | grep -m 1 %s | grep -o -P '(?<=:).*(?<=:)' |  sed 's/\:$//g'" % (vdb)

    try:
        process = subprocess.Popen([cmd_str], stdout=PIPE, stderr=PIPE, shell=True)
        output, code = process.communicate()
    except:
       my_msg = my_msg + ' Error [1]: srvctl module get_orahome() error - retrieving oracle_home excpetion: %s' % (sys.exc_info()[0])
       my_msg = my_msg + "%s, %s, %s %s" % (sys.exc_info()[0], sys.exc_info()[1], my_msg, sys.exc_info()[2])
       raise Exception (my_msg)

    ora_home = output.strip()

    if not ora_home:
        my_msg = ' Error[get_dbhome()]: srvctl module get_orahome() error - retrieving oracle_home excpetion: %s' % (sys.exc_info()[0])
        my_msg = my_msg + "%s, %s, %s %s" % (sys.exc_info()[0], sys.exc_info()[1], my_msg, sys.exc_info()[2])
        raise Exception (my_msg)

    return(ora_home)


def get_orahome_procid(vdb):
    """Get database Oracle Home from the running process."""
    global msg

    # get the pmon process id for the running database.
    # 10189  tstdb1
    try:
        cmd_str = "pgrep -lf _pmon_%s | /bin/sed 's/ora_pmon_/ /; s/asm_pmon_/ /' | /bin/grep -v sed" % (vdb)
        vproc = str(commands.getstatusoutput(cmd_str)[1])
    except:
        err_cust_err_msg = 'Error: get_orahome_procid() - pgrep lf pmon: (%s)' % (sys.exc_info()[0])
        err_cust_err_msg = cust_err_msg + "%s, %s, %s %s" % (sys.exc_info()[0], sys.exc_info()[1], err_msg, sys.exc_info()[2])
        raise Exception (err_msg)

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
    try:
      vhome = str(commands.getstatusoutput("sudo ls -l /proc/" + vprocid + "/exe | awk -F'>' '{ print $2 }' | sed 's/\/bin\/oracle$//' ")[1])
    except:
      custom_err_msg = 'Error[ get_orahome_procid() ]:  (%s)' % (sys.exc_info()[0])
      err_cust_err_msg = cust_err_msg + "%s, %s, %s %s" % (sys.exc_info()[0], sys.exc_info()[1], err_msg, sys.exc_info()[2])
      raise Exception (err_msg)

    ora_home = vhome.strip()

    # msg = msg + "exiting get_orahome_procid(%s) returning: ora_home: %s" % (vdb,ora_home)

    return(ora_home)


# ==============================================================================
# =================================== MAIN =====================================
# ==============================================================================
def main ():
    """ Check the lsnrctl state using command line """
    global msg
    global err_msg
    global grid_home
    debugme = "false"
    vasm_sid = "+ASM1"
    voracle_user = "oracle"

    ansible_facts={}

    module = AnsibleModule(
      argument_spec = dict(
        db_name         = dict(required=True),
        asm_dg          = dict(required=True)
      ),
      supports_check_mode=False,
    )

    # Get arguements passed from Ansible playbook
    vdb          = module.params["db_name"]
    vasm_dg      = module.params["asm_dg"]

    if vasm_dg[0] != "+":
        vasm_dg = "+%s" % (vasm_dg)

    if not grid_home:
        vgrid_home = get_grid_home()

    vnode_num = get_node_num()

    if not vdb[-1].isdigit():
        voracle_sid = vdb + vnode_num
    else:
        if vdb[-1] != "1":
            voracle_sid = vdb[:-1] + vnode_num
        vdb = vdb[:-1]

    # Make sure an alias doesn't already exist
    try:
        os.environ['ORACLE_HOME'] = vgrid_home
        os.environ['ORACLE_SID'] = vasm_sid
        cmd_str = "echo ls -l %s/%s/spfile%s.ora | %s/bin/asmcmd" % (vasm_dg.upper(),vdb,vdb,vgrid_home)
        process = subprocess.Popen([cmd_str], stdout=PIPE, stderr=PIPE, shell=True)
        output, code = process.communicate()
    except:
        custom_err_msg = 'Error[ checking if alias already exists ]'
        custom_err_msg = custom_err_msg + "%s, %s, %s" % (sys.exc_info()[0], sys.exc_info()[1], sys.exc_info()[2])
        raise Exception (custom_err_msg)

    spfile = [ item for item in output.split() if "spfile" in item ]

    if debugme:
        msg = "[1] make sure alias doesnt already exist with cmd: %s and results: %s " % (cmd_str,spfile)

    if len(spfile) > 1:
        msg = "spfile already exists: %s/%s/%s => %s" % (vasm_dg,vdb,spfile[0],spfile[1])
        module.exit_json( msg=msg, ansible_facts={} , changed=False)

    # Else get the name of the parameterfile.
    try:
        # os.environ['USER'] = voracle_user
        os.environ['ORACLE_HOME'] = vgrid_home
        os.environ['ORACLE_SID'] = vasm_sid
        cmd_str = "echo ls -l %s/%s/parameterfile/ | %s/bin/asmcmd" % (vasm_dg.upper(),vdb,vgrid_home)
        process = subprocess.Popen([cmd_str], stdout=PIPE, stderr=PIPE, shell=True)
        output, code = process.communicate()
    except:
        custom_err_msg = 'Error [ getting parameter file name]'
        custom_err_msg = custom_err_msg + "%s, %s, %s" % (sys.exc_info()[0], sys.exc_info()[1], sys.exc_info()[2])
        raise Exception (custom_err_msg)

    spfile_orig = [ item for item in output.split() if "spfile" in item][0]

    if debugme:
        msg = msg + "[2] use this command: %s to get the parameterfile name: %s" % (cmd_str, spfile_orig)

    # Create the alias
    try:
        # os.environ['USER'] = voracle_user
        os.environ['ORACLE_HOME'] = vgrid_home
        os.environ['ORACLE_SID'] = vasm_sid
        cmd_str = "echo mkalias %s/%s/parameterfile/%s %s/%s/spfile%s.ora | %s/bin/asmcmd" % (vasm_dg,vdb,spfile_orig,vasm_dg,vdb,vdb,vgrid_home)
        process = subprocess.Popen([cmd_str], stdout=PIPE, stderr=PIPE, shell=True)
        output, code = process.communicate()
    except:
        custom_err_msg = 'Error [ getting parameter file name]'
        custom_err_msg = custom_err_msg + "%s, %s, %s" % (sys.exc_info()[0], sys.exc_info()[1], sys.exc_info()[2])
        raise Exception (custom_err_msg)

    if debugme:
        msg = msg + "[3] Alias created ? with this command: %s" % (cmd_str)

    # Check the alias and return the results
    try:
        # os.environ['USER'] = voracle_user
        os.environ['ORACLE_HOME'] = vgrid_home
        os.environ['ORACLE_SID'] = vasm_sid
        cmd_str = "echo ls -l %s/%s/spfile%s.ora | %s/bin/asmcmd" % (vasm_dg.upper(),vdb,vdb,vgrid_home)
        process = subprocess.Popen([cmd_str], stdout=PIPE, stderr=PIPE, shell=True)
        output, code = process.communicate()
    except:
        custom_err_msg = 'Error[ checking if alias already exists ]'
        custom_err_msg = custom_err_msg + "%s, %s, %s" % (sys.exc_info()[0], sys.exc_info()[1], sys.exc_info()[2])
        raise Exception (custom_err_msg)

    spfile = [ item for item in output.split() if "spfile" in item]

    if debugme:
        msg = msg + "[4] Check Alias with this command: %s and output: %s with final result (spfile): %s" % (cmd_str,output,spfile)

    msg = "Module mkalias exiting successfully. Created alias: %s/%s/%s => %s " % (vasm_dg,vdb,spfile[0],spfile[1])

    # print json.dumps( ansible_facts_dict )
    module.exit_json( msg=msg, ansible_facts={} , changed=True)

# code to execute if this program is called directly
if __name__ == "__main__":
    main()
