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
            ** this can be obtained dynamically from sourcefacts.

'''
#Global variables
istrue = ['True','TRUE','true','YES','Yes','yes','t','T','y','Y']
affirm = [ 'True', 'TRUE', 'T', 't', 'true', 'Yes', 'YES', 'Y', 'y']
oracle_home=""
err_msg = ""
msg = ""
debugme = False
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
host_debug_log = "/tmp/mod_debug.log"

def add_to_msg(mytext):
    """Passed some text add it to the msg"""
    global msg

    if not msg:
        msg = mytext
    else:
        msg = msg + " " + mytext


def debugg(debug_str):
    """If debugging is on add debugging string to global msg"""
    global debugme

    if debugme:
        add_to_msg(debug_str)
        write_to_file(debug_str)


def write_to_file(info_str):
    """write this string to debug log"""
    global host_debug_log

    f =  open(host_debug_log, 'a')
    for aline in info_str.split("\n"):
        f.write(aline + "\n")
    f.close()


def get_grid_home():
    """Determine the Grid Home directory
       using ps -eo args
       returns string."""
    global grid_home

    output = run_sub("/bin/ps -eo args | /bin/grep ocssd.bin | /bin/grep -v grep | /bin/awk '{print $1}'")
    grid_home = (output.strip()).replace('/bin/ocssd.bin', '')

    return(grid_home)


def get_node_num():
    """Return current node number to ensure that srvctl is only executed on one node (1)"""
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
    """Return database home as recorded in /etc/oratab"""

    output = run_sub("/bin/cat /etc/oratab | /bin/grep -m 1 %s | /bin/grep -o -P '(?<=:).*(?<=:)' |  /bin/sed 's/\:$//g'" % (vdb))

    ora_home = output.strip()

    debugg("get_dbhome(%s) output: %s returning: %s" % (vdb, output, ora_home))

    return(ora_home)


def israc():
    """Determine if a host is running RAC or Single Instance"""
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
    """Encapsulate error handling and run subprocess cmds here"""
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
    """Run a subprocess with environmental vars
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

    if not output:
        return("")
    else:
        return(output)


def run_cmd(cmd_str):
    """Encapsulate all error handline in one fx. Run cmds here."""
    global msg

    # get the pmon process id for the running database.
    # 10189  tstdb1
    try:
        vproc = str(commands.getstatusoutput(cmd_str)[1])
    except:
        add_to_msg('Error: run_cmd(%s) :: cmd_str=%s' % (sys.exc_info()[0],cmd_str))
        add_to_msg("Meta:: %s, %s, %s %s" % (sys.exc_info()[0], sys.exc_info()[1], msg, sys.exc_info()[2]))
        raise Exception (msg)

    if vproc:
        return(vproc)
    else:
        return("")


def get_orahome_procid(vdb):
    """Get database Oracle Home from the running process."""
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
    """Retrieve the ASM DB name"""
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
    """ Check the lsnrctl state using command line """
    global msg
    global err_msg
    global grid_home
    global istrue
    global debugme
    global affirm

    vasm_sid = "+ASM"
    voracle_user = "oracle"

    ansible_facts={}

    module = AnsibleModule(
      argument_spec = dict(
        db_name         = dict(required=True),
        asm_dg          = dict(required=True),
        debugging       = dict(required=False)
      ),
      supports_check_mode=False,
    )

    # Get arguements passed from Ansible playbook
    vdb          = module.params["db_name"]
    vasm_dg      = module.params["asm_dg"]
    vdebug       = module.params["debugging"]

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

    if visrac in istrue:
        if not vdb[-1].isdigit():
            voracle_sid = vdb + vnode_num
        else:
            if vdb[-1] != "1":
                voracle_sid = vdb[:-1] + vnode_num
            vdb = vdb[:-1]
        debugg("visrac is %s, instance name: %s" % (visrac, vdb))

    vdb_home = get_dbhome(vasm_sid)

    debugg("main: called get_dbhome(%s) returned: %s" %(vasm_sid,vdb_home))

    # Make sure an alias doesn't already exist
    debugg("[1] make sure alias doesnt already exist")
    output = run_sub_env("echo ls -l %s/%s/spfile%s.ora | %s/bin/asmcmd" % (vasm_dg.upper(),vdb,vdb,vdb_home), {'oracle_home': vdb_home, 'oracle_sid': vasm_sid })
    debugg("main: spfile output=%s" % (output))
    if not 'does not exist' in output and 'spfile' in output:
        debugg("[1a] spfile found in output %s" % (output))
        spfile = [ item for item in output.split() if "spfile" in item ]

        if len(spfile) > 1:
            debugg("[1b] spfile already exists %s" % (spfile))
            add_to_msg("spfile already exists: %s/%s/%s => %s" % (vasm_dg,vdb,spfile[0],spfile[1]))
            module.exit_json( msg=msg, ansible_facts={} , changed=False)

    # Else get the name of the parameterfile.
    output = run_sub_env("echo ls -l %s/%s/parameterfile/ | %s/bin/asmcmd" % (vasm_dg.upper(),vdb.upper(),vdb_home), {'oracle_home': vdb_home, 'oracle_sid': vasm_sid })
    debugg("output=%s" % (output))
    if output:
        spfile_orig = [ item for item in output.split() if "spfile" in item ][0]

        debugg("[2] get the parameterfile name: %s" % (spfile_orig))

    # Create the alias
    output = run_sub_env("echo mkalias %s/%s/parameterfile/%s %s/%s/spfile%s.ora | %s/bin/asmcmd" % (vasm_dg.upper(),vdb.upper(),spfile_orig,vasm_dg.upper(),vdb.upper(),vdb,vdb_home), {'oracle_home': vdb_home, 'oracle_sid': vasm_sid })

    debugg("[3] Alias created output %s" % (output))

    # Check the alias and return the results
    output = run_sub_env("echo ls -l %s/%s/spfile%s.ora | %s/bin/asmcmd" % (vasm_dg.upper(),vdb,vdb,vdb_home), {'oracle_home': vdb_home, 'oracle_sid': vasm_sid } )

    spfile = [ item for item in output.split() if "spfile" in item]

    debugg("[4] Check Alias")

    add_to_msg("Module mkalias exiting successfully. Created alias: %s/%s/%s => %s " % (vasm_dg,vdb,spfile,spfile))

    # print json.dumps( ansible_facts_dict )
    module.exit_json( msg=msg, ansible_facts={} , changed=True)

# code to execute if this program is called directly
if __name__ == "__main__":
    main()
