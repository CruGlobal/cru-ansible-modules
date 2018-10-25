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
import re
from subprocess import (PIPE, Popen)
from __builtin__ import any as exists_in  # exist_in(word in x for x in mylist)

#Global variables
oracle_home=""
err_msg = ""
msg = ""
DebugMe = True
sleep_time = 3
default_ttw = 2
default_expected_num_reg_lsnrs = 1
grid_home = ""
node_number = ""

ANSIBLE_METADATA = {'status': ['stableinterface'],
                    'supported_by': 'Cru DBA team',
                    'version': '0.1'}

DOCUMENTATION = '''
---
module: setcntrlfile (set control file)
short description: Given ASM diskgroup and database name it looks for the control file in ASM,
                   then opens the database in nomount state and sets the control file parameter.

'''

EXAMPLES = '''

    # when standing up a new database using restore, or clone etc.
    # this will look in ASM for new control files and then set the control_files parameter
    # in the database. i.e control_files = +DATA3/stgdb/controlfile/current.404.989162475
    - name: Map new alias to spfile
      setcntrlfile:
        db_name: "{{ dest_db_name }}"
        db_home: "{{ oracle_home }}"
        asm_dg: "{{ database_parameters[dest_db_name].asm_dg_name }}"
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
    """Get Oracle database Home from the running process."""
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
    global oracle_home
    global DebugMe
    debugme = "false"
    vasm_sid = "+ASM1"
    voracle_user = "oracle"
    vasm_fra = "+FRA"
    err_gettn_or_cuttn_data = 0
    err_gettn_or_cuttn_fra = 0

    ansible_facts={}

    module = AnsibleModule(
      argument_spec = dict(
        db_name         = dict(required=True),
        db_home         = dict(required=True),
        asm_dg          = dict(required=True)
      ),
      supports_check_mode=False,
    )

    # Get arguements passed from Ansible playbook
    vdb             = module.params["db_name"]
    voracle_home    = module.params["db_home"]
    vasm_dg         = module.params["asm_dg"]

    # if the first character of the asm_dg is not '+' add it.
    if vasm_dg[0] != "+":
        vasm_dg = "+%s" % (vasm_dg)

    # get the grid home
    vgrid_home = get_grid_home()

    # get the current node number
    vnode_num = get_node_num()

    # if the database name doesn't have an instance number add it
    # if it does have a node number make sure it's the correct one
    if not vdb[-1].isdigit():
        voracle_sid = vdb + vnode_num
    else:
        if vdb[-1] != int(vnode_num):
            voracle_sid = vdb[:-1] + vnode_num
        vdb = vdb[:-1]

    # Get control file from ASM (+DATA).
    try:
        os.environ['ORACLE_HOME'] = vgrid_home
        os.environ['ORACLE_SID'] = vasm_sid
        cmd_str = "echo ls %s/%s/controlfile | %s/bin/asmcmd" % (vasm_dg.upper(),vdb,vgrid_home)
        process = subprocess.Popen([cmd_str], stdout=PIPE, stderr=PIPE, shell=True)
        output, code = process.communicate()
    except:
        custom_err_msg_gettn_asm_data = 'Error [ getting control file name from %s ] grid_home: %s asm_sid: %s  asm_dg: %s cmd_str: %s' % (vasm_dg,vgrid_home,vasm_sid,vasm_dg,cmd_str)
        custom_err_msg_gettn_asm_data = custom_err_msg_gettn_asm_data + "%s, %s, %s" % (sys.exc_info()[0], sys.exc_info()[1], sys.exc_info()[2])
        err_gettn_or_cuttn_data = 1
        # raise Exception (custom_err_msg) - if one or the other control file is successful don't throw an error message

    if output:
        try:
            vcntr_file_data = [ item for item in output.split() if "Current" in item ][0]
        except:
            custom_err_msg_cuttn_asm_data = 'Error [ getting control file name from %s ] grid_home: %s asm_sid: %s  asm_dg: %s cmd_str: %s' % (vasm_dg,vgrid_home,vasm_sid,vasm_dg,cmd_str)
            custom_err_msg_cuttn_asm_data = custom_err_msg_cuttn_asm_data + "%s, %s, %s" % (sys.exc_info()[0], sys.exc_info()[1], sys.exc_info()[2])
            err_gettn_or_cuttn_data = 1

    if DebugMe:
        msg = "[1] This command: %s used to get control file name: %s from: %s " % (cmd_str,vcntr_file_data,vasm_dg)

    # Get the control file name from +FRA.
    try:
        os.environ['ORACLE_HOME'] = vgrid_home
        os.environ['ORACLE_SID'] = vasm_sid
        cmd_str = "echo ls %s/%s/controlfile | %s/bin/asmcmd" % (vasm_fra.upper(),vdb,vgrid_home)
        process = subprocess.Popen([cmd_str], stdout=PIPE, stderr=PIPE, shell=True)
        output, code = process.communicate()
    except:
        custom_err_msg_gettn_asm_fra = 'Error [ getting control file name fron %s ] grid_home: %s asm_sid: %s asm_dg: %s cmd_str: %s' % (vasm_fra,vgrid_home,vasm_sid,vasm_dg,cmd_str)
        custom_err_msg_gettn_asm_fra = custom_err_msg_gettn_asm_fra + "%s, %s, %s" % (sys.exc_info()[0], sys.exc_info()[1], sys.exc_info()[2])
        err_gettn_or_cuttn_fra = 1
        # raise Exception (custom_err_msg)

    try:
        vcntr_file_fra = [ item for item in output.split() if "Current" in item][0]
    except:
        custom_err_msg_cuttn_asm_fra = 'Error [ getting control file name fron %s ] grid_home: %s asm_sid: %s asm_dg: %s cmd_str: %s' % (vasm_fra,vgrid_home,vasm_sid,vasm_dg,cmd_str)
        custom_err_msg_cuttn_asm_fra = custom_err_msg_cuttn_asm_fra + "%s, %s, %s" % (sys.exc_info()[0], sys.exc_info()[1], sys.exc_info()[2])
        err_gettn_or_cuttn_fra = 1

    # If neither control file ( +DATA or +FRA ) exists raise error - as long as one does, continue
    if int(err_gettn_or_cuttn_fra) == 1 and int(err_gettn_or_cuttn_data) == 1:
        if int(err_gettn_or_cuttn_fra) == 1:
            if custom_err_msg_gettn_asm_fra:
                raise Exception(custom_err_msg_gettn_asm_fra)
            elif custom_err_msg_cuttn_asm_fra:
                raise Exception(custom_err_msg_cuttn_asm_fra)
        elif int(err_gettn_or_cuttn_data) == 1:
            if custom_err_msg_gettn_asm_fra:
                raise Exception(custom_err_msg_gettn_asm_data)
            elif custom_err_msg_cuttn_asm_fra:
                raise Exception(custom_err_msg_gettn_asm_data)

    if DebugMe:
        msg = msg + "[2] This command: %s used to get control file name: %s from: %s " % (cmd_str,vcntr_file_fra,vasm_fra)

    time.sleep(int(sleep_time))

    # Startup nomount the database
    try:
        os.environ['ORACLE_HOME'] = voracle_home
        os.environ['ORACLE_SID'] = voracle_sid
        cmd_str = "%s/bin/sqlplus / as sysdba" % (voracle_home)
        cmd_str1 = "startup nomount"
        process = subprocess.Popen(cmd_str, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
        output, code = process.communicate(cmd_str1)
    except:
        custom_err_msg = 'Error [ startup nomount of database %s ] oracle_home: %s db_sid: %s cmd_str: %s' % (vdb,voracle_home,voracle_sid,cmd_str)
        custom_err_msg = custom_err_msg + "%s, %s, %s" % (sys.exc_info()[0], sys.exc_info()[1], sys.exc_info()[2])
        raise Exception (custom_err_msg)

    if DebugMe:
        msg = msg + "[3] Startup nomount: %s" % (cmd_str)

    time.sleep(int(sleep_time))

    # create the set control_files command based on which controlfiles exist
    if int(err_gettn_or_cuttn_fra) == 0 and int(err_gettn_or_cuttn_data) == 0:
        cmd_str3 = "alter system set control_files='%s/%s/controlfile/%s','%s/%s/controlfile/%s' scope=spfile;\n" % (vasm_dg,vdb,vcntr_file_data,vasm_fra,vdb,vcntr_file_fra)
    elif int(err_gettn_or_cuttn_fra) == 0:
        cmd_str3 = "alter system set control_files='%s/%s/controlfile/%s' scope=spfile;\n" % (vasm_fra,vdb,vcntr_file_fra)
    elif int(err_gettn_or_cuttn_data) == 0:
        cmd_str3 = "alter system set control_files='%s/%s/controlfile/%s' scope=spfile;\n" % (vasm_dg,vdb,vcntr_file_data)

    # Run the alter system command and set the controlfile parameter
    try:
        cmd_str1 = '%s/bin/sqlplus / as sysdba' % (voracle_home)
        os.environ['ORACLE_HOME'] = voracle_home
        os.environ['ORACLE_SID'] = voracle_sid
        process = subprocess.Popen(cmd_str1, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
        output, code = process.communicate(cmd_str3)
    except:
        custom_err_msg = 'Error[ setting control files in database %s ] oracle_home: %s oracle_sid: %s asm_dg: %s asm_fra: %s database: %s command: %s' % (vdb,voracle_home,voracle_sid,vasm_dg,vasm_fra,vdb,cmd_str)
        custom_err_msg = custom_err_msg + "%s, %s, %s" % (sys.exc_info()[0], sys.exc_info()[1], sys.exc_info()[2])
        raise Exception (custom_err_msg)

    if DebugMe:
        msg = msg + "[3] Alter system set controlfile with this command: %s oracle_home: %s oracle_sid: %s output: %s" % (cmd_str,voracle_home,voracle_sid,output)

    time.sleep(int(sleep_time))

    # Shut the database back down
    try:
        os.environ['ORACLE_HOME'] = voracle_home
        os.environ['ORACLE_SID'] = voracle_sid
        cmd_str1 = '%s/bin/sqlplus / as sysdba' % (voracle_home)
        cmd_str2 = "shutdown immediate\n"
        process = subprocess.Popen([cmd_str1], stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
        output, code = process.communicate(cmd_str2)
    except:
        custom_err_msg = 'Error[ shutting down the database %s after setting control files ] oracle_home: %s oracle_sid: %s asm_dg: %s asm_fra: %s database: %s command: %s' % (vdb,voracle_home,voracle_sid,vasm_dg,vasm_fra,vdb,cmd_str)
        custom_err_msg = custom_err_msg + "%s, %s, %s" % (sys.exc_info()[0], sys.exc_info()[1], sys.exc_info()[2])
        raise Exception (custom_err_msg)

    if DebugMe:
        msg = msg + "[4] Alter system set controlfile with this command: %s and output: %s " % (cmd_str,output)
        msg = msg + "Module setcntrlfile exiting successfully. Controlfile set. %s: %s , %s: %s. Database shutdown; exit code: %s" % (vasm_dg,vcntr_file_data,vasm_fra,vcntr_file_fra,code)
    else:
        msg = "Module setcntrlfile exiting successfully. Controlfile set. %s: %s , %s: %s. Database shutdown; exit code: %s" % (vasm_dg,vcntr_file_data,vasm_fra,vcntr_file_fra,code)

    # print json.dumps( ansible_facts_dict )
    module.exit_json( msg=msg, ansible_facts={} , changed=True)

# code to execute if this program is called directly
if __name__ == "__main__":
    main()
