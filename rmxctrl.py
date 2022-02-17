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
from subprocess import (PIPE, Popen)


ANSIBLE_METADATA = {'status': ['stableinterface'],
                    'supported_by': 'Cru DBA team',
                    'version': '0.1'}

DOCUMENTATION = '''
---
module: rmxctrl
short_description: Remove extra controlfiles.
quick notes:
    - This module runs on the remote host. Debug logs etc will be there. see debug_log for location.
    - This module runs in conjuntion with sysdbafacts to get existing control files.

description:
    After active duplication has completed and the database is sent to racit
    more than one control file exists in ASM and FRA diskgroup on ASM
    only one in each diskgroup (+DATA, +FRA) of which is used in the database.
    This module will determine which control files are being used and delete
    the extra, non-used controlfiles.

    Example of existing controlfiles after active duplication:
      SQL> show parameter control
              control_files  string	 +DATA3/TSTDB/CONTROLFILE/current.333.1095774051, +FRA/TSTDB/CONTROLFILE/current.26448.1095774051
      ASMCMD> ls -l +DATA3/TSTDB/CONTROLFILE
            Type         Redund  Striped  Time             Sys  Name
            CONTROLFILE  UNPROT  FINE     FEB 04 13:00:00  Y    Current.333.1095774051  <<= only this one is being used in +DATA3/tstdb/controlfile
            CONTROLFILE  UNPROT  FINE     FEB 04 13:00:00  Y    Current.352.1095774051
      ASMCMD> ls -l +FRA/tstdb/CONTROLFILE
            Type         Redund  Striped  Time             Sys  Name
            CONTROLFILE  UNPROT  FINE     FEB 04 13:00:00  Y    Current.26448.1095774051 <<= only this one is being used in +FRA/tstdb/controlfile
            CONTROLFILE  UNPROT  FINE     FEB 04 13:00:00  Y    Current.26509.1095774051
'''

EXAMPLES = '''
    # when standing up a new database using restore, or clone etc.
    # this will look in asm for a new spfile and create an alias to it.
    - name: Remove extra controlfiles
      rmxctrl:
        db_name: "{{ dest_db_name }}"
        asm_dg: "{{ cntlfilefacts['diskgroups'] }}"
        existing_controlfiles: "{{ cntlfilefacts['control_files'] }}"
        is_rac: "{{ is_rac }}"
        refname: rmxctl
        host: "{{ dest_host }}"
      when: master_node

    Notes:
        asm_dg - only +DATA name needs specified. +FRA is assumed.
        refname - name you would like to reference the return values by if needed.
        existing_controlfiles - can be obtained by running sysdba facts
        against the new database once duplication is complete and then
        passing the results : cntlfilefacts['control_files']['data']
        to rmxctrl

          - name: Get CONTROL_FILES using sysdba facts
            local_action:
              module: sysdbafacts
              syspwd: "{{ database_passwords[source_db_name].sys }}"
              db_name: "{{ dest_db_name }}" # use source db password, because the dest db passwords haven't been set yet
              host: "{{ inventory_hostname }}"
              is_rac: "{{ orafacts['is_rac'] }}"
              oracle_home: "{{ oracle_home }}"
              refname: cntlfilefacts
              ignore: False
              debugging: False
            # no_log: false
            when:
              - master_node|bool
              # - sourcefacts is not defined
            become: no
            register: sysdba_facts
            tags: [racit,set_cntrl_file]

        sysdbafacts return values look like this: ( showing controlfiles only.)
        many other values are returned.

            cntlfilefacts:  <= call sysdbafacts with refname cntlfilefacts
              control_files:
                data: +DATA3/TSTDB/CONTROLFILE/current.333.1095774051
                fra: +FRA/TSTDB/CONTROLFILE/current.26448.1095774051

'''

#Global variables
# This is global in that it calls library/pymods/crumods.py msg variable
affirm = [ 'True', 'TRUE', True, 'T', 't', 'true', 'Yes', 'YES', 'Y', 'y']
oracle_home=""
err_msg = ""
msg = ""
debugme = True
grid_home = ""
node_number = ""
default_refname = "rmxctrl"
# number of registered listeners: currently 2 ( UNKNOWN and BLOCKED )
# [oracle@tlorad01]:tstdb1:/u01/oracle/ansible_stage/utils/tstdb/dup/2018-08-12> lsnrctl status | grep tstdb
# Service "tstdb.ccci.org" has 2 instance(s).
#   Instance "tstdb1", status UNKNOWN, has 1 handler(s) for this service...
#   Instance "tstdb1", status BLOCKED, has 1 handler(s) for this service...
debug_log = os.path.expanduser("~/.debug.log")

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
    global affirm

    if debugme in affirm:
        add_to_msg(debug_str)
        write_to_file(debug_str)


def write_to_file(info_str):
    """
    write this string to debug log
    """
    global debug_log

    f =  open(debug_log, 'a')
    for aline in info_str.split("\n"):
        f.write(aline + "\n")
    f.close()


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
    debugg("run_sub()...starting with cmd_str={}".format(cmd_str))

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
     Run a subprocess with
     environmental vars (env)
     passed in as dictionary: {'ORACLE_HOME': value, ORACLE_SID: value }
    """
    global msg

    debugg("run_sub_env()...starting with cmd_str={} env={}".format(cmd_str, str(env)))

    try:
        # passed in python dictionary is 'env'
        os.environ['ORACLE_HOME'] = env['ORACLE_HOME']
        os.environ['ORACLE_SID'] = env['ORACLE_SID']
        debugg("Running cmd_str=%s with ORACLE_HOME: %s and ORACLE_SID: %s" % (cmd_str, env['ORACLE_HOME'] or "WRONG KEY?", env['ORACLE_SID'] or "WRONG KEY?"))
        process = subprocess.Popen([cmd_str], stdout=PIPE, stderr=PIPE, shell=True, env=dict(env))
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


def get_asm_db():
    """
    Retrieve the ASM DB name
    """
    cmd_str = "/bin/ps -ef | grep _pmon_ | grep -v grep | grep '+' | awk '{print $8}' | cut -d'_' -f 3"
    output = run_cmd(cmd_str)
    # tmp = output.split()
    # tmp = [ i for i in tmp if '+' in i ]
    # tmp = tmp[0].split('_')
    # tmp = tmp[2]

    debugg("get_asm_db() returning %s" % (output) )
    return(output)


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


def get_dbhome(vdb):
    """
    Return database home as recorded in /etc/oratab
    """

    output = run_sub("/bin/cat /etc/oratab | /bin/grep -m 1 %s | /bin/grep -o -P '(?<=:).*(?<=:)' |  /bin/sed 's/\:$//g'" % (vdb))

    ora_home = output.strip()

    debugg("get_dbhome(%s) output: %s returning: %s" % (vdb, output, ora_home))

    return(ora_home)


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

    return(output.strip())


def get_asm_controlfiles(db, asm_dg, asm_info, existing):
    """

    Given the DATA ASM diskgroup : +DATA3 return a list of all controlfiles
    in both +DATA3 and +FRA
    for the specified database.
    asm_info = { 'db': +ASM1, 'home': /app/19.0.0/grid } - needed to get controlfiles

    Use this to filter out existing control files from those returned.
    existing = {'db': 'tstdb', 'controlfiles': {'FRA': 'current.26448.1095774051', 'DATA': 'current.333.1095774051'}}

    def run_sub_env(cmd_str, env=None):
    where asm_info = { ORACLE_SID: value, 'ORACLE_HOME': value }

    """
    debugg("get_asm_controlfiles()...starting with db={} asm_dg={} asm_info={}".format(db or "EMPTY!", asm_dg or "EMPTY!", str(asm_info) or "EMPTY"))
    asm_dgs = ['+FRA', asm_dg]
    # env = { 'oracle_sid' : asm_info['db'], 'oracle_home' : asm_info['home'] }
    to_del = []
    del_dict = {}

    debugg("get_asm_controlfiles()...existing={}".format(str(existing)))
    for dg in asm_dgs:
        cntrl_to_filter = existing['controlfiles'][dg]
        # try using the center part of existing controlfile Current.333.1095774051 => filter 333
        f1 = cntrl_to_filter.split(".")[1]

        # filter out control file being used and get a list of extra unneeded control files to delete.                                                                                                                           existing = {'db': 'tstdb', 'controlfiles': {'FRA': 'current.26448.1095774051', 'DATA': 'current.333.1095774051'}}
        cmd_str = "echo ls -l {dg}/{db}/controlfile | {gi}/bin/asmcmd | grep -v ASMCMD | awk '{{ print $8 }}' | grep -v mail | grep -v '{f1}' ".format(dg=dg,db=db,gi=asm_info['ORACLE_HOME'], f1=f1)
        debugg("FOR LOOP processing dg={} cmd_str={}".format(dg, cmd_str))
        # Current.333.1095774051
        # Current.352.1095774051
        output = run_sub_env(cmd_str,asm_info)
        debugg("get_asm_controlfiles()...run_sub_env() output = {}".format(output))
        for item in output.split():
            if item not in existing:
                to_del.append(item)


        del_dict.update({ dg: to_del })
        to_del = []

    debugg("get_asm_controlfiles()...returning del_dict={}".format(str(del_dict)))
    return(del_dict)


def del_xtra_controlfiles(cfd, db, asm_info):
    """
    cfd - python dictionary containing control file info, a dictionary containing the dg and list of controlfiles to delete.
          cfd={'+FRA': ['Current.26509.1095774051'], '+DATA3': ['Current.352.1095774051']}
    db - is the original database i.e. tstdb
    asm_info = { 'ORACLE_SID': +ASM1, 'ORACLE_HOME': '/app/19.0.0/grid' }
    del_dict={'+FRA': ['Current.26509.1095774051'], '+DATA3': ['Current.352.1095774051']}
    given this delete all controlfiles in the list. These are extras
    """
    debugg("del_xtra_controlfiles()...starting with cfd={} db={} asm_info={}".format(str(cfd) or "Empty!", db, str(asm_info)))
    results = {}
    if not cfd:
        debugg("cfd is empty...no extra control files to delete....returning....")
        return()

    for dg in list(cfd.keys()):
        debugg("FOR LOOP processing dg {}".format(dg))
        # for each controlfile in the key's list cfd['+DATA3'] = ['Current.352.1095774051']
        for cf in cfd[dg]:
            cmd_str = "echo y | echo rm {dg}/{db}/controlfile/{cf} | {oh}/bin/asmcmd".format(dg=dg, db=db, cf=cf, oh=asm_info['ORACLE_HOME'])
            output = run_sub_env(cmd_str, asm_info)

            if output:
                reply = output
            else:
                reply = "Deleted"

            if results.get(dg, None):
                results[dg].update( { cf : reply } )
            else:
                results.update( { dg : { cf : reply } } )

    return(results)

# ==============================================================================
# =================================== MAIN =====================================
# ==============================================================================
def main ():
    """
    This module will delete any old controlfiles in the +ASM diskgroup for the given db
    from previous runs if they exist.

    This module works in conjuntion with a task which passes a registered variable
    as input to the existing_controlfiles parameter of this module.

    See example above in EXAMPLES
    """
    global msg
    global err_msg
    global debugme
    global affirm
    global default_refname
    asm_db = ""
    visrac = ""
    vasm_sid = "+ASM"
    voracle_user = "oracle"

    ansible_facts={}

    module = AnsibleModule(
      argument_spec = dict(
        db_name               = dict(required=True),
        asm_dg                = dict(required=True),
        existing_controlfiles = dict(required=False),
        is_rac                = dict(required=False),
        host                  = dict(required=True),
        refname               = dict(required=False),
        debugging             = dict(required=False)
      ),
      supports_check_mode=False,
    )

    # Get arguements passed from Ansible playbook
    vdb                    = module.params["db_name"]
    vasm_dg                = module.params["asm_dg"]
    vexisting_controlfiles = module.params["existing_controlfiles"]
    visrac                 = module.params["is_rac"]
    vhost                  = module.params["host"]
    vrefname               = module.params["refname"]
    vdebug                 = module.params["debugging"]

    if not vrefname:
        vrefname = default_refname

    ansible_facts={ vrefname: {} }

    # if asm dg passed in without '+' add it.
    if vasm_dg[0] != "+":
        vasm_dg = "+%s" % (vasm_dg.upper())
    else:
        vasm_dg = vasm_dg[0].upper()
    debugg("{} resides in vasm_dg = {}".format(vdb, str(vasm_dg)))

    if vdebug in affirm:
        debugme = True

    asm_db = get_asm_db()

    if visrac is None:
        visrac = israc()

    vasm_home = get_dbhome(asm_db)

    asm_info = { 'ORACLE_SID': asm_db, 'ORACLE_HOME': vasm_home }
    debugg("asm_info => {}".format(str(asm_info)))

    # convert string vexisting_controlfiles to dictionary
    # vexisting_controlfiles = 'fra': '+FRA/TSTDB/CONTROLFILE/current.26448.1095774051', 'data': '+DATA3/TSTDB/CONTROLFILE/current.333.1095774051'}
    vexisting_controlfiles = ast.literal_eval(vexisting_controlfiles)
    debugg("existing_controlfiles passed in => {}".format(str(vexisting_controlfiles)))
    # reformat existing controlfiles
    vexisting_controlfiles = { 'db': vdb , 'controlfiles': { vasm_dg : vexisting_controlfiles['data'], '+FRA': vexisting_controlfiles['fra'] } }
    debugg("main :: reformatting vexisting_controlfiles = {}".format(str(vexisting_controlfiles)))

    # vexisting_controlfiles = {'controlfiles': {'+FRA': ' +FRA/TSTDB/CONTROLFILE/current.7017.1096628163', '+DATA3': '+DATA3/TSTDB/CONTROLFILE/current.291.1096628163'}, 'db': 'tstdb'}
    for k in list(vexisting_controlfiles['controlfiles'].keys()):
        debugg("PROCESSING k={} ITEM={}".format(k, vexisting_controlfiles['controlfiles'][k]))
        # PROCESSING ITEM = +FRA/TSTDB/CONTROLFILE/current.26448.1095774051
        n = vexisting_controlfiles['controlfiles'][k].split("/")[len(vexisting_controlfiles['controlfiles'][k].split("/"))-1]
        # create a list of existing controlfile names.
        # shave the whole path to just the controlfile name here: '+FRA/TSTDB/CONTROLFILE/current.26448.1095774051' => current.26448.1095774051
        vexisting_controlfiles['controlfiles'][k] = n

    debugg("New vexisting_controlfiles={}".format(str(vexisting_controlfiles)))

    # Whatever controlfiles are not being used by the db we'll delete.

    # get a dictionary of all controlfiles vexisting_controlfiles = {'controlfiles': {'+FRA': '+FRA/TSTDB/CONTROLFILE/current.26448.1095774051', '+DATA3': '+DATA3/TSTDB/CONTROLFILE/current.333.1095774051'}, 'db': 'tstdb'}
    # asm_info = {'ORACLE_SID': '+ASM1', 'ORACLE_HOME': '/app/19.0.0/grid'}
    xtra_controlfiles = get_asm_controlfiles(vdb, vasm_dg, asm_info, vexisting_controlfiles)

    results = del_xtra_controlfiles(xtra_controlfiles, vdb, asm_info)

    # print json.dumps( ansible_facts_dict )
    module.exit_json( msg=msg, ansible_facts=results , changed=True)

# code to execute if this program is called directly
if __name__ == "__main__":
    main()
