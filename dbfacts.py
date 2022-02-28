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
module: dbfacts
short_description: Get Oracle Database facts from a remote database.
                   (remote database = a database not in the group being operated on)

notes:
    Returned values are then available to use in Ansible.
    This module will run on your local host.

requirements: [ python2.* ]
author: "DBA Oracle module Team"
'''

EXAMPLES = '''

    # if cloning a database and source database information is desired
    - local_action:
        module: dbfacts
        systempwd: "{{ database_passwords[source_db_name].system }}"
        db_name: "{{ source_db_name }}"
        host: "{{ source_host }}"
        refname: "{{ refname_str }} (1)"
        ignore: True (2)
        debugging: False
      become_user: "{{ remote_user }}"
      register: src_facts

      (1) refname - name used in Ansible to reference these facts ( i.e. sourcefacts, destfacts )

      (2) ignore - (connection errors) is optional. If you know the source
          database may be down set ignore: True. If connection to the
          source database fails the module will not throw a fatal error
          and continue.

   NOTE: these modules can be run with the when: master_node statement.
         However, their returned values cannot be referenced in
         roles or tasks later. Therefore, when running fact collecting modules,
         run them on both nodes. Do not use the "when: master_node" clause.

'''

# Add anything to this list from v$parameter table to retrieve for use in ansible_facts.
vparams=[ "cluster_database",
          "compatible",
          "sga_target",
          "pga_aggregate_target",
          "sga_max_size",
          "db_recovery_file_dest",
          "db_recovery_file_dest_size",
          "diagnostic_dest",
          "remote_listener",
          "db_unique_name",
          "db_block_size",
          "remote_login_passwordfile",
          "spfile",
          "user_dump_dest",
          "core_dump_dest",
          "background_dump_dest",
          "audit_file_dest",
          "db_files",
          "standby_file_management",
          "log_archive_config",
          "log_archive_dest_2",
          "dg_broker_start",
          "dg_broker_config_file1",
          "dg_broker_config_file2",
          "standby_file_management",
          "fal_server",
          "db_domain"
        ]

msg = ""
debugme = True
defrefname = "dbfacts"
affirm = ['True','TRUE', True,'true','T','t','Yes','YES','yes','y','Y']
db_home_name = "dbhome_1"
debug_log = os.path.expanduser("~/.debug.log")
utils_settings_file = os.path.expanduser("~/.utils")
debug_log2 = os.path.expanduser("~/.debug.log")
lh_domain = ".ccci.org"
dr_domain = ".dr.cru.org"

def set_debug_log():
    """ Set the debug_log value to write debugging messages to """
    global utils_settings_file
    global debug_log
    global debugme

    if not debugme or debug_log:
        return

    try:
        with open(utils_settings_file, 'r') as f1:
            line = f1.readline()
            while line:
                if 'ans_dir' in line:
                    tmp = line.strip().split("=")[1]
                    debug_log = tmp + "/bin/.utils/debug.log"
                    return

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

    if not debugme:
        return

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
    """Determine if a host is running RAC or Single Instance"""
    global err_msg
    if host_str is None:
        exit

    if "org" in host_str:
        host_str = host_str.replace(".ccci.org","")

    # if the last digits is 1 or 2 ( something less than 10) and not 0 (60) return True
    if host_str[-1:].isdigit() and int(host_str[-1:]) < 10 and int(host_str[-1:]) != 0:
        return(True)
    else:
        return(False)
    # Determine if a host is Oracle RAC ( return 1 ) or Single Instance ( return 0 )
    # vproc = run_cmd("ps -ef | grep lck | grep -v grep | wc -l")
    #
    # if int(vproc) > 0:
    #     # if > 0 "lck" processes running, it's RAC
    #     debugg("israc() returning True")
    #     return(True)
    # else:
    #     debugg("israc() returning False")
    return(False)


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


# ==============================================================================
# =================================== MAIN =====================================
# ==============================================================================

def main ():
    """ Return Oracle database parameters from a database not in the specified group"""
    global msg
    global defrefname
    global debugme
    global db_home_name
    global dr_domain
    global lh_domain
    ansible_facts={}
    is_rac = None
    global affirm
    ignore_err_flag = False

    # Name to call facts dictionary being passed back to Ansible
    # This will be the name you reference in Ansible. i.e. source_facts['sga_target'] (source_facts)
    refname = ""

    module = AnsibleModule(
      argument_spec = dict(
        systempwd       =dict(required=True),
        db_name         =dict(required=True),
        host            =dict(required=True),
        refname         =dict(required=False),
        ignore          =dict(required=False),
        debugging       =dict(required=False)
      ),
      supports_check_mode=True,
    )

    # Get arguements passed from Ansible playbook
    vdbpass    = module.params.get('systempwd')
    vdb        = module.params.get('db_name')
    vdbhost    = module.params.get('host')
    vrefname   = module.params.get('refname')
    vignore    = module.params.get('ignore')
    vdebug     = module.params.get('debugging')

    if vdebug in affirm:
      debugme = True
      set_debug_log()
    else:
      debugme = False

    if vignore is None:
      vignore = False

    if '.org' in vdbhost:
        vdbhost = vdbhost.replace(lh_domain,"").replace(dr_domain, "")

    visrac = israc(vdbhost)

    if not cx_Oracle_found:
        ansible_facts[refname].update( { "success" : "false" } )
        # module.exit_json(msg=msg, ansible_facts=ansible_facts, changed="False")
        module.fail_json(msg="Error: cx_Oracle module not found", ansible_facts=ansible_facts, changed="False")

    # set the Anisble variable reference name
    if not vrefname:
        refname = defrefname
    else:
        refname = vrefname

    # check required vars passed in are not NULL.
    if ( vdbpass is not None) and (vdb is not None) and (vdbhost is not None):

        try:

            if '.org' in vdbhost:
                vdbhost = vdbhost.replace(lh_domain,"").replace(dr_domain, "")

            if visrac:
                vdb = vdb + vdbhost[-1:]

            if '.org' not in vdbhost:
                if "dr" in vdbhost:
                    vdbhost = vdbhost + dr_domain
                else:
                    vdbhost = vdbhost + lh_domain

            dsn_tns = cx_Oracle.makedsn(vdbhost, '1521', vdb)
        except cx_Oracle.DatabaseError as exc:
            # try special case where single instance on rac:
            error, = exc.args
            if vignore:
                add_to_msg("Failed to create dns_tns: %s" %s (error.message))
            else:
                ansible_facts[refname].update( { "success" : "false" } )
                # module.exit_json(msg=msg, ansible_facts=ansible_facts, changed="False")
                module.fail_json(msg='TNS generation error: %s, db name: %s host: %s' % (error.message, vdb, vdbhost), changed=False)

        debugg("DEBUG[01] :: dsn_tns=%s system password=%s" % (dsn_tns,vdbpass))
        ansible_facts = { refname : { } }
        debugg("attempting to connect as system/{}@{}".format(vdbpass or "EMPTY!", str(dsn_tns)))
        try:
            con = cx_Oracle.connect('system', vdbpass, dsn_tns)
        except cx_Oracle.OperationalError as exc:
        # except (cx_Oracle.OperationalError, cx_Oracle.DatabaseError) as exc:
            error, = exc.args
            debugg("EXCEPT TRYING TO MAKE CONNECTION\nvdbpass={} dsn_tns={}!".format(vdbpass or "EMPTY!", str(dsn_tns)))
            if "dr" in vdbhost:
                msg = "DR database {} cannot be queried in standby mode.\nExact error:\n\t{}".format(vdb, error.message)
                debugg(msg)
                ansible_facts[refname].update( { "success" : "false" } )
                module.exit_json(msg=msg, ansible_facts=ansible_facts, changed="False")
            else:
                if vdb[-1:].isdigit():
                    vdb = vdb[:-1]
                    debugg(">>> vdb={} attempting to create connection without digit".format(vdb))
                    try:
                        dsn_tns = cx_Oracle.makedsn(vdbhost, '1521', vdb)
                        debugg(dsn_tns)
                        con = cx_Oracle.connect('system', vdbpass, dsn_tns)
                    except:
                        error, = exc.args
                        if vignore:
                          add_to_msg("DB CONNECTION FAILED : %s" % (error.message))
                          debugg(" vignore: %s " % (vignore))
                          ansible_facts[refname].update( { "success" : "false" } )
                          module.exit_json(msg=msg, ansible_facts=ansible_facts, changed="False")
                        else:
                          module.fail_json(msg='Database connection error: %s, tnsname: %s host: %s' % (error.message, vdb, vdbhost), changed=False)
        debugg("Connection good!")
        cur = con.cursor()

        # get parameters listed in the header of this program defined in "vparams"
        for idx in range(len(vparams)):
            try:
              v_sel = "select value from v$parameter where name = '" + vparams[idx] + "'"
              cur.execute(v_sel)
            except cx_Oracle.DatabaseError as exc:
              error, = exc.args
              if not vignore:
                  ansible_facts[refname].update( { "success" : "false" } )
                  module.fail_json(msg='Error selecting name from v$asmdiskgroup, Error: %s' % (error.message), changed=False)
              else:
                  add_to_msg("Error while attempting to retrieve parameter %s, %s continuing..." % (vparams[idx],error.message))
                  # if an error just occurred on the select and ignore errors is True skip this and go on
                  continue

            vtemp = cur.fetchall()
            vtemp = vtemp[0][0]

            debugg("param=%s value=%s" % (vparams[idx], vtemp))

            # module.fail_json(msg='processing: vparams[idx]= %s and vtemp = %s' % (vparams[idx], vtemp), changed=False)
            if 'sga_target' == vparams[idx] or 'db_recovery_file_dest_size' == vparams[idx]:
                vtemp = convert_size(float(vtemp),"M")
                ansible_facts[refname].update({ vparams[idx]: vtemp })
            elif 'db_domain' == vparams[idx]:
                ansible_facts[refname].update({ 'domain': "." + vtemp })
            elif 'db_recovery_file_dest' == vparams[idx]:
                ansible_facts[refname].update({ vparams[idx]: vtemp })
            elif 'listener' in vparams[idx]:
                if vtemp is None:
                    ansible_facts[refname].update({ vparams[idx]: 'None' })
                else:
                    head, sep, tail = vtemp.partition('.')
                    ansible_facts[refname].update({vparams[idx]: head})
            elif 'cluster_database' == vparams[idx]:
                debugg(">>>> In PARAM FOR LOOP : %s = %s" % (vparams[idx], vtemp))
                if vtemp.upper() == 'TRUE':
                    is_rac = True
                    ansible_facts[refname].update({'cluster_database': 'True' })
                    ansible_facts[refname].update({'is_rac': 'True' })
                else:
                    is_rac = False
                    ansible_facts[refname].update({ 'cluster_database': 'False' })
                    ansible_facts[refname].update({ 'is_rac': 'False' })
                debugg( ">>>> In PARAM FOR LOOP : is_rac = %s" % (is_rac) )
            else:
                try:
                    ansible_facts[refname][vparams[idx]] = vtemp
                except:
                    debugg("Error while adding %s %s" % (str(vparams[idx]), str(vtemp)))

        debugg("#### PARAMS ADDED ansible_facts[%s]= %s " % (refname, ansible_facts[refname]))

        # select source db version
        try:
            cur.execute('select version from v$instance')
        except cx_Oracle.DatabaseError as exc:
            error, = exc.args
            if not vignore:
                module.fail_json(msg='Error selecting version from v$instance, Error: %s' % (error.message), changed=False)
            else:
                ignore_err_flag = True
                add_to_msg("Error selecting version: %s" % (error.message))

        # if an error just occurred on the select and ignore errors is True skip this and go on
        if not ignore_err_flag:
            dbver =  cur.fetchall()
            retver = dbver[0][0]
            usable_ver = ".".join(retver.split('.')[0:-1])
            ansible_facts[refname].update({'version': usable_ver, 'oracle_version_full': retver, 'major_version': usable_ver.split(".")[0]})
        ignore_err_flag = False

        # select instance name
        try:
            cur.execute('select instance_name from v$instance')
        except cx_Oracle.DatabaseError as exc:
            error, = exc.args
            if not vignore:
                module.fail_json(msg='Error selecting instance_name from v$instance, Error: %s' % (error.message), changed=False)
            else:
                ignore_err_flag = True
                add_to_msg("Error selecting instance_name: %s" % (error.message))

        # if an error just occurred on the select and ignore errors is True skip this and go on
        if not ignore_err_flag:
            vinst =  cur.fetchall()
            vinst = vinst[0][0].strip()
            ansible_facts[refname].update( { 'sid':vinst } )
        ignore_err_flag = False

        # select host_name
        try:
            cur.execute('select host_name from v$instance')
        except cx_Oracle.DatabaseError as exc:
            error, = exc.args
            if not vignore:
                module.fail_json(msg='Error selecting host_name from v$instance, Error: %s' % (error.message), changed=False)
            else:
                ignore_err_flag = True
                add_to_msg("Error selecting hostname: %s " % (error.message))
        # if an error just occurred on the select and ignore errors is True skip this and go on
        if not ignore_err_flag:
            vtemp = cur.fetchall()
            vtemp = vtemp[0][0]
            ansible_facts[refname]['host_name'] = vtemp
        ignore_err_flag = False

        # actual db_file count
        try:
          cur.execute('select count(*) from dba_data_files')
        except cx_Oracle.DatabaseError as exc:
          error, = exc.args
          if vignore:
              ignore_err_flag = True
              add_to_msg("Error selecting dba_data_files count: %s " % (error.message))
          else:
              module.fail_json(msg='Error selecting host_name from v$instance, Error: %s' % (error.message), changed=False)
        # if an error just occurred on the select and ignore errors is True skip this and go on
        if not ignore_err_flag:
            vtemp = cur.fetchall()
            vtemp = vtemp[0][0]
            ansible_facts[refname].update( { 'db_files_actual': vtemp } )
        ignore_err_flag = False

        # Find archivelog mode.
        try:
          cur.execute('select log_mode from v$database')
        except cx_Oracle.DatabaseError as exc:
          error, = exc.args
          if vignore:
              ignore_err_flag = True
              add_to_msg("Error selecting dba_data_files count: %s " % (error.message))
          else:
              module.fail_json(msg='Error selecting log_mode from v$database, Error: %s' % (error.message), changed=False)

        if not ignore_err_flag:
            vtemp = cur.fetchall()
            vtemp = vtemp[0][0]
            if vtemp == 'ARCHIVELOG':
              vtemp = 'True'
            else:
              vtemp = 'False'
            ansible_facts[refname].update( { 'archivelog' : vtemp } )
        ignore_err_flag = False

       # Check dataguard enabled
       # select count(*) from v$archive_dest where status = 'VALID' and target = 'STANDBY';
       # if the result > 0 dataguard is enabled
        try:
          cur.execute('select count(*) from v$archive_dest where status = \'VALID\' and target = \'STANDBY\'')
        except cx_Oracle.DatabaseError as exc:
          error, = exc.args
          if vignore:
              ignore_err_flag = True
              add_to_msg("Error determining if dataguard is enabled, Error: %s" % (error.message))
          else:
              module.fail_json(msg='Error determining if dataguard is enabled: %s' % (error.message), changed=False)

        if not ignore_err_flag:
            vtemp = cur.fetchall()
            vtemp = vtemp[0][0]
            if int(vtemp) > 0:
              vtemp = 'True'
            else:
              vtemp = 'False'
            ansible_facts[refname].update( { 'dg_enabled' : vtemp } )
        ignore_err_flag = False

       # Check if flashback is on.
        try:
          cur.execute('select flashback_on from v$database')
        except cx_Oracle.DatabaseError as exc:
          error, = exc.args
          if vignore:
              ignore_err_flag = True
              add_to_msg("Error selecting flashback_on from v$database, Error: %s" % (error.message))
          else:
              module.fail_json(msg='Error selecting flashback_on from v$database, Error: %s' % (error.message), changed=False)

        if not ignore_err_flag:
            vtemp = cur.fetchall()
            vtemp = vtemp[0][0]
            if vtemp == 'YES':
              vtemp = 'True'
            else:
              vtemp = 'False'
            ansible_facts[refname].update( { 'flashback_on' : vtemp } )
        ignore_err_flag = False

       # Check for force logging.
        try:
          cur.execute('select force_logging from v$database')
        except cx_Oracle.DatabaseError as exc:
          error, = exc.args
          if vignore:
              ignore_err_flag = True
              add_to_msg("Error selecting force_logging from v$database, Error: %s" % (error.message))
          else:
              module.fail_json(msg='Error selecting force_logging from v$database, Error: %s' % (error.message), changed=False)

        if not ignore_err_flag:
            vtemp = cur.fetchall()
            vtemp = vtemp[0][0]
            if vtemp == 'YES':
              vtemp = 'True'
            else:
              vtemp = 'False'
            ansible_facts[refname].update( { 'force_logging' : vtemp } )
        ignore_err_flag = False

        # count online redo log groups
        try:
          cur.execute('select count(distinct group#) from v$logfile where type=\'ONLINE\'')
        except cx_Oracle.DatabaseError as exc:
          error, = exc.args
          if vignore:
              ignore_err_flag = True
              add_to_msg("Error selecting v$logfile count: %s " % (error.message))
          else:
              module.fail_json(msg='Error selecting count from v$logfile, Error: %s' % (error.message), changed=False)
        # if an error just occurred on the select and ignore errors is True skip this and go on
        if not ignore_err_flag:
            vtemp = cur.fetchall()
            vtemp = vtemp[0][0]
            ansible_facts[refname].update( { 'online_redo_log_group_count': vtemp } )
        ignore_err_flag = False

        # count standby redo logs
        try:
          cur.execute('select count(*) from v$standby_log')
        except cx_Oracle.DatabaseError as exc:
          error, = exc.args
          if vignore:
              ignore_err_flag = True
              add_to_msg("Error selecting v$standby_log count: %s " % (error.message))
          else:
              module.fail_json(msg='Error selecting count from v$standby_log, Error: %s' % (error.message), changed=False)
        # if an error just occurred on the select and ignore errors is True skip this and go on
        if not ignore_err_flag:
            vtemp = cur.fetchall()
            vtemp = vtemp[0][0]
            ansible_facts[refname].update( { 'standby_redo_log_group_count': vtemp } )
        ignore_err_flag = False

        # Determine if SIEBEL or PS Database
        try:
          cur.execute("select username from dba_users where username in ('SYSADM','FINADM','SIEBEL')")
          debugg("cmd_str = {}".format("select username from dba_users where username in ('SYSADM','FINADM','SIEBEL')"))
        except cx_Oracle.DatabaseError as exc:
          error, = exc.args
          if vignore:
              ignore_err_flag = True
              add_to_msg("Error selecting dba_data_files count: %s " % (error.message))
          else:
              module.fail_json(msg='Error selecting log_mode from v$database, Error: %s' % (error.message), changed=False)

        special_case = ""
        if not ignore_err_flag:
            vtemp = cur.fetchall()
            debugg("determine if ps or seibel....vtemp={}".format(str(vtemp)))
            if not vtemp:
                ansible_facts[refname].update( { 'siebel': 'False','ps_hr': 'False', 'ps_fin' : 'False', 'ps': 'False', 'ps_owner': 'None' } )
            else:
                vtemp = vtemp[0][0]
                if vtemp == 'SIEBEL':
                    ansible_facts[refname].update( { 'siebel': 'True','ps_hr': 'False', 'ps_fin' : 'False', 'ps': 'False', 'ps_owner': 'None' } )
                elif vtemp == "FINADM":
                    ansible_facts[refname].update( { 'siebel': 'False','ps_hr': 'False', 'ps_fin' : 'True', 'ps': 'True', 'ps_owner': 'finadm' } )
                elif vtemp == "SYSADM":
                    ansible_facts[refname].update( { 'siebel': 'False','ps_hr': 'True', 'ps_fin' : 'False', 'ps': 'True', 'ps_owner': 'sysadm' } )
                else:
                    ansible_facts[refname].update( { 'siebel': 'False','ps_hr': 'False', 'ps_fin' : 'False', 'ps': 'False', 'ps_owner': 'None' } )

        ignore_err_flag = False

        # # get db_size - This works, but takes WAY TOO LONG for larger databases.
        # cmd_str = "select round(sum(used.bytes) / 1024 / 1024 / 1024 ) as db_size from (select bytes from v$datafile union all select bytes from v$tempfile union all select bytes from v$log) used, (select sum(bytes) as p from dba_free_space) free group by free.p"
        # try:
        #   cur.execute(cmd_str)
        #   debugg("get db_size :: cmd_str = {}".format(cmd_str))
        # except cx_Oracle.DatabaseError as exc:
        #   error, = exc.args
        #   if vignore:
        #       ignore_err_flag = True
        #       add_to_msg("Error selecting dba_data_files count: %s " % (error.message))
        #   else:
        #       module.fail_json(msg='Error selecting log_mode from v$database, Error: %s' % (error.message), changed=False)
        #
        # if not ignore_err_flag:
        #     vtemp = cur.fetchall()
        #     vtemp = str(vtemp[0][0]) + "G"
        #     ansible_facts[refname].update( { 'db_size' : vtemp } )
        # ignore_err_flag = False

        # Get dbid for active db duplication without target, backup only
        try:
            cur.execute('select dbid from v$database')
        except cx_Oracle.DatabaseError as exc:
            error, = exc.args
            if vignore:
                ignore_err_flag = True
                add_to_msg('Error selecting dbid from v$database, Error: code : %s, message: %s, context: %s' % (error.code, error.message, error.context))
            else:
                module.fail_json(msg='Error selecting dbid from v$database, Error: code : %s, message: %s, context: %s' % (error.code, error.message, error.context), changed=False)

        if not ignore_err_flag:
            vtemp = cur.fetchall()
            vtemp = vtemp[0][0]
            ansible_facts[refname].update( { 'dbid': vtemp } )

        ignore_err_flag = False

        # Check for db supplemental logging. This is required for logminer which is used by Fivetran
        try:
            cur.execute('select supplemental_log_data_min from v$database')
        except cx_Oracle.DatabaseError as exc:
            error, = exc.args
            if vignore:
                ignore_err_flag = True
                add_to_msg('Error selecting supplemental_log_data_min from v$database, Error: code : %s, message: %s, context: %s' % (error.code, error.message, error.context))
            else:
                module.fail_json(msg='Error selecting supplemental_log_data_min from v$database, Error: code : %s, message: %s, context: %s' % (error.code, error.message, error.context), changed=False)

        if not ignore_err_flag:
            vtemp = cur.fetchall()
            vtemp = vtemp[0][0]
            ansible_facts[refname].update( { 'supplemental_log_data_min': vtemp } )

        ignore_err_flag = False

        # if is_rac:
            # Find ASM diskgroups used by the database
        try:
            cur.execute("select name from v$asm_diskgroup where state='CONNECTED'")
        except cx_Oracle.DatabaseError as exc:
            error, = exc.args
            if vignore:
                ignore_err_flag = True
                add_to_msg('Error selecting name from v$asmdiskgroup, Error: %s' % (error.message))
            else:
                module.fail_json(msg='Error selecting name from v$asmdiskgroup, Error: %s' % (error.message), changed=False)

        if not ignore_err_flag:
            vtemp = cur.fetchall()
            disks = {}
            for item in vtemp:
                debugg("ASM Diskgroup: item={}".format(item))
                if 'data' in item[0].lower():
                    disks.update( { 'data':item[0] } )
                elif 'fra' in item[0].lower():
                    disks.update( { 'fra':item[0] } )
            # diskgroups = [row[0] for row in cur.fetchall()]
            ansible_facts[refname].update({ 'diskgroups': disks }) #diskgroups
        else:
            ansible_facts[refname].update({ 'diskgroups': 'None' })
        ignore_err_flag = False

        # Open cursors - used in populating dynamic pfiles
        try:
            cur.execute("select value from v$parameter where name = 'open_cursors'")
        except cx_Oracle.DatabaseError as exc:
            error, = exc.args
            if vignore:
                ignore_err_flag = True
                add_to_msg('Error selecting value open_cursors, Error: %s' % (error.message))
            else:
                module.fail_json(msg='Error selecting value open_cursors, Error: %s' % (error.message), changed=False)

        if not ignore_err_flag:
            vtemp = cur.fetchall()
            vtemp = vtemp[0][0]
            ansible_facts[refname].update( { 'open_cursors': vtemp} )
        ignore_err_flag = False

        # pga_aggregate_target - used in populating dynamic pfiles
        try:
          cur.execute("select value from v$parameter where name = 'pga_aggregate_target'")
        except cx_Oracle.DatabaseError as exc:
          error, = exc.args
          if vignore:
              add_to_msg('Error selecting value pga_aggregate_target, Error: %s' % (error.message))
              ignore_err_flag = True
          else:
              module.fail_json(msg='Error selecting value pga_aggregate_target, Error: %s' % (error.message), changed=False)

        if not ignore_err_flag:
            vtemp = cur.fetchall()
            vtemp = vtemp[0][0]
            vtemp = convert_size(float(vtemp),"M")

            ansible_facts[refname].update({ 'pga_aggregate_target': vtemp })
        ignore_err_flag = False

        # use_large_pages - used in populating dynamic pfiles
        try:
            cur.execute("select value from v$parameter where name = 'use_large_pages'")
        except cx_Oracle.DatabaseError as exc:
            error, = exc.args
            if vignore:
                add_to_msg('Error selecting value use_large_pages, Error: %s' % (error.message))
                ignore_err_flag = True
            else:
                module.fail_json(msg='Error selecting value use_large_pages, Error: %s' % (error.message), changed=False)

        if not ignore_err_flag:
            vtemp = cur.fetchall()
            vtemp = vtemp[0][0]
            ansible_facts[refname].update({'use_large_pages': vtemp})
        ignore_err_flag = False

        # Is Block Change Tracking (BCT) enabled or disabled?
        try:
            cur.execute("select status from v$block_change_tracking")
        except cx_Oracle.DatabaseError as exc:
            error, = exc.args
            if vignore:
                ignore_err_flag = True
                add_to_msg('Error getting status of BCT, Error: %s' % (error.message))
            else:
                module.fail_json(msg='Error getting status of BCT, Error: %s' % (error.message), changed=False)

        if not ignore_err_flag:
            vtemp = cur.fetchall()
            vtemp = vtemp[0][0]
            ansible_facts[refname].update({ 'bct_status': vtemp })
        ignore_err_flag = False

        # db_create_online_log_dest_# that aren't null. Needed for utils restore.
        # they will need to be changed in the new database.
        log_dests={}
        try:
            cur.execute("select name,value from v$parameter where replace(value,'+','') in (select name from  v$asm_diskgroup where state = 'CONNECTED' and name not like '%FRA%')")
        except cx_Oracle.DatabaseError as exc:
            error, = exc.args
            if vignore:
                ignore_err_flag = True
                add_to_msg('Error selecting version from v$instance, Error: %s' % (error.message))
            else:
                module.fail_json(msg='Error selecting version from v$instance, Error: %s' % (error.message), changed=False)

        if not ignore_err_flag:
            try:
              online_logs =  cur.fetchall()
              for create_item,item_value in online_logs:
                log_dests.update({create_item: item_value})
            except cx_Oracle.DatabaseError as exc:
                error, = exc.args
                module.fail_json(msg='Error getting directory info, Error: %s' % (error.message), changed=False)

            ansible_facts[refname].update({ 'log_dest': log_dests })
        ignore_err_flag = False

        # Does the ULNFSA02_DATAPUMP directory exist?
        dirs={}
        try:
            cur.execute("select directory_name, directory_path from dba_directories order by directory_name")
        except cx_Oracle.DatabaseError as exc:
            error, = exc.args
            if vignore:
                ignore_err_flag = True
                add_to_msg('Error getting directory info, Error: %s' % (error.message))
            else:
                module.fail_json(msg='Error getting directory info, Error: %s' % (error.message), changed=False)

        if not ignore_err_flag:
            try:
                vtemp = cur.fetchall()
                for vdir,vpath in vtemp:
                    dirs.update({vdir: vpath})
            except:
                msg = msg + "%s, %s, %s" % (sys.exc_info()[0], sys.exc_info()[1], sys.exc_info()[2])
                msg = msg + ' dir returned meta %s vdir: %s vpath: %s' % (vtemp,vdir,vpath)
                module.fail_json(msg='ERROR: %s' % (msg), changed=False)

            ansible_facts[refname].update( { 'dirs': dirs } )
        ignore_err_flag = False

        # BCT path
        try:
            cur.execute("select filename from v$block_change_tracking")
        except cx_Oracle.DatabaseError as exc:
            error, = exc.args
            if vignore:
                add_to_msg('Error getting status of BCT, Error: %s' % (error.message))
                ignore_err_flag = True
            else:
                module.fail_json(msg='Error getting status of BCT, Error: %s' % (error.message), changed=False)

        if not ignore_err_flag:
            vtemp = cur.fetchall()
            vtemp = vtemp[0][0]
            ansible_facts[refname].update( { 'bct_file': vtemp } )
        ignore_err_flag = True

        meta_msg = ''

        # Get a list of schema owners that aren't Oracle owned schemas
        oracle_owned = "'MDSYS','SQLTXADMIN','PUBLIC','OUTLN','CTXSYS','FLOWS_FILES','SYSTEM','ORACLE_OCM','EXFSYS','APEX_030200','DBSNMP','ORDSYS','ORDPLUGINS','TOAD','SQLTXPLAIN','APPQOSSYS','XDB','ORDDATA','SYS','WMSYS','SI_INFORMTN_SCHEMA','MIGDBA'"
        try:
            cur.execute("select unique(owner) from dba_objects where owner not in (%s)" % (oracle_owned))
        except cx_Oracle.DatabaseError as exc:
            error, = exc.args
            if vignore:
                ignore_err_flag = True
                add_to_msg('Error getting status of BCT, Error: %s' % (error.message))
            else:
                module.fail_json(msg='Error getting status of BCT, Error: %s' % (error.message), changed=False)

        if not ignore_err_flag:
            vtemp = cur.fetchall()
            add_to_msg("schema_owners={}".format(vtemp))
            owner_list = []
            for own in vtemp:
                owner_list.append(own[0].encode("utf-8"))

            ansible_facts[refname].update( { 'schema_owners': owner_list } )

        ignore_err_flag = False

        # Get default_temp_tablespace and default_permanet_tablespace
        try:
            cur.execute("select property_name,property_value from database_properties where property_name like 'DEFAULT%TABLESPACE'")
        except cx_Oracle.DatabaseError as exc:
            error, = exc.args
            if vignore:
                ignore_err_flag = True
                add_to_msg('Error getting status of BCT, Error: %s' % (error.message))
            else:
                module.fail_json(msg='Error getting status of BCT, Error: %s' % (error.message), changed=False)

        if not ignore_err_flag:
            vtemp = cur.fetchall()
            if cur.rowcount > 0:
                ansible_facts[refname].update({ vtemp[0][0]: vtemp[0][1] } )
                ansible_facts[refname].update({ vtemp[1][0]: vtemp[1][1] } )
        ignore_err_flag = False

        # Get tablespace name like %USER% if one exists:
        try:
            cur.execute("select name from v$tablespace where upper(name) like '%USER%'")
        except cx_Oracle.DatabaseError as exc:
            error, = exc.args
            if vignore:
              ignore_err_flag = True
              add_to_msg('Error getting status of BCT, Error: %s' % (error.message))
            else:
              module.fail_json(msg='Error getting status of BCT, Error: %s' % (error.message), changed=False)

        if not ignore_err_flag:
            vtemp = cur.fetchall()
            vtemp = vtemp[0][0]
            if vtemp:
                ansible_facts[refname].update( { 'USER_TABLESPACE': vtemp } )
        ignore_err_flag = False

        if usable_ver[:2] != "11":
            # Get sourcedb home:
            try:
              cur.execute("select SYS_CONTEXT ('USERENV','ORACLE_HOME') from dual")
            except cx_Oracle.DatabaseError as exc:
              error, = exc.args
              if vignore:
                  ignore_err_flag = True
                  add_to_msg('Error getting sourcedb home, Error: %s' % (error.message))
              else:
                  module.fail_json(msg='Error getting sourcedb home, Error: %s' % (error.message), changed=False)

            if not ignore_err_flag:
                vtemp = cur.fetchall()
                vtemp = vtemp[0][0]
                if vtemp:
                    ansible_facts[refname].update( { 'oracle_home': vtemp} )
                    # extract version from oracle_home: /app/oracle/12.1.0.2/dbhome_1
                    v=vtemp.split("/")[3]
                    ansible_facts[refname].update( { 'oracle_version': v} )

            ignore_err_flag = False

        # We know there's no easy way like 12 to get database home internally for 11 so set 11 manually
        if "11" in ansible_facts[refname]['compatible']:
            comp = ""
            db_home = ""
            comp = ansible_facts[refname]['compatible']
            if len(comp.split(".")) > 4:
                db_home = comp[:-(len(comp.split(".")[4])+1):]
            else:
                db_home = comp
            home = '/app/oracle/%s/%s' %  ( db_home, db_home_name)
            debugg("-------- > home=%s" % (home))
            # this splits compatible 11.2.0.4.0 then takes len of the last item adds 1 for the decimal '-' drops the last two chars
            ansible_facts[refname].update( { 'oracle_home' : home } )

        # See if dbainfo user/schema exists
        try:
            cur.execute("select 1 from dba_users where username = 'DBAINFO'")
        except cx_Oracle.DatabaseError as exc:
            error, = exc.args
            if vignore:
                ignore_err_flag = True
                add_to_msg('Error checking existance of dbinfo schema, Error: %s' % (error.message))
            else:
                module.fail_json(msg='Error checking existance of dbinfo schema, Error: %s' % (error.message), changed=False)

        if not ignore_err_flag:
            vtemp = cur.fetchall()
            if cur.rowcount == 0:
                ansible_facts[refname].update({'dbainfo': {'schema_exists': 'False' }} )
                ansible_facts[refname]['dbainfo'].update({'table_exists': 'False' })
            else:
                ansible_facts[refname].update({'dbainfo': {'schema_exists': 'True'}} )

                # if dbainfo schema exists see if dbainfo table exists in the schema
                if cur.rowcount == 1:

                    try:
                        cur.execute("select 1 from dba_objects where owner = 'DBAINFO' and object_name = 'DBAINFO' and object_type ='TABLE' ")
                    except cx_Oracle.DatabaseError as exc:
                        error, = exc.args
                        module.fail_json(msg='Error getting status of BCT, Error: %s' % (error.message), changed=False)

                    vtemp = cur.fetchall()
                    if cur.rowcount == 0:
                        ansible_facts[refname]['dbainfo'].update({'table_exists': 'False' } )
                    else:
                        ansible_facts[refname]['dbainfo'].update({'table_exists': 'True' } )

            ignore_err_flag = False

        try:
            cur.close()
        except cx_Oracle.DatabaseError as exc:
          error, = exc.args
          add_to_msg('Error closing cursor: Error: %s' % (error.message))
          # module.fail_json(msg='Error closing cursor: Error: %s' % (error.message), changed=False)
          pass # No reason to fail at this point

        add_to_msg("Custom module dbfacts, called as refname: %s, succeeded for %s database." % (refname, vdb))

        vchanged="False"

        # print json.dumps( ansible_facts_dict )
        ansible_facts[refname].update( { "success" : "true" } )

  # if parameters were NULL return informative error.
    else:

        msg="Custom module dbfacts Failed"
        # sourcefacts={}
        if vdbpass is None:
            ansible_facts['systempwd'] = 'missing'
        else:
            ansible_facts['systempwd'] = 'ok'

        if vdb is None:
            ansible_facts['source_db_name'] = 'missing'
        else:
            ansible_facts['source_db_name'] = 'ok'

        if vdbhost is None:
            ansible_facts['source_host'] = 'missing'
        else:
            ansible_facts['source_host'] = 'ok'

        if vrefname is None:
            ansible_facts['refname'] = 'missing'
        else:
            ansible_facts['refname'] = 'ok'

        vchanged="False"

        # print json.dumps( ansible_facts_dict )
        ansible_facts[refname].update( { "success" : "false" } )


    module.exit_json( msg=msg, ansible_facts=ansible_facts , changed=vchanged)

# code to execute if this program is called directly
if __name__ == "__main__":
    main()
