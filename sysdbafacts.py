#!/usr/bin/env python3

from ansible.module_utils.basic import *
from ansible.module_utils.facts import *
import subprocess
import sys
import os
import json
import re
import math
import string

try:
    import cx_Oracle
    cx_Oracle_found = True
except ImportError:
    cx_Oracle_found = False

# Created by: S Kohler
# Date: March 16, 2019
#
# updated: 2019 : August 7
#
# Reference links
# http://www.oracle.com/technetwork/articles/dsl/prez-python-queries-101587.html

# Notes: IAW this doc : http://docs.ansible.com/ansible/latest/dev_guide/developing_modules_general.html
# This module was setup to return a dictionary called anything the user passes in as a reference name
# in the ansible playbook, and roles. The facts in this module are referenced by using the format:
#                    whatevertheusercalledit['key'] which returns associated value - the ref name : "sourcefacts" was created in this module
#     example:    {{ sysdbafact['control_files'] }} => +DATA3/TSTDB/CONTROLFILE/current.570.1002968751, +FRA/TSTDB/CONTROLFILE/current.22751.1002968751

ANSIBLE_METADATA = {
    'status': ['stableinterface'],
    'supported_by': 'Cru DBA team',
    'version': '0.1'
}

DOCUMENTATION = '''
---
module: sysdbafacts
short_description: Wanted to be able to connect to a database in startup nomount state
                   to retrieve the current control_files. This overcomes the problem
                   with setcntrlfile module where it retrieves the controlfiles using
                   asmcmd and there can sometimes be multiple control files.

notes: Returned values are then available to use in Ansible.
requirements: [ python2.* ]
author: "DBA Oracle module Team"
'''

EXAMPLES = '''

# When cloning a database source database password file use is recommended.
- local_action:
    module: sysdbafacts
    syspwd: "{{ database_passwords[source_db_name].sys }}"
    db_name: "{{ source_db_name }}"
    host: "{{ source_host }}"
    israc: "{{ sourcefacts['cluster_database']|bool }}" (1)
    pfile_name: "pfile.ora" (2)
    share_dir: "{{ share_dir }}" (3)
    src_passwd_dir: "{{ /oracle_home/dbs }}" (4)
    oracle_home: "{{ oracle_home }}"
    refname: "{{ refname_str }} (5)"
    ignore: True (6)
    force_switch: True (7)
    debugging: False (8)
  become_user: "{{ utils_local_user }}"
  register: sys_facts

  (1) israc      - (required) is RAC - this matters when the module puts together the db sid

  (2) pfile_name - optional. If provided a pfile by that name will be created to that share directory/filename
                   if not provided pfile.ora will be used. ( using default is recommended )

  (3) share_dir  - location thats accessible to both source db and dest db

  (4) src_passwd_dir - location of the source db password file : $ORACLE_HOME/dbs

  (5) refname        - (optional) name used in Ansible to reference these facts ( i.e. sourcefacts, destfacts, sysdbafacts )

  (6) ignore     - (optional) (connection errors). If you know the source
      database may be down set ignore: True. If connection to the
      source database fails the module will not throw a fatal error
      to stop the play and continue.

  (7) force_switch - Force archive logs to write to disk. Helpful when working with new db that hasn't.

  (8) debugging  - (optional) - if 'True' will add debugging statements to the output msg.

'''

RETURN = '''

original_message:
    description: The original name param that was passed in
    type: str
    returned: always

message:
    description: The output message that the test module generates
    type: str
    returned: always

'''
# Add anything from v$parameter table to retrieve here and it will be available
# for reference when this module runs.
vparams=[ "compatible",
          "sga_target",
          "diagnostic_dest",
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
          "control_files" ]

# Global vars
msg = ""
debugme = False
default_ora_base = "/app/oracle/"
defualt_ora_home = "dbhome_1"
default_pfile_name = "src_pfile.ora" # This is the name the play is looking for
default_refname = "sysdbafacts"
affirm = [ 'True', 'TRUE', 'true', True, 'YES', 'Yes', 'yes', 't', 'T', 'y', 'Y', 'ON', 'on', 'On']


def add_to_msg(mytext):
    """Passed some text add it to the msg"""
    global msg

    if not msg:
        msg = str(mytext)
    else:
        msg = msg + " " + str(mytext)


def debugg(a_str):
    """If debugging is on add debugging string to global msg"""
    global debugme

    if debugme:
        add_to_msg(a_str)


def convert_size(size_bytes, vunit):

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


# ==============================================================================
# =================================== MAIN =====================================
# ==============================================================================
def main ():
  """ Run as sysdba against a database in startup nomount mode and retrun parameters"""
  global msg
  global debugme
  global affirm
  global default_ora_base
  global default_pfile_name
  global default_refname

  ansible_facts={}

  module = AnsibleModule(
      argument_spec = dict(
        syspwd          =dict(required=True),
        db_name         =dict(required=True),
        host            =dict(required=True),
        is_rac          =dict(required=True),
        refname         =dict(required=True),
        oracle_home     =dict(required=True),
        pfile_name      =dict(required=False),
        src_passwd_dir  =dict(required=False),
        share_dir       =dict(required=False),
        ignore          =dict(required=False),
        force_switch    =dict(required=False),
        debugging       =dict(required=False)
      ),
      supports_check_mode=True,
  )

  # Get arguements passed from Ansible playbook
  vdbpass           = module.params.get('syspwd')
  vdb               = module.params.get('db_name')
  vdbhost           = module.params.get('host')
  visrac            = module.params.get('is_rac')
  vrefname          = module.params.get('refname')
  vignore           = module.params.get('ignore')
  vpfile            = module.params.get('pfile_name')
  vsrc_passwd_dir   = module.params.get('src_passwd_dir')
  vshare_dir        = module.params.get('share_dir')
  voracle_home      = module.params.get('oracle_home')
  vdebugging        = module.params.get('debugging')
  vforce_switch     = module.params.get('force_switch')

  if vdebugging:
      debugme = vdebugging

  if vdb:
      orig_vdb_name = vdb

  if vignore is None or not vignore:
      vignore = False

  if not cx_Oracle_found:
    module.fail_json(msg="Error: cx_Oracle module not found!")

  if not vrefname:
    refname = default_refname
  else:
    refname = vrefname

  # check vars passed in are not NULL. All are needed to connect to source db
  if ( vdbpass is not None) and (vdb is not None) and (vdbhost is not None):

    ansible_facts = { refname : {} }

    if '.org' in vdbhost:
        # vdbhost => full_hostname and abbr_hostname
        abbr_hostname = vdbhost.replace(".ccci.org","")
        full_hostname = vdbhost
    else:
        abbr_hostname = vdbhost.replace(".ccci.org","")
        full_hostname = abbr_hostname + ".ccci.org"

    if visrac in affirm:
        if abbr_hostname[-1:].isdigit():
            vdb = vdb + abbr_hostname[-1:]
        else:
            add_to_msg("Error attempting to configure database sid for db: %s" % (vdb))

    try:
      # vdb = vdb + vdbhost[-1:]
      dsn_tns = cx_Oracle.makedsn(full_hostname, '1521', vdb)
    except cx_Oracle.DatabaseError as exc:
      error, = exc.args
      module.fail_json(msg='TNS generation error: %s, db name: %s host: %s' % (error.message, vdb, vdbhost), changed=False)

    debugg("Attempting to connect to db=%s full_hostname=%s dsn_tns=%s " % (vdb, full_hostname, dsn_tns))

    try:
      con = cx_Oracle.connect(dsn=dsn_tns,user='sys',password=vdbpass,mode=cx_Oracle.SYSDBA)
    except cx_Oracle.DatabaseError as exc:
      error, = exc.args
      debugg("Oracle-Error-Code: %s" % (error.code))
      debugg("Oracle-Error-Message: %s" % (error.message))
      if vignore:
          add_to_msg("DB CONNECTION FAILED")
          debugg("vignore: %s" % (vignore))
          module.exit_json(msg=msg, ansible_facts=ansible_facts, changed="False")
      else:
          error, = exc.args
          module.fail_json(msg='Database connection error: %s, tnsname: %s host: %s' % (error.message, vdb, vdbhost), changed=False)

    cur = con.cursor()

    # Generate pfile :
    if vpfile: #  and str(vdbhost[-1:]) == "1:
        cmd_str = "create pfile='%s/%s' from spfile" % (vshare_dir,vpfile)
        try:
          cur.execute(cmd_str)
        except cx_Oracle.DatabaseError as exc:
          error, = exc.args
          if vignore in ("true","yes"):
              add_to_msg("Error creating pfile: %s" % (error.message))
          else:
              module.fail_json(msg='Error creating pfile : %s' % (error.message), changed=False)

        ansible_facts[refname].update( { 'pfile': { 'written':'true','location': vpfile} } )


    # select source db version
    try:
      cur.execute('select version from v$instance')
    except cx_Oracle.DatabaseError as exc:
      error, = exc.args
      module.fail_json(msg='Error selecting version from v$instance, Error: %s' % (error.message), changed=False)

    dbver =  cur.fetchall()
    retver = dbver[0][0]
    usable_ver = ".".join(retver.split('.')[0:-1])
    ansible_facts[refname].update( { 'version': usable_ver, 'oracle_version_full': retver } )
    default_db_ver = usable_ver

    # if vpfile ( pfile ) parameter is defined copy the password file and from $ORACLE_HOME to pfile directory
    # get the directory part and dropping the pfile.ora name ( can be any name this way ).
    if vpfile:
        # pfile_dir = vpfile[:vpfile.rindex('/')] # vpfile contains /the/full/path/to/pfile.ora  This command strips /pfile.ora so only the dir is left.
        if not vsrc_passwd_dir:
            orapw_source = "%s/dbs" % (voracle_home)
        else:
            orapw_source = vsrc_passwd_dir

        if not vshare_dir:
            orapw_dest = "/app/oracle/backups/%s/pfile" % (orig_vdb_name.upper())
        else:
            orapw_dest = vshare_dir

        try:
            cmd_str = "create or replace directory ORAPW_DEST as '%s'" % (orapw_dest)
            cur.execute(cmd_str)
        except cx_Oracle.DatabaseError as exc:
            add_to_msg("Error creating orapw_dest directory")
            error, = exc.args
            if not vignore:
                module.fail_json(msg='Error creating orapwd dest dir: %s msg: %s cmd_str: %s' % (error.message,msg,cmd_str), changed=False)
            else:
                add_to_msg('Error creating orapwd dest dir: %s msg: %s cmd_str: %s' % (error.message,msg,cmd_str))

        add_to_msg("orapw_dest directory [%s] created successfully." % (orapw_dest))

        try:
            cmd_str = "create or replace directory ORAPW_SOURCE as '%s'" % (orapw_source)
            cur.execute(cmd_str)
        except cx_Oracle.DatabaseError as exc:
            add_to_msg("Error creating orapw_dest directory")
            error, = exc.args
            if not vignore:
                module.fail_json(msg='Error creating orapwd dest dir (%s): %s cmd_str: %s msg: %s' % (orapw_source,error.message,cmd_str,msg), changed=False)
            else:
                add_to_msg('Error creating orapwd dest dir (%s): %s cmd_str: %s msg: %s' % (orapw_source,error.message,cmd_str,msg))

        add_to_msg("orapw_source directory [%s] created successfully." % (orapw_source))

        try:
            # Rename the current pfile to backup
            cmd_str = ("BEGIN UTL_FILE.FRENAME('%s','orapw%s','%s','orapw%s_bu', TRUE); END;" % ('ORAPW_DEST',vdb,'ORAPW_DEST',vdb))
            cur.execute(cmd_str)
        except cx_Oracle.DatabaseError as exc:
            add_to_msg("Error: attempting to rename current password file orapw%s to orapw%s_bu" % (vdb,vdb))
            error, = exc.args
            if 'ORA-29283' not in error.message:
                if not vignore:
                    module.fail_json(msg='Error: Renaming orapwd file in data domain backups dir: %s cmd_str: %s msg: %s' % (error.message, cmd_str, msg), changed=False)
                else:
                    add_to_msg('Error: Renaming orapwd file in backups dir: %s cmd_str: %s msg: %s' % (error.message, cmd_str, msg))

        # because the above command has no output unless it fails. If you've made it
        # here, assume it succeeded.
        add_to_msg("orapw file successfully renamed.")

        cmd_str = ("BEGIN DBMS_FILE_TRANSFER.COPY_FILE(source_directory_object=>'ORAPW_SOURCE',source_file_name=>'orapw%s',destination_directory_object=>'ORAPW_DEST',destination_file_name=>'orapw%s'); END;" % (vdb,vdb))

        # Copy orapwd file from source dir ( ORACLE_HOME/dbs ) to dest dir ( /apps/oracle/backups/DBNAME/pfile )
        try:
          cur.execute(cmd_str)
        except cx_Oracle.DatabaseError as exc:
            add_to_msg("Error attempting to copy orapw%s from %s to %s" %(vdb,orapw_source, orapw_dest))
            error, = exc.args
            if not vignore:
                module.fail_json(msg='Error: Copying orapwd file from ORACLE_HOME/dbs to /app/oracle/backups/%s/pfile : %s' % (vdb.upper, error.message), changed=False)
            else:
                add_to_msg('Error: Copying orapwd file from ORACLE_HOME/dbs to /app/oracle/backups/%s/pfile : %s' % (vdb.upper, error.message))

        add_to_msg("orapwd%s successfully copied from %s to: %s " % (vdb, orapw_source, orapw_dest))

    # select host_name
    try:
        cmd_str = 'select host_name from v$instance'
        cur.execute(cmd_str)
    except cx_Oracle.DatabaseError as exc:
        error, = exc.args
        if not vignore:
            module.fail_json(msg='Error selecting host_name from v$instance, Error: %s' % (error.message), changed=False)
        else:
            add_to_msg('Error selecting host_name from v$instance, Error: %s' % (error.message))

    vtemp = cur.fetchall()
    vtemp = vtemp[0][0]
    ansible_facts[refname].update( { 'host_name': vtemp } )

    # Find archivelog mode.
    try:
      cur.execute('select log_mode from v$database')
    except cx_Oracle.DatabaseError as exc:
      error, = exc.args
      if not vignore:
          module.fail_json(msg='Error selecting log_mode from v$database, Error: %s' % (error.message), changed=False)
      else:
          add_to_msg('Error selecting log_mode from v$database, Error: %s' % (error.message))

    vtemp = cur.fetchall()
    vtemp = vtemp[0][0]
    if vtemp == 'ARCHIVELOG':
      vtemp = 'True'
    else:
      vtemp = 'False'

    ansible_facts[refname].update( { 'archivelog': vtemp } )

    # Get dbid for active db duplication without target, backup only
    try:
      cur.execute('select dbid from v$database')
    except cx_Oracle.DatabaseError as exc:
      error, = exc.args
      if not vignore:
          module.fail_json(msg='Error selecting dbid from v$database, Error: code : %s, message: %s, context: %s' % (error.code, error.message, error.context), changed=False)
      else:
          add_to_msg('Error selecting dbid from v$database, Error: code : %s, message: %s, context: %s' % (error.code, error.message, error.context))


    vtemp = cur.fetchall()
    vtemp = vtemp[0][0]
    ansible_facts[refname].update( { 'dbid': vtemp } )

    # Find ASM diskgroups used by the database
    try:
      cur.execute("select name from v$asm_diskgroup where state='CONNECTED' and name not like '%FRA%'")
    except cx_Oracle.DatabaseError as exc:
      error, = exc.args
      if not vignore:
        module.fail_json(msg='Error selecting name from v$asmdiskgroup, Error: %s' % (error.message), changed=False)
      else:
          add_to_msg('Error selecting name from v$asmdiskgroup, Error: %s' % (error.message))

    vtemp = cur.fetchall()
    if vtemp:
        vtemp = vtemp[0][0]
        # diskgroups = [row[0] for row in cur.fetchall()]
        ansible_facts[refname].update( { 'diskgroups': vtemp } )
    else:
        ansible_facts[refname].update( { 'diskgroups': 'None' } )

    # Open cursors - used in populating dynamic pfiles
    try:
      cur.execute("select value from v$parameter where name = 'open_cursors'")
    except cx_Oracle.DatabaseError as exc:
      error, = exc.args
      if not vignore:
          module.fail_json(msg='Error selecting value open_cursors, Error: %s' % (error.message), changed=False)
      else:
          add_to_msg('Error selecting value open_cursors, Error: %s' % (error.message))

    vtemp = cur.fetchall()
    vtemp = vtemp[0][0]
    ansible_facts[refname]['open_cursors'] = vtemp

    # pga_aggregate_target - used in populating dynamic pfiles
    try:
      cur.execute("select value from v$parameter where name = 'pga_aggregate_target'")
    except cx_Oracle.DatabaseError as exc:
      error, = exc.args
      if not vignore:
          module.fail_json(msg='Error selecting value pga_aggregate_target, Error: %s' % (error.message), changed=False)
      else:
          add_to_msg('Error selecting value pga_aggregate_target, Error: %s' % (error.message))

    vtemp = cur.fetchall()
    vtemp = vtemp[0][0]
    ansible_facts[refname]['pga_aggregate_target'] = vtemp

    # use_large_pages - used in populating dynamic pfiles
    try:
      cur.execute("select value from v$parameter where name = 'use_large_pages'")
    except cx_Oracle.DatabaseError as exc:
      error, = exc.args
      if not vignore:
          module.fail_json(msg='Error selecting value use_large_pages, Error: %s' % (error.message), changed=False)
      else:
          add_to_msg('Error selecting value use_large_pages, Error: %s' % (error.message))

    vtemp = cur.fetchall()
    vtemp = vtemp[0][0]
    ansible_facts[refname]['use_large_pages'] = vtemp

    # Is Block Change Tracking (BCT) enabled or disabled?
    try:
      cur.execute("select status from v$block_change_tracking")
    except cx_Oracle.DatabaseError as exc:
      error, = exc.args
      if not vignore:
          module.fail_json(msg='Error getting status of BCT, Error: %s' % (error.message), changed=False)
      else:
          add_to_msg('Error getting status of BCT, Error: %s' % (error.message))

    vtemp = cur.fetchall()
    vtemp = vtemp[0][0]
    ansible_facts[refname]['bct_status'] = vtemp

    # db_create_online_log_dest_# that aren't null. Needed for utils restore.
    # they will need to be changed in the new database.
    log_dests={}
    try:
      cur.execute("select name,value from v$parameter where replace(value,'+','') in (select name from  v$asm_diskgroup where state = 'CONNECTED' and name not like '%FRA%')")
    except cx_Oracle.DatabaseError as exc:
      error, = exc.args
      if not vignore:
          module.fail_json(msg='Error selecting version from v$instance, Error: %s' % (error.message), changed=False)
      else:
          add_to_msg('Error selecting version from v$instance, Error: %s' % (error.message))

    try:
      online_logs =  cur.fetchall()
      for create_item,item_value in online_logs:
        log_dests.update({create_item: item_value})
    except cx_Oracle.DatabaseError as exc:
      error, = exc.args
      module.fail_json(msg='Error getting directory info, Error: %s' % (error.message), changed=False)

    ansible_facts[refname]['log_dest'] = log_dests


    # Does the ULNFSA02_DATAPUMP directory exist?
    dirs={}
    try:
      cur.execute("select directory_name, directory_path from dba_directories order by directory_name")
    except cx_Oracle.DatabaseError as exc:
      error, = exc.args
      module.fail_json(msg='Error getting directory info, Error: %s' % (error.message), changed=False)

    try:
        vtemp = cur.fetchall()
        for vdir,vpath in vtemp:
            dirs.update({vdir: vpath})
    except:
        msg = msg + "%s, %s, %s" % (sys.exc_info()[0], sys.exc_info()[1], sys.exc_info()[2])
        msg = msg + ' dir returned meta %s vdir: %s vpath: %s' % (vtemp,vdir,vpath)
        module.fail_json(msg='ERROR: %s' % (msg), changed=False)

    ansible_facts[refname]['dirs'] = dirs

    # Force log switch to force archvielogs to write
    log_switch =False
    if vforce_switch in affirm:
        try:
          cur.execute("ALTER SYSTEM CHECKPOINT")
        except cx_Oracle.DatabaseError as exc:
          error, = exc.args
          if vignore not in affirm:
              module.fail_json(msg='Error getting sourcedb home, Error: %s' % (error.message), changed=False)

        log_switch = True

        try:
            cur.execute("ALTER SYSTEM SWITCH LOGFILE")
        except cx_Oracle.DatabaseError as exc:
            error, = exc.args
            if vignore not in affirm:
                module.fail_json(msg='Error getting sourcedb home, Error: %s' % (error.message), changed=False)

        ansible_facts[refname]['archive_log_switch'] = log_switch
    else:
        ansible_facts[refname]['archive_log_switch'] = log_switch

    # BCT path
    try:
      cur.execute("select filename from v$block_change_tracking")
    except cx_Oracle.DatabaseError as exc:
      error, = exc.args
      module.fail_json(msg='Error getting status of BCT, Error: %s' % (error.message), changed=False)

    vtemp = cur.fetchall()
    vtemp = vtemp[0][0]
    ansible_facts[refname]['bct_file'] = vtemp

    meta_msg = ''


    # Get default_temp_tablespace and default_permanet_tablespace
    try:
      cur.execute("select property_name,property_value from database_properties where property_name like 'DEFAULT%TABLESPACE'")
    except cx_Oracle.DatabaseError as exc:
      error, = exc.args
      module.fail_json(msg='Error getting status of BCT, Error: %s' % (error.message), changed=False)

    vtemp = cur.fetchall()
    if cur.rowcount > 0:
        ansible_facts[refname][vtemp[0][0]] = vtemp[0][1]
        ansible_facts[refname][vtemp[1][0]] = vtemp[1][1]

    # Get tablespace name like %USER% if one exists:
    try:
      cur.execute("select name from v$tablespace where upper(name) like '%USER%'")
    except cx_Oracle.DatabaseError as exc:
      error, = exc.args
      module.fail_json(msg='Error getting status of BCT, Error: %s' % (error.message), changed=False)

    vtemp = cur.fetchall()
    vtemp = vtemp[0][0]
    if vtemp:
        ansible_facts[refname]['USER_TABLESPACE'] = vtemp

    if usable_ver[:2] == "12":
        # Get sourcedb home:
        try:
          cur.execute("select SYS_CONTEXT ('USERENV','ORACLE_HOME') from dual")
        except cx_Oracle.DatabaseError as exc:
          error, = exc.args
          module.fail_json(msg='Error getting sourcedb home, Error: %s' % (error.message), changed=False)

        vtemp = cur.fetchall()
        vtemp = vtemp[0][0]
        if vtemp:
            ansible_facts[refname]['oracle_home'] = vtemp

    # elif usable_ver[:2] == "11":
    #
    #     cmd_str = "DECLARE\n  OH varchar2(100);\nBEGIN\n  dbms_system.get_env('ORACLE_HOME', :OH);\n  dbms_output.put_line(OH);\nEND;"
    #
    #     # Get sourcedb home:
    #     try:
    #       cur.execute(cmd_str)
    #     except cx_Oracle.DatabaseError as exc:
    #       error, = exc.args
    #       module.fail_json(msg='Error getting sourcedb home, Error: %s' % (error.message), changed=False)
    #
    #     vtemp = cur.fetchall()
    #     vtemp = vtemp[0][0]
    #     if vtemp:
    #         ansible_facts[refname]['oracle_home'] = vtemp

    # See if dbainfo user/schema exists
    try:
      cur.execute("select 1 from dba_users where username = 'DBAINFO'")
    except cx_Oracle.DatabaseError as exc:
      error, = exc.args
      module.fail_json(msg='Error getting status of BCT, Error: %s' % (error.message), changed=False)

    vtemp = cur.fetchall()
    if cur.rowcount == 0:
        ansible_facts[refname].update({'dbainfo': {'exists': 'False' }} )
        ansible_facts[refname]['dbainfo'].update({'dbainfo': 'False' })
    else:
        ansible_facts[refname].update({'dbainfo': {'dbainfo_schema': 'True'}} )

    # if dbainfo schema exists see if dbainfo table exists in the schema
    if cur.rowcount == 1:

        try:
            cur.execute("select 1 from dba_objects where owner = 'DBAINFO' and object_name = 'DBAINFO' and object_type ='TABLE' ")
        except cx_Oracle.DatabaseError as exc:
            error, = exc.args
            module.fail_json(msg='Error getting status of BCT, Error: %s' % (error.message), changed=False)

        vtemp = cur.fetchall()
        if cur.rowcount == 0:
            ansible_facts[refname]['dbainfo'].update({'dbainfo': 'False' } )
        else:
            ansible_facts[refname]['dbainfo'].update({'dbainfo': 'True' } )

    # get parameters listed in the header of this program defined in "vparams"
    for idx in range(len(vparams)):
        try:
          cmd_str = "select value from v$parameter where name = '" + vparams[idx] + "'"
          cur.execute(cmd_str)
        except cx_Oracle.DatabaseError as exc:
          error, = exc.args
          if msg:
              msg = msg + "idx: %s " % (idx)
          else:
              msg = "idx: %s" % (idx)
          module.fail_json(msg='Error selecting name from v$asmdiskgroup, Error: %s' % (error.message), changed=False)

        vtemp = cur.fetchall()
        debugg("RAW OUTPUT [vtemp] => %s" % (vtemp))
        vtemp = vtemp[0][0]
        debugg("idx=%s vparams=%s cmd_str=%s output=%s" % (idx,vparams[idx],cmd_str,vtemp))
        try:
            if 'sga_target' == vparams[idx] or 'db_recovery_file_dest_size' == vparams[idx]:
                vtemp = convert_size(float(vtemp),"M")
                ansible_facts[refname][vparams[idx]] = vtemp
            elif 'db_recovery_file_dest' == vparams[idx]:
                try:
                    ansible_facts[refname][vparams[idx]] = vtemp[1:]
                except Exception as e:
                    ansible_facts[refname][vparams[idx]] = "None"
            elif 'listener' in vparams[idx]:
                try:
                    head, sep, tail = vtemp.partition('.')
                    ansible_facts[refname][vparams[idx]] = head
                except Exception as e:
                    ansible_facts[refname][vparams[idx]] = "None"
            elif 'control_files' == vparams[idx]:
                debugg("'control_files' == vparams[idx] => %s vtemp => %s len=%s" % (vparams[idx], vtemp, str(len(vtemp))))
                tmp = vtemp.split(',')
                debugg("after splitting on comma tmp = %s len= %s " % (tmp, len(tmp)))
                data_cntrlfile = tmp[0].strip()
                ansible_facts[refname][vparams[idx]] = {'data_control_file': data_cntrlfile}
                try:
                    if len(tmp) > 1:
                        fra_cntrlfile = tmp[1].strip()
                        ansible_facts[refname][vparams[idx]].update({'fra_control_file': fra_cntrlfile})
                except Exception as e:
                    add_to_msg(e.args)
                    module.fail_json(msg=msg, changed=False)
            else:
                ansible_facts[refname][vparams[idx]] = vtemp
        except cx_Oracle.DatabaseError as exc:
          error, = exc.args
          if msg:
              add_to_msg("vtemp: %s " % (vtemp))
              add_to_msg('Error selecting name from v$asmdiskgroup, Error: %s' % (error.message))
          module.fail_json(msg=msg, changed=False)

    try:
        cur.close()
    except cx_Oracle.DatabaseError as exc:
      error, = exc.args
      module.fail_json(msg='Error getting status of BCT, Error: %s' % (error.message), changed=False)

    msg="Custom module dbfacts succeeded for %s database." % (vdb)

    vchanged="False"

  # if parameters were NULL return informative error.
  else:

    msg="Custom module dbfacts Failed"
    # sourcefacts={}
    if module.params['syspwd'] is None:
        ansible_facts['syspwd'] = 'missing'
    else:
        ansible_facts['syspwd'] = 'ok'

    if module.params['source_db_name'] is None:
        ansible_facts['source_db_name'] = 'missing'
    else:
        ansible_facts['source_db_name'] = 'ok'

    if module.params['source_host'] is None:
        ansible_facts['source_host'] = 'missing'
    else:
        ansible_facts['source_host'] = 'ok'

    if module.params['refname'] is None:
        ansible_facts['refname'] = 'missing'
    else:
        ansible_facts['refname'] = 'ok'

    vchanged="False"

  # print json.dumps( ansible_facts_dict )
  module.exit_json( msg=msg, ansible_facts=ansible_facts , changed=vchanged)

# code to execute if this program is called directly
if __name__ == "__main__":
   main()
