#!/usr/bin/env python3

from ansible.module_utils.basic import *
from ansible.module_utils.facts import *
# import commands
import subprocess
import sys
import os
import json
import re
import math

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
module: sourcefacts
short_description: Get Oracle Database facts from a remote database.
(remote database = a database not in the group being operated on)

notes: Returned values are then available to use in Ansible.
requirements: [ python2.* ]
author: "DBA Oracle module Team"
'''

EXAMPLES = '''

    # if cloning a database and source database information is desired
    - local_action:
        module: sourcefacts
        systempwd: "{{ database_passwords[source_db_name].system }}"
        source_db_name: "{{ source_db_name }}"
        source_host: "{{ source_host }}"
        ignore: True (1)
      become_user: "{{ remote_user }}"
      register: src_facts

      (1) ignore (connection errors) is optional. If you know the source
          database may be down set ignore: True. If connection to the
          source database fails the module will not throw a fatal error
          and continue.

   NOTE: these modules can be run with the when: master_node statement.
         However, their returned values cannot be referenced in
         roles or tasks later. Therefore, when running fact collecting modules,
         run them on both nodes. Do not use the "when: master_node" clause.

'''

# Add anything from v$parameter table to retrieve in sourcefacts here.
vparams=[ "compatible",
          "sga_target",
          "sga_max_size",
          "memory_target",
          "memory_max_target",
          "pga_aggregate_target",
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
          "db_files" ]
msg = ""
debugme = False

def msgg(add_string):
    """Passed a string add it to the msg to pass back to the user"""
    global msg

    if msg:
        msg = msg + add_string
    else:
        msg = add_string


def debugg(add_string):
    """If debugme is True add this debugging information to the msg to be passed out"""
    global debugme
    global msg

    if debugme == "True":
        msgg(add_string)


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
  """ Return Oracle database parameters from a database not in the specified group"""
  global msg
  ansible_facts={}

  # Name to call facts dictionary being passed back to Ansible
  # This will be the name you reference in Ansible. i.e. source_facts['sga_target'] (source_fact)
  refname = 'sourcefacts'

  os.system("/usr/bin/scl enable python27 bash")
  # os.system("scl enable python27 bash")

  module = AnsibleModule(
      argument_spec = dict(
        systempwd       =dict(required=True),
        source_db_name  =dict(required=True),
        source_host     =dict(required=True),
        ignore          =dict(required=False)
      ),
      supports_check_mode=True,
  )

  # Get arguements passed from Ansible playbook
  vdbpass = module.params.get('systempwd')
  vdb     = module.params.get('source_db_name')
  vdbhost = module.params.get('source_host')
  vignore = module.params.get('ignore')

  if vignore is None:
      vignore = False

  if not cx_Oracle_found:
    debugg("cx_Oracle not found")
    module.fail_json(msg="cx_Oracle module not found")

  # check vars passed in are not NULL. All are needed to connect to source db
  if ( vdbpass is not None) and (vdb is not None) and (vdbhost is not None):

    try:
      vdb = vdb + vdbhost[-1:]
      dsn_tns2 = cx_Oracle.makedsn(vdbhost, '1521', vdb)
    except cx_Oracle.DatabaseError as exc:
      error, = exc.args
      debugg("failed on cx_Oracle.makedsn(vdbhost=%s, '1521', vdb=%s and dsn_tns2: %s)" % (vdbhost,vdb,dsn_tns2) )
      module.fail_json(msg='TNS generation error: %s, db name: %s host: %s' % (error.message, vdb, vdbhost), changed=False)

    try:
      con = cx_Oracle.connect('system', vdbpass, dsn_tns2)
    #except cx_Oracle.DatabaseError as exc:
    except cx_Oracle.DatabaseError as e:
      if vignore:
          debugg("err.msg=%s" % (e) )
          msg="DB CONNECTION FAILED"
          if debugme:
              msg = msg + " vignore: %s " % (vignore)
          module.exit_json(msg=msg, ansible_facts=ansible_facts, changed="False")
      else:
          raise
          #module.fail_json(msg='Database connection error: %s, tnsname: %s' % (error.message, vdb), changed=False)

    cur = con.cursor()

    # select source db version
    try:
      cur.execute('select version from v$instance')
    except cx_Oracle.DatabaseError as exc:
      error, = exc.args
      module.fail_json(msg='Error selecting version from v$instance, Error: %s' % (error.message), changed=False)

    dbver =  cur.fetchall()
    retver = dbver[0][0]
    usable_ver = ".".join(retver.split('.')[0:-1])
    ansible_facts[refname] = {'version': usable_ver, 'oracle_version_full': retver, 'major_version': usable_ver.split(".")[0]}

    # select host_name
    try:
      cur.execute('select host_name from v$instance')
    except cx_Oracle.DatabaseError as exc:
      error, = exc.args
      module.fail_json(msg='Error selecting host_name from v$instance, Error: %s' % (error.message), changed=False)

    vtemp = cur.fetchall()
    vtemp = vtemp[0][0]
    ansible_facts[refname]['host_name'] = vtemp

    # actual db_file count
    try:
      cur.execute('select count(*) from dba_data_files')
    except cx_Oracle.DatabaseError as exc:
      error, = exc.args
      module.fail_json(msg='Error selecting host_name from v$instance, Error: %s' % (error.message), changed=False)

    vtemp = cur.fetchall()
    vtemp = vtemp[0][0]
    ansible_facts[refname]['db_files_actual'] = vtemp

    # Find archivelog mode.
    try:
      cur.execute('select log_mode from v$database')
    except cx_Oracle.DatabaseError as exc:
      error, = exc.args
      module.fail_json(msg='Error selecting log_mode from v$database, Error: %s' % (error.message), changed=False)

    vtemp = cur.fetchall()
    vtemp = vtemp[0][0]
    if vtemp == 'ARCHIVELOG':
      vtemp = 'True'
    else:
      vtemp = 'False'

    ansible_facts[refname]['archivelog'] = vtemp

    # Get dbid for active db duplication without target, backup only
    try:
      cur.execute('select dbid from v$database')
    except cx_Oracle.DatabaseError as exc:
      error, = exc.args
      module.fail_json(msg='Error selecting dbid from v$database, Error: code : %s, message: %s, context: %s' % (error.code, error.message, error.context), changed=False)

    vtemp = cur.fetchall()
    vtemp = vtemp[0][0]
    ansible_facts[refname]['dbid'] = vtemp

    # Find ASM diskgroups used by the database
    try:
      cur.execute("select name from v$asm_diskgroup where state='CONNECTED' and name not like '%FRA%'")
    except cx_Oracle.DatabaseError as exc:
      error, = exc.args
      module.fail_json(msg='Error selecting name from v$asmdiskgroup, Error: %s' % (error.message), changed=False)

    vtemp = cur.fetchall()
    vtemp = vtemp[0][0]
    # diskgroups = [row[0] for row in cur.fetchall()]
    ansible_facts[refname]['diskgroups'] = vtemp #diskgroups

    # Open cursors - used in populating dynamic pfiles
    try:
      cur.execute("select value from v$parameter where name = 'open_cursors'")
    except cx_Oracle.DatabaseError as exc:
      error, = exc.args
      module.fail_json(msg='Error selecting value open_cursors, Error: %s' % (error.message), changed=False)

    vtemp = cur.fetchall()
    vtemp = vtemp[0][0]
    ansible_facts[refname]['open_cursors'] = vtemp

    # pga_aggregate_target - used in populating dynamic pfiles
    try:
      cur.execute("select value from v$parameter where name = 'pga_aggregate_target'")
    except cx_Oracle.DatabaseError as exc:
      error, = exc.args
      module.fail_json(msg='Error selecting value pga_aggregate_target, Error: %s' % (error.message), changed=False)

    vtemp = cur.fetchall()
    vtemp = vtemp[0][0]
    ansible_facts[refname]['pga_aggregate_target'] = vtemp

    # use_large_pages - used in populating dynamic pfiles
    try:
      cur.execute("select value from v$parameter where name = 'use_large_pages'")
    except cx_Oracle.DatabaseError as exc:
      error, = exc.args
      module.fail_json(msg='Error selecting value use_large_pages, Error: %s' % (error.message), changed=False)

    vtemp = cur.fetchall()
    vtemp = vtemp[0][0]
    ansible_facts[refname]['use_large_pages'] = vtemp

    # Is Block Change Tracking (BCT) enabled or disabled?
    try:
      cur.execute("select status from v$block_change_tracking")
    except cx_Oracle.DatabaseError as exc:
      error, = exc.args
      module.fail_json(msg='Error getting status of BCT, Error: %s' % (error.message), changed=False)

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
      module.fail_json(msg='Error selecting version from v$instance, Error: %s' % (error.message), changed=False)

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

    if usable_ver[:2] == "12":
        # Get sourcedb home if version 12+ otherwise doesn't work in 11g or lower:
        try:
          cur.execute("select SYS_CONTEXT ('USERENV','ORACLE_HOME') from dual")
        except cx_Oracle.DatabaseError as exc:
          error, = exc.args
          module.fail_json(msg='Error getting sourcedb home, Error: %s' % (error.message), changed=False)

        vtemp = cur.fetchall()
        vtemp = vtemp[0][0]
        if vtemp:
            ansible_facts[refname]['oracle_home'] = vtemp

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


    # See if dbainfo user/schema exists
    try:
      cur.execute("select 1 from dba_users where username = 'DBAINFO'")
    except cx_Oracle.DatabaseError as exc:
      error, = exc.args
      module.fail_json(msg='Error getting status of BCT, Error: %s' % (error.message), changed=False)

    vtemp = cur.fetchall()
    if cur.rowcount == 0:
        ansible_facts[refname].update({'dbainfo': {'exists': 'False' }} )
        ansible_facts[refname]['dbainfo'].update({'dba_work': 'False' })
    else:
        ansible_facts[refname].update({'dbainfo': {'exists': 'True'}} )

    # if dbainfo exists see if dba_work table exists
    if cur.rowcount == 1:

        try:
            cur.execute("select 1 from dba_objects where owner = 'DBAINFO' and object_name = 'DBAINFO'")
        except cx_Oracle.DatabaseError as exc:
            error, = exc.args
            module.fail_json(msg='Error getting status of BCT, Error: %s' % (error.message), changed=False)

        vtemp = cur.fetchall()
        if cur.rowcount == 0:
            ansible_facts[refname]['dbainfo'].update({'dba_work': 'False' } )
        else:
            ansible_facts[refname]['dbainfo'].update({'dba_work': 'True' } )


    # get parameters listed in the header of this program defined in "vparams"
    # for idx in range(len(vparams)):
    for a_param in vparams:
        try:
          v_sel = "select value from v$parameter where name = '%s'" % (a_param)
          cur.execute(v_sel)
        except cx_Oracle.DatabaseError as exc:
          error, = exc.args
          module.fail_json(msg='Error selecting name from v$asmdiskgroup, Error: %s' % (error.message), changed=False)

        vtemp = cur.fetchall()
        vtemp = vtemp[0][0]
        # if 'sga_target' == vparams[idx] or 'db_recovery_file_dest_size' == vparams[idx]:
        if 'sga_target' == a_param or 'db_recovery_file_dest_size' == a_param:
            vtemp = convert_size(float(vtemp),"K")
            # ansible_facts[refname][vparams[idx]] = vtemp
            ansible_facts[refname][a_param] = vtemp
        # elif 'db_recovery_file_dest' == vparams[idx]:
        elif 'db_recovery_file_dest' == a_param:
            # ansible_facts[refname][vparams[idx]] = vtemp[1:]
            ansible_facts[refname][a_param] = vtemp[1:]
        # elif 'listener' in vparams[idx]:
        elif 'listener' in a_param:
            head, sep, tail = vtemp.partition('.')
            # ansible_facts[refname][vparams[idx]] = head
            ansible_facts[refname][a_param] = head
        else:
            # ansible_facts[refname][vparams[idx]] = vtemp
            ansible_facts[refname][a_param] = vtemp

    try:
        cur.close()
    except cx_Oracle.DatabaseError as exc:
      error, = exc.args
      module.fail_json(msg='Error getting status of BCT, Error: %s' % (error.message), changed=False)

    msg="Custom module sourcefacts succeeded"

    vchanged="False"

  # if parameters were NULL return informative error.
  else:

    msg="Custom module sourcefacts Failed"
    # sourcefacts={}
    if module.params['systempwd'] is None:
      ansible_facts['systempwd'] = 'missing'
    else:
      ansible_facts['systempwd'] = 'ok'

    if module.params['source_db_name'] is None:
      ansible_facts['source_db_name'] = 'missing'
    else:
      ansible_facts['source_db_name'] = 'ok'

    if module.params['source_host'] is None:
      ansible_facts['source_host'] = 'missing'
    else:
      ansible_facts['source_host'] = 'ok'

    vchanged="False"

  # print json.dumps( ansible_facts_dict )
  module.exit_json( msg=msg, ansible_facts=ansible_facts , changed=vchanged)

# code to execute if this program is called directly
if __name__ == "__main__":
   main()
