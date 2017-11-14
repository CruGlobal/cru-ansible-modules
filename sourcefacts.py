#!/opt/rh/python27/root/usr/bin/python

from ansible.module_utils.basic import *
from ansible.module_utils.facts import *
import commands
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

# Name to call facts dictionary being passed back to Ansible
# This will be the name you reference in Ansible. i.e. source_facts['sga_target'] (source_facts)
refname = 'sourcefacts'

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

    # if cloning and source database information is desired
    - local_action: sourcefacts
        systempwd="{{ database_passwords[source_db_name].system }}"
        source_db_name="{{ source_db_name }}"
        source_host="{{ source_host }}"
      become_user: "{{ remote_user }}"
      register: src_facts

'''

# Parameters we're just retriveing from v$parameter table
vparams=[ "compatible", "sga_target", "db_recovery_file_dest", "db_recovery_file_dest_size", "diagnostic_dest", "remote_listener", "db_unique_name", "db_block_size"]


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
  ansible_facts={}

  os.system("/usr/bin/scl enable python27 bash")
  # os.system("scl enable python27 bash")

  module = AnsibleModule(
      argument_spec = dict(
        systempwd       =dict(required=True),
        source_db_name  =dict(required=True),
        source_host     =dict(required=True)
      ),
      supports_check_mode=True,
  )

  # Get arguements passed from Ansible playbook
  vdbpass = module.params.get('systempwd')
  vdb = module.params.get('source_db_name') + '1'
  vdbhost = module.params.get('source_host') # + '.ccci.org'

  if not cx_Oracle_found:
    module.fail_json(msg="cx_Oracle module not found")

  # check vars passed in are not NULL. All are needed to connect to source db
  if ( vdbpass is not None) and (vdb is not None) and (vdbhost is not None):

    try:
      dsn_tns2 = cx_Oracle.makedsn(vdbhost, '1521', vdb)
    except cx_Oracle.DatabaseError, exception:
      error, = exception.args
      module.fail_json(msg='TNS generation error: %s, db name: %s host: %s' % (error.message, vdb, vdbhost), changed=False)

    try:
      con = cx_Oracle.connect('system', vdbpass, dsn_tns2)
    except cx_Oracle.DatabaseError, exception:
      error, = exception.args
      module.fail_json(msg='Database connection error: %s, tnsname: %s' % (error.message, vdb), changed=False)

    cur = con.cursor()

    # select source db version
    try:
      cur.execute('select version from v$instance')
    except cx_Oracle.DatabaseError, exception:
      error, = exception.args
      module.fail_json(msg='Error selecting version from v$instance, Error: %s' % (error.message), changed=False)

    dbver =  cur.fetchall()
    retver = dbver[0][0]
    usable_ver = ".".join(retver.split('.')[0:-1])
    ansible_facts[refname] = {'oracle_version': usable_ver, 'oracle_version_full': retver}

    # select host_name
    try:
      cur.execute('select host_name from v$instance')
    except cx_Oracle.DatabaseError, exception:
      error, = exception.args
      module.fail_json(msg='Error selecting host_name from v$instance, Error: %s' % (error.message), changed=False)

    vtemp = cur.fetchall()
    vtemp = vtemp[0][0]
    ansible_facts[refname]['host_name'] = vtemp

    # Find archivelog mode.
    try:
      cur.execute('select log_mode from v$database')
    except cx_Oracle.DatabaseError, exception:
      error, = exception.args
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
    except cx_Oracle.DatabaseError, exception:
      error, = exception.args
      module.fail_json(msg='Error selecting dbid from v$database, Error: code : %s, message: %s, context: %s' % (error.code, error.message, error.context), changed=False)

    vtemp = cur.fetchall()
    vtemp = vtemp[0][0]
    ansible_facts[refname]['dbid'] = vtemp

    # Find ASM diskgroups used by the database
    try:
      cur.execute("select name from v$asm_diskgroup where state='CONNECTED' and name not like '%FRA%'")
    except cx_Oracle.DatabaseError, exception:
      error, = exception.args
      module.fail_json(msg='Error selecting name from v$asmdiskgroup, Error: %s' % (error.message), changed=False)

    vtemp = cur.fetchall()
    vtemp = vtemp[0][0]
    # diskgroups = [row[0] for row in cur.fetchall()]
    ansible_facts[refname]['diskgroups'] = vtemp #diskgroups

    # Is Block Change Tracking (BCT) enabled or disabled?
    try:
      cur.execute("select status from v$block_change_tracking")
    except cx_Oracle.DatabaseError, exception:
      error, = exception.args
      module.fail_json(msg='Error getting status of BCT, Error: %s' % (error.message), changed=False)

    vtemp = cur.fetchall()
    vtemp = vtemp[0][0]
    ansible_facts[refname]['bct_status'] = vtemp


    meta_msg = ''

    # See if master_notes table exists
    # try:
    #   cur.execute("select 1 from all_objects where object_name like 'MASTER_NOTE%'")
    # except cx_Oracle.DatabaseError, exception:
    #   error, = exception.args
    #   module.fail_json(msg='Error selecting master_notes from v$instance, Error: %s' % (error.message), changed=False)
    #
    # vtemp = cur.fetchall()
    # if cur.rowcount == 0:
    #     ansible_facts[refname]['master_notes'] = "False"
    # else:
    #     ansible_facts[refname]['master_notes'] = "True"

    # get parameters listed in the header of this program defined in "vparams"
    for idx in range(len(vparams)):
        try:
          v_sel = "select value from v$parameter where name = '" + vparams[idx] + "'"
          cur.execute(v_sel)
        except cx_Oracle.DatabaseError, exception:
          error, = exception.args
          module.fail_json(msg='Error selecting name from v$asmdiskgroup, Error: %s' % (error.message), changed=False)

        vtemp = cur.fetchall()
        vtemp = vtemp[0][0]
        if 'sga_target' == vparams[idx] or 'db_recovery_file_dest_size' == vparams[idx]:
            vtemp = convert_size(float(vtemp),"M")
            ansible_facts[refname][vparams[idx]] = vtemp
        elif 'db_recovery_file_dest' == vparams[idx]:
            ansible_facts[refname][vparams[idx]] = vtemp[1:]
        elif 'listener' in vparams[idx]:
            head, sep, tail = vtemp.partition('.')
            ansible_facts[refname][vparams[idx]] = head
        else:
            ansible_facts[refname][vparams[idx]] = vtemp

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
