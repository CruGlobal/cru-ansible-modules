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

notes: Returned values are then available to use in Ansible.
requirements: [ python2.* ]
author: "DBA Oracle module Team"
'''

EXAMPLES = '''

    # if cloning a database and source database information is desired
    - local_action:
        module: rmancat
        systempwd: "{{ database_passwords[source_db_name].system }}"
        dbid: {{ sourcefacts['dbid'] }}
        cdb: "cat"
        pdb: "catcdb"
        schema_owner: rco
        host: "{{ source_host }}"
      register: rmancat_facts

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

msg = ""
debugme = False
# defaults
d_schema_owner = "rco"
d_cdb = "cat"
d_pdb = "catcdb"
d_domain = ".ccci.org"


# ==============================================================================
# =================================== MAIN =====================================
# ==============================================================================
def main ():
  """ Return Oracle database parameters from a database not in the specified group"""
  global msg
  ansible_facts={}

  # Name to call facts dictionary being passed back to Ansible
  # This will be the name you reference in Ansible. i.e. source_facts['sga_target'] (source_facts)
  refname = "rmancat"

  os.system("/usr/bin/scl enable python27 bash")
  # os.system("scl enable python27 bash")

  module = AnsibleModule(
      argument_spec = dict(
        systempwd         =dict(required=True),
        db_name           =dict(required=True),
        dbid              =dict(required=True),
        cdb               =dict(required=False),
        pdb               =dict(required=False),
        schema_owner      =dict(required=False),
        host              =dict(required=True)
      ),
      supports_check_mode=True,
  )

  # Get arguements passed from Ansible playbook
  v_syspwd       = module.params.get('systempwd')
  p_db_name      = module.params.get('db_name')
  p_dbid         = module.params.get('dbid')
  p_cdb          = module.params.get('cdb')
  p_pdb          = module.params.get('pdb')
  p_schema_owner = module.params.get('schema_owner')
  v_host         = module.params.get('host')

  if not cx_Oracle_found:
    module.fail_json(msg="Error: cx_Oracle module not found")

  if p_cdb is None:
      v_cdb = d_cdb
  else:
      v_cdb = p_cdb

  if p_pdb is None:
      v_pdb = d_pdb
  else:
      v_pdb = p_pdb

  if p_schema_owner is None:
      v_schema_owner = d_schema_owner
  else:
      v_schema_owner = p_schema_owner

  if p_dbid:
      v_dbid = p_dbid

  if p_db_name:
      v_db_name = p_db_name

  # check vars passed in are not NULL. All are needed to connect to source db
  if ( v_syspwd is not None ) and ( v_host is not None ) and ( v_cdb is not None ) and ( v_pdb is not None ) and (v_dbid is not None):

    if v_pdb[-1].isdigit():
        v_pdb = v_pdb[:-1]

    if d_domain in v_host:
        v_host = v_host.replace(d_domain,'')

    try:
        dsn_tns = cx_Oracle.makedsn(v_host, '1521', v_pdb)
    except cx_Oracle.DatabaseError as exc:
        error, = exc.args
        module.fail_json(msg='TNS generation error: %s, db name: %s host: %s' % (error.message, v_pdb, vdbhost), changed=False)

    try:
        con = cx_Oracle.connect('system', v_syspwd, dsn_tns)
    except cx_Oracle.DatabaseError as exc:
        error, = exc.args
        module.fail_json(msg='Database connection error: %s, tnsname: %s, pdb %s' % (error.message, dsn_tns, v_pdb), changed=False)

    cur = con.cursor()

    try:
        cmd_str = "SELECT CASE WHEN MAX(object_name) IS NULL THEN 'False' ELSE 'True' END table_exists FROM dba_objects WHERE object_name = '%s'" % ('v$pdbs')
        cur.execute(cmd_str)
    except cx_Oracle.DatabaseError as exc:
        error, = exc.args
        module.fail_json(msg='Error determining if v_pdbs table exists, Error: %s' % (error.message), changed=False)

    vtemp = cur.fetchall()
    vpuggable_db = vtemp[0][0]

    if vpuggable_db:
        # set container database
        try:
            cmd_str = "alter session set container = %s" % (v_cdb.upper())
            cur.execute(cmd_str)
        except cx_Oracle.DatabaseError as exc:
            error, = exc.args
            module.fail_json(msg='Error setting container, Error: %s container: %s' % (error.message,v_cdb.upper()), changed=False)

    # Is database registered with RMAN
    try:
        cmd_str = "SELECT CASE WHEN MAX(dbid) IS NULL THEN 'False' ELSE 'True' END db_registered FROM %s.rc_database where name = '%s' and dbid = '%s'" % (v_schema_owner,v_db_name.upper(),v_dbid)
        cur.execute(cmd_str)
    except cx_Oracle.DatabaseError as exc:
        error, = exc.args
        module.fail_json(msg='Error selecting host_name from v$instance, Error: %s' % (error.message), changed=False)

    vtemp = cur.fetchall()
    vtemp = vtemp[0][0]
    ansible_facts[refname]= { v_db_name: { 'registered': vtemp, 'dbid': v_dbid} }

    try:
        cur.close()
    except cx_Oracle.DatabaseError as exc:
      error, = exc.args
      module.fail_json(msg='Error getting status of BCT, Error: %s' % (error.message), changed=False)

    msg="Custom module rmancat succeeded for pluggable database: %s and container: %s ." % (v_pdb,v_cdb)

    vchanged="False"

    # print json.dumps( ansible_facts_dict )
    module.exit_json( msg=msg, ansible_facts=ansible_facts , changed=vchanged)

  # if parameters were NULL return informative error.
  else:

    msg="Custom module rmancat Failed"

    if module.params['system_pwd'] is None:
        ansible_facts['system_pwd'] = 'missing'
    else:
        ansible_facts['system_pwd'] = 'ok'

    if module.params['cdb'] is None:
        ansible_facts['cdb'] = 'missing'
    else:
        ansible_facts['cdb'] = 'ok'

    if module.params['pdb'] is None:
        ansible_facts['pdb'] = 'missing'
    else:
        ansible_facts['pdb'] = 'ok'

    if module.params['host'] is None:
        ansible_facts['host'] = 'missing'
    else:
        ansible_facts['host'] = 'ok'

    vchanged="False"

  # print json.dumps( ansible_facts_dict )
  module.exit_json( msg=msg, ansible_facts=ansible_facts , changed=vchanged)

# code to execute if this program is called directly
if __name__ == "__main__":
   main()
