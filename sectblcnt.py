#!/opt/rh/python27/root/usr/bin/python

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
# Date: May 20, 2019

ANSIBLE_METADATA = {'status': ['stableinterface'],
                    'supported_by': 'Cru DBA team',
                    'version': '0.1'}

DOCUMENTATION = '''
---
module: psadmsectblcnt
short_description: PS Admin Security Table Count

notes: Returned the value of security tables in the PS Admin schema
requirements: [ python2.* ]
author: "DBA Oracle module Team"
'''

EXAMPLES = '''

    # if cloning a database and source database information is desired
    - local_action:
        module: psadmsectblcnt
        ps_admin: "{{ ps_admin }}"
        systempwd: "{{ database_passwords[source_db_name].system }}"
        db_name: "{{ dest_db_name }}"
        host: "{{ dest_host }}"
        oracle_home: "{{ oracle_home }}" (2)
        refname: "{{ refname_str }} (3)"
        ignore: True (4)
      become_user: "{{ utils_local_user }}"
      register: sec_tbl_count

      (3) refname - name used in Ansible to reference these facts ( i.e. sourcefacts, destfacts, sysdbafacts )

      (4) ignore - (connection errors) is optional. If you know the source
          database may be down set ignore: True. If connection to the
          source database fails the module will not throw a fatal error
          to stop the play and continue.

   NOTE: these modules can be run with the when: master_node statement.
         However, their returned values cannot be referenced in
         roles or tasks later. Therefore, when running fact collecting modules,
         run them on both nodes. Do not use the "when: master_node" clause.

'''

def_ref_name = "sectblcount"
secTblList = ('PSACCESSPROFILE','PSOPRDEFN')
msg = ""

def add_to_msg(inStr):
    """Add strings to msg"""
    global msg
    if inStr:
        msg = msg + " " + inStr
    else:
        msg = inStr

# ==============================================================================
# =================================== MAIN =====================================
# ==============================================================================
def main ():
  """ Return the number of security tables owned by the PS Admin schema"""

  global msg
  global def_ref_name
  global secTblList
  debugme = True
  vchanged = False

  ansible_facts={}

  # Name to call facts dictionary being passed back to Ansible
  # This will be the name you reference in Ansible. i.e. source_facts['sga_target'] (source_facts)
  refname = ""

  os.system("/usr/bin/scl enable python27 bash")
  # os.system("scl enable python27 bash")

  module = AnsibleModule(
      argument_spec = dict(
        ps_admin        =dict(required=True),
        systempwd       =dict(required=True),
        db_name         =dict(required=True),
        host            =dict(required=True),
        refname         =dict(required=False),
        ignore          =dict(required=False)
      ),
      supports_check_mode=False,
  )

  # Get arguements passed from Ansible playbook
  vpsadmin  = module.params.get('ps_admin')
  vdbpass   = module.params.get('systempwd')
  vdb       = module.params.get('db_name')
  vdbhost   = module.params.get('host')
  vrefname  = module.params.get('refname')
  vignore   = module.params.get('ignore')

  if vignore is None:
      vignore = False

  if '.org' in vdbhost:
    vdbhost = vdbhost.replace('.ccci.org','')

  if not cx_Oracle_found:
    module.fail_json(msg="Error: cx_Oracle module not found")

  if not vrefname:
    refname = def_ref_name
  else:
    refname = vrefname

  # check vars passed in are not NULL. All are needed to connect to source db
  if ( vdbpass is not None) and (vdb is not None) and (vdbhost is not None) and (vpsadmin is not None):

        try:
          vdb = vdb + vdbhost[-1:]
          dsn_tns = cx_Oracle.makedsn(vdbhost, '1521', vdb)
        except cx_Oracle.DatabaseError as exc:
          error, = exc.args
          if vignore:
              msg = "Failed to create dns_tns: %s" %s (error.message)
          else:
              module.fail_json(msg='TNS generation error: %s, db name: %s host: %s' % (error.message, vdb, vdbhost), changed=False)

        try:
          con = cx_Oracle.connect('system', vdbpass, dsn_tns)
        except cx_Oracle.DatabaseError as exc:
          error, = exc.args
          if vignore:
              add_to_msg("DB CONNECTION FAILED : %s" % (error.message))
              if debugme:
                  add_to_msg(" vignore: %s " % (vignore))
              module.exit_json(msg=msg, ansible_facts=ansible_facts, changed="False")
          else:
              module.fail_json(msg='Database connection error: %s, tnsname: %s host: %s' % (error.message, vdb, vdbhost), changed=False)

        cur = con.cursor()

        # select source db version
        try:
            cmd_str = 'select count(*) from dba_objects where owner = \'%s\' and object_type = \'TABLE\' and object_name in %s' % (vpsadmin.upper(),str(secTblList))
            if debugme:
                add_to_msg("cmd_str = %s" % (cmd_str))
            cur.execute(cmd_str)
        except cx_Oracle.DatabaseError as exc:
            error, = exc.args
            add_to_msg('Error selecting version from v$instance, Error: %s' % (error.message))
            module.fail_json(msg=msg, changed=False)

        dbver =  cur.fetchall()
        vsectblecnt = dbver[0][0]
        ansible_facts[refname] = { vpsadmin: { 'security_table_count':vsectblecnt } }

        add_to_msg("module completed successfully.")

        module.exit_json( msg=msg, ansible_facts=ansible_facts , changed=vchanged)

  else:

        if vdb is None:
            add_to_msg("Required parameter database name not defined.")

        if vdbhost is None:
            add_to_msg("Required parameter host not defined.")

        if vdbpass is None:
            add_to_msg("Required database password not defined.")

        if vpsadmin is None:
            add_to_msg("Required PS Admin not defined.")

        add_to_msg('Error closing cursor: Error: %s' % (msg))

        module.fail_json(msg=msg, changed=False)

# code to execute if this program is called directly
if __name__ == "__main__":
   main()
