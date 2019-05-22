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

    # if refreshing this will check that the defined list of PS security tables
    # was saved off to the ps_admin (Asiu) for datapump export prior to
    # deleting the database, so that they can be restored after refresh.

    - local_action:
        module: psadmsectblcnt
        ps_admin: "{{ ps_admin }}" (1)
        table_list: "{{ security_table_list }}" (1)
        systempwd: "{{ database_passwords[source_db_name].system }}"
        db_name: "{{ dest_db_name }}"
        host: "{{ dest_host }}"
        refname: "{{ refname_str }}" (2)
        ignore: True (3)
      become: yes
      become_user: "{{ utils_local_user }}"
      register: sec_tbl_count
      when: master_node

      (1) ps_admin, table_list and num_sec_tables - are defined in
          vars/utils/utils_env.yml
          num_sec_tables is used after the count is obtained to fail if
          the count is less than expected.
          Fail when:
          - sectblcount[ps_admin]['security_table_count'] < num_sec_tables

      (2) refname - can be defined to refer to the output later. The default
          is 'sectblcount' ( see above Fail when statement )
          but the user can define anything.

      (3) ignore - (connection errors) is optional. If you know the source
          database may be down set ignore: True. If connection to the
          source database fails the module will not throw a fatal error
          to stop the play and continue. However, not if the result is critical.


'''

def_ref_name = "sectblcount"
msg = ""
debugme = False

def add_to_msg(inStr):
    """Add strings to msg"""
    global msg

    if inStr:
        msg = msg + " " + inStr
    else:
        msg = inStr


def debugg(inStr):
    """If debugme is True add debugging info to msg"""
    if debugme:
        add_to_msg(inStr)


def convertToTuple(inStr):
    """Convert table_list parameter string to tuple"""
    tmp = inStr.split(",")
    return(tuple(tmp))

# ==============================================================================
# =================================== MAIN =====================================
# ==============================================================================
def main ():
  """ Return the number of security tables owned by the PS Admin schema"""

  global msg
  global def_ref_name
  global debugme

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
        table_list      =dict(required=True),
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
  vtblList  = module.params.get('table_list')
  vdb       = module.params.get('db_name')
  vdbhost   = module.params.get('host')
  vrefname  = module.params.get('refname')
  vignore   = module.params.get('ignore')
  vdebugme  = module.params.get('debugme')

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

  if vtblList is None:
    module.fail_json(msg="Error: a string containing tables must be provided: \"table1,table2\" etc. ")
  else:
    secTblList = convertToTuple(vtblList)

  debugg("secTblList = %s" % (str(secTblList)))

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

        debugg("about to run select for secTblList = %s parameter table list vtblList = %s " % (secTblList, vtblList))
        # select source db version
        try:
            cmd_str = 'select count(*) from dba_objects where owner = \'%s\' and object_type = \'TABLE\' and object_name in %s' % (vpsadmin.upper(),str(secTblList))
            debugg("cmd_str = %s" % (cmd_str))
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
