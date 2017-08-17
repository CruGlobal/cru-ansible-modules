#!/opt/rh/python27/root/usr/bin/python

from ansible.module_utils.basic import *
from ansible.module_utils.facts import *
import commands
import subprocess
import sys
import os
import json
import re
import cx_Oracle
import math

# Notes: IAW this doc : http://docs.ansible.com/ansible/latest/dev_guide/developing_modules_general.html
# This module was setup to return a dictionary called "ansible_facts" which then makes those facts usable
# in the ansible playbook, and roles. The facts in this module are referenced by using the format:
#                    source_db_facts['key'] which returns associated value - the ref name : "source_db_facts" was created in this module
#     example:    {{ source_db_facts['oracle_version'] }} => 11.2.0.4

ANSIBLE_METADATA = {'status': ['stableinterface'],
                    'supported_by': 'Cru DBA team',
                    'version': '0.1'}

DOCUMENTATION = '''
---
module: srcdbfacts
short_description: Get Oracle Database facts from a remote database.
(remote database = a database not in the group being operated on)

notes: Returned values are then available to use in Ansible.
requirements: [ python2.* ]
author: "DBA Oracle module Team"
'''

EXAMPLES = '''

    # if cloning and source database information is desired
    - local_action: srcdbfacts
        systempwd="{{ database_passwords[source_db_name].system }}"
        source_db_name="{{ source_db_name }}"
        source_host="{{ source_host }}"
      become_user: "{{ local_user }}"
      register: source_db_facts

'''

# source_db_facts={}  # define a python dictionary to pass facts back to Ansible

# Parameters we're just retriveing from v$parameter table
# NOTE: IF YOU ADD A PARAMETER: DON'T FORGET THE COMMA AND QUOTES!
vparams=[ "compatible", "sga_target", "db_recovery_file_dest", "db_recovery_file_dest_size", "diagnostic_dest", "remote_listener" ]

def convert_size(size_bytes):

   if size_bytes == 0:
       return "0B"

   size_name = ("B", "K", "M", "G", "T", "P", "E", "Z", "Y")
   i = int(math.floor(math.log(size_bytes, 1024)))
   p = math.pow(1024, i)
   s = round(size_bytes / p, 1)
   return "%s%s" % (int(round(s)), size_name[i])


# =================================== MAIN =====================================
def main ():
  """ Return Oracle database parameters from a database not in the specified group"""
  ansible_facts={}

  os.system("/usr/bin/scl enable python27 bash")
  # os.system("scl enable python27 bash")

  module = AnsibleModule(
      argument_spec=dict(
        systempwd=dict(required=True),
        source_db_name=dict(required=True),
        source_host=dict(required=True)
      ),
      supports_check_mode=True,
  )

  # define dictionary obj to return from this module
  ansible_facts_dict={
     "changed": False,
     "msg": "",
     "ansible_facts": {}
  }

  # get the vars that are needed to connect to the source db
  vdbpass = module.params.get('systempwd')
  vdb = module.params.get('source_db_name') + '1'
  vdbhost = module.params.get('source_host') # + '.ccci.org'

  # check vars passed in are not NULL. All are needed to connect to source db
  if ( vdbpass is not None) and (vdb is not None) and (vdbhost is not None) :

    dsn_tns2 = cx_Oracle.makedsn(vdbhost, '1521', vdb)
    db2 = cx_Oracle.connect('system', vdbpass, dsn_tns2)
    cur = db2.cursor()

    # Retrieve source db version
    cur.execute('select version from v$instance')
    tempver = cur.fetchall()
    retver = tempver[0][0]
    usable_ver = ".".join(retver.split('.')[0:-1])

    ansible_facts['source_db_facts'] = {'oracle_version': usable_ver, 'oracle_version_full': retver}

    meta_msg = ''

    for idx in range(len(vparams)):
        v_sel = "select value from v$parameter where name = '" + vparams[idx] + "'"
        cur.execute(v_sel)
        vtemp = cur.fetchall()
        vtemp=vtemp[0][0]
        if 'sga_target' == vparams[idx] or 'recovery_file_dest_size' in vparams[idx]:
            vtemp = convert_size(float(vtemp))
            ansible_facts['source_db_facts'][vparams[idx]] = vtemp
        elif 'listener' in vparams[idx]: # special handling of remote_listener - strip out just test-scan
            head, sep, tail = vtemp.partition('.')
            ansible_facts['source_db_facts'][vparams[idx]] = head
        else:
            ansible_facts['source_db_facts'][vparams[idx]] = vtemp

    msg="srcdbfacts succeeded" # , Please note: parameters " + meta_msg + " sizes in GB"

    vchanged="False"

  # if parameters were NULL return informative error.
  else:

    msg="Module srcdbfacts Failed"
    # source_db_facts={}
    if module.params['systempwd'] is None:
      # source_db_facts['systempwd'] = 'missing'
      ansible_facts['systempwd'] = 'missing'
    else:
      # source_db_facts['systempwd'] = 'passed'
      ansible_facts['systempwd'] = 'passed'

    if module.params['source_db_name'] is None:
      # source_db_facts['source_db_name'] = 'missing'
      ansible_facts['source_db_name'] = 'missing'
    else:
      # source_db_facts['source_db_name'] = 'passed'
      ansible_facts['source_db_name'] = 'passed'

    if module.params['source_host'] is None:
      # source_db_facts['source_host'] = 'missing'
      ansible_facts['source_host'] = 'missing'
    else:
      # source_db_facts['source_host'] = 'passed'
      ansible_facts['source_host'] = 'passed'

    vchanged="False"


  ansible_facts_dict = {
     "changed" : vchanged,
     "msg": msg,
     "ansible_facts": {}
  }

  # print json.dumps( ansible_facts_dict )
  module.exit_json( msg=msg, ansible_facts=ansible_facts , changed=vchanged)

# code to execute if this program is called directly
if __name__ == "__main__":
   main()
