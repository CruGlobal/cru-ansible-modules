#!/opt/rh/python27/root/usr/bin/python
# scl enable python27 bash
# export LD_LIBRARY_PATH=$ORACLE_HOME/lib:$LD_LIBRARY_PATH
#
# Module for Ansible to execute srvctl commands
#
#
# Written by : Cru Ansible Module development team
#
#  To use your cutom module via command line pass it in to the playbook using:
#  --module-path custom_modules
#
# When the module is ready for deployment, put it in the module libarary
#
# This module will execute Oracle srvctl commands on an Oracle host
#
# For programming:
# ansible-playbook restore_db.yml -i cru_inventory --extra-vars="hosts=test_rac dest_db_name=testdb source_host=plorad01 source_db_name=jfprod" --tags "srvctl" --step -vvv
#
# The srvctl functionality to include: (to be checked off when implemented)
#  [ ]  1) srvctl start database
#
# This file is part of Ansible
#
# Ansible is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# Ansible is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with Ansible.  If not, see <http://www.gnu.org/licenses/>.
# [..] various imports
# from ansible.module_utils.basic import AnsibleModule
#
# Last updated Thursday 22, 2018    Sam Kohler
#
from ansible.module_utils.basic import *
from ansible.module_utils.facts import *
import commands
import json
import sys
import os
import os.path
import subprocess
from subprocess import PIPE,Popen
import re
from datetime import datetime
from datetime import timedelta

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
    - name: start database
      srvctl:
        cmd: start
        obj: database
         db: tstdb
      become_user: "{{ remote_user }}"
      register: src_facts

'''
def get_orahome(vdb):


def db_status(vdb):
    """Return the status of the database"""

    try:
      # v_bu_list=str(commands.getstatusoutput("export ORACLE_SID=" + vsrcdb + "1;" + "export ORACLE_HOME=" + vohome + "; echo 'list backup of spfile summary;' | " + vohome + "/bin/rman catalog rco/" + vrco + "@cat target /")[1])
      v_bu_list=str(commands.getstatusoutput("export ORACLE_HOME=" + vohome + "; eexport ORACLE_SID = " + vdbid + "; list backup of spfile summary;' | " + vohome + "/bin/rman catalog rco/" + vrco + "@cat target /")[1])
    except:
        err_msg = err_msg + ' Error: spfile_bu() retrieving spfiile bu summary : (%s)' % (sys.exc_info()[0])

# ==============================================================================
# =================================== MAIN =====================================
# ==============================================================================
def main ():
  """ Execute srvctl commands """
  ansible_facts={}

  # Name to call facts dictionary being passed back to Ansible
  # This will be the name you reference in Ansible. i.e. source_facts['sga_target'] (source_facts)
  refname = 'srvctl'

  os.system("/usr/bin/scl enable python27 bash")
  # os.system("scl enable python27 bash")

  module = AnsibleModule(
      argument_spec  = dict(
        cmd        =dict(required=True),
        obj        =dict(required=True),
        db         =dict(required=True)
      ),
      supports_check_mode=True,
  )



  try:
    dsn_tns2 = cx_Oracle.makedsn(vdbhost, '1521', vdb)
  except cx_Oracle.DatabaseError, exception:
    error, = exception.args
    module.fail_json(msg='TNS generation error: %s, db name: %s host: %s' % (error.message, vdb, vdbhost), changed=False)
