#!/opt/rh/python27/root/usr/bin/python
# scl enable python27 bash
# export LD_LIBRARY_PATH=$ORACLE_HOME/lib:$LD_LIBRARY_PATH
#
# Module for Ansible to retrieve Oracle RMAN facts from a host about a particular database.
#
#
# Written by : Cru Ansible Module development team
#
#  To use your cutom module via command line pass it in to the playbook using:
#  --module-path custom_modules
#
# When the module is ready for deployment, put it in the module libarary
#
# This module will get Oracle RMAN information from an Oracle database server
#
# For programming:
# ansible-playbook restore_db.yml -i cru_inventory --extra-vars="hosts=test_rac dest_db_name=testdb source_host=plorad01 source_db_name=jfprod" --tags "rmanfacts" --step -vvv
#
# The Data collection to include: (to be checked off when implemented)
#  [ ]  1) list of spfile backups with metadata # this will tell how far back the database can be recovered to when using spfile recovery
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


ANSIBLE_METADATA = {'status': ['stableinterface'],
                    'supported_by': 'Cru DBA team',
                    'version': '0.3'}

DOCUMENTATION = '''
---
module: rmanfacts
short_description: Collect Oracle RMAN metadata on a remote host for a given db.
notes: Returned values are then available to use in Ansible.
requirements: [ python2.* ]
author: "DBA Oracle module Team"
'''

EXAMPLES = '''

  - name: Gather RMAN backup facts for database
    rmanfacts:
      rco: "{{ database_passwords['rman'].rco }}"
      dbid: "{{ sourcefacts['dbid'] }}"
      source_db: "{{ source_db_name }}"
      bu_type: spfile
      home: "{{ oracle_home }}"
      stage: "{{ oracle_stage }}"

      Breakdown:
        rco: rman database password
        dbid: can run on destination host when dbid is used
        db_name: name of source database
        type: type of backup information to retrieve. for this restore spfile backups are needed.


'''
ora_home = ''
ora_sid = ''
err_msg=''
v_rec_count=0
vrco=''
vdbid=''
vsrcdb=''
vbu_type=''
vohome=''
vstage=''


def get_spfile_info ():
    """Return a list of spfile backups including Level (LV), Month, Day, Year and time of day"""
    global err_msg
    spfile_bus={}
    velement=[]
    vcounter=0

    # create a list of spfile backups and write to a file:
    try:
        # v_bu_list=str(commands.getstatusoutput("export ORACLE_SID=" + vsrcdb + "1;" + "export ORACLE_HOME=" + vohome + "; echo 'list backup of spfile summary;' | " + vohome + "/bin/rman catalog rco/" + vrco + "@cat target /")[1])
        v_bu_list=str(commands.getstatusoutput("export ORACLE_SID=" + vsrcdb + "1; export ORACLE_HOME=" + vohome + "; echo 'set dbid = " + vdbid + "; list backup of spfile summary;' | " + vohome + "/bin/rman catalog rco/" + vrco + "@cat target /")[1])
    except:
        err_msg = err_msg + ' Error: get_spfile_info () retrieving spfiile bu summary : (%s)' % (sys.exc_info()[0])

    try:
        for line in v_bu_list.split('\n'):
            if line.split(' ')[0][0].isdigit():
                vcounter += 1
                spfile_bus.update({ "bu_no": vcounter,
                                    "key": line.split(' ')[0],
                                    "type": line.split(' ')[1],
                                    "level": line.split(' ')[2],
                                    "status": line.split(' ')[3],
                                    "device": line.split(' ')[4],
                                    "compl_time": line.split(' ')[5] + line.split(' ')[6] + line.split(' ')[7] + line.split(' ')[8],
                                    "pieces": line.split(' ')[9],
                                    "copies": line.split(' ')[10],
                                    "compressed": line.split(' ')[11],
                                    "tag": line.split(' ')[12] })
    except:
        err_msg = err_msg + ' Error: get_spfile_info () reading lines from rman_info2.log file: (%s)' % (sys.exc_info()[0])

    # vtmp=type(spfile_bus)
    return(spfile_bus)

# ==============================================================================
# ================================== Main ======================================
# ==============================================================================

def main(argv):
  global ora_home
  global err_msg
  global v_rec_count
  global vrco
  global vdbid
  global vsrcdb
  global vbu_type
  global vohome
  global vstage

  ansible_facts={ 'rmanfacts': {} }

  module = AnsibleModule(
      argument_spec = dict(
        rcopwd          =dict(required=True),
        dbid            =dict(required=True),
        source_db       =dict(required=True),
        bu_type         =dict(required=True),
        ohome           =dict(required=True),
        stage           =dict(required=True)
      ),
      supports_check_mode=True,
  )

  # Get arguements passed from Ansible playbook
  vrco = module.params.get('rcopwd')
  vdbid = module.params.get('dbid')
  vsrcdb = module.params.get('source_db')
  vbu_type = module.params.get('bu_type')
  vohome = module.params.get('ohome')
  vstage = module.params.get('stage')


if (not vrco and not vdbid and not vsrcdb and not vbu_type and not vohome and not vstage):

      vtmp=get_spfile_info()

      ansible_facts['rmanfacts']['spfile'] = vtmp

      # Add any error messages caught before passing back
      if err_msg:
        msg = msg + err_msg

      module.exit_json( msg=msg , ansible_facts=ansible_facts , changed="False")

      sys.exit(0)

else:
      msg="\nError retrieving RMAN backup information for " + vbu_type


      msg = msg + err_msg

      module.fail_json( msg=msg )

      sys.exit(1)

# code to execute if this program is called directly
if __name__ == "__main__":
   main(sys.argv)
