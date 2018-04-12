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
from datetime import datetime
from datetime import timedelta


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

err_msg=''
v_rec_count=0
vrco=''
vdbid=''
vsrcdb=''
vbu_type=''
vohome=''
vstage=''


def spfile_bu():
  """Return a list of spfile backups including Level (LV), Month, Day, Year and time of day"""

  global err_msg
  global vsrcdb
  global vohome
  global vdbid
  global vrco

  spfile_list = {}
  linecount = 0
  one_dict = {}
  fieldcounter=0
  tmp_date_dict={}
  bu_date={}
  spex={}
  tmpdatestr=''

  # create a list of spfile backups and write to a file:
  try:
    # v_bu_list=str(commands.getstatusoutput("export ORACLE_SID=" + vsrcdb + "1;" + "export ORACLE_HOME=" + vohome + "; echo 'list backup of spfile summary;' | " + vohome + "/bin/rman catalog rco/" + vrco + "@cat target /")[1])
    v_bu_list=str(commands.getstatusoutput("export NLS_DATE_FORMAT='Mon DD YYYY HH24:MI:SS'; export ORACLE_SID=" + vsrcdb + "1; export ORACLE_HOME=" + vohome + "; echo 'set dbid = " + vdbid + "; list backup of spfile summary;' | " + vohome + "/bin/rman catalog rco/" + vrco + "@cat target /")[1])
  except:
      err_msg = err_msg + ' Error: spfile_bu() retrieving spfiile bu summary : (%s)' % (sys.exc_info()[0])

  try:
    for oneline in v_bu_list.split('\n'):
      if oneline:
        if (not oneline[:1].isalpha()) and (oneline[:1] != '-' and oneline[:1] != '='):
          linecount += 1
          fieldcounter = 0
          one_dict={}
          for afield in oneline.split(' '):
            if afield:
              if fieldcounter == 0:
                one_dict['key'] = afield
                fieldcounter += 1
              elif fieldcounter == 1:
                one_dict['type'] = afield
                fieldcounter += 1
              elif fieldcounter == 2:
                one_dict['level'] = afield
                fieldcounter += 1
              elif fieldcounter == 3:
                one_dict['status'] = afield
                fieldcounter += 1
              elif fieldcounter == 4:
                one_dict['device'] = afield
                fieldcounter += 1
              elif fieldcounter == 5:
                tmp_date_dict['month'] = afield
                fieldcounter += 1
              elif fieldcounter == 6:
                tmp_date_dict['day'] = afield
                fieldcounter += 1
              elif fieldcounter == 7:
                tmp_date_dict['year'] = afield
                fieldcounter += 1
              elif fieldcounter == 8:
                tmp_date_dict['time'] = afield
                fieldcounter += 1
              elif fieldcounter == 9:
                one_dict['pieces'] = afield
                fieldcounter += 1
              elif fieldcounter == 10:
                one_dict['copies'] = afield
                fieldcounter += 1
              elif fieldcounter == 11:
                one_dict['compressed'] = afield
                fieldcounter += 1
              elif fieldcounter == 12:
                one_dict['tag'] = afield
                fieldcounter += 1
          # To restore the database using spfile, you have to restore to at least 1 second past the backup time
          # convert the time stamp to datetime obj then add one second to it so it's ready to use in the playbook
          dt_obj = datetime.strptime(str(tmp_date_dict['time']), '%H:%M:%S')
          temptime = dt_obj + timedelta(seconds=1)
          newtime = str(temptime.time())
          # Put all the date time info together in the proper format to use in the RMAN restore script: DD-MON-YYYY HH24:MI:SS
          tmpdatestr = tmp_date_dict['day'] + "-" + tmp_date_dict['month'].upper() + "-" + tmp_date_dict['year'] + " " + newtime
          one_dict.update({'backup_date': tmpdatestr})
          spfile_list.update({linecount: one_dict})
  except:
      err_msg = err_msg + ' Error: spfile_bu() parsing fields : (%s)' % (sys.exc_info()[0])

  return(spfile_list) #(spfile_list)


# ==============================================================================
# ================================== Main ======================================
# ==============================================================================

def main(argv):
  global err_msg
  global v_rec_count
  global vrco
  global vdbid
  global vsrcdb
  global vbu_type
  global vohome
  global vstage
  vtemp = {}
  msg = ""

  ansible_facts={ 'rmanfacts': {} }

  module = AnsibleModule(
      argument_spec = dict(
        rman_pwd        =dict(required=True),
        dbid            =dict(required=True),
        source_db       =dict(required=True),
        bu_type         =dict(required=True),
        ora_home        =dict(required=True),
        staging_path    =dict(required=True)
      ),
      supports_check_mode=True,
  )

  # Get arguements passed from Ansible playbook
  vrco     = module.params.get('rman_pwd')
  vdbid    = module.params.get('dbid')
  vsrcdb   = module.params.get('source_db')
  vbu_type = module.params.get('bu_type')
  vohome   = module.params.get('ora_home')
  vstage   = module.params.get('staging_path')


  # if (not vrco) and (not vdbid) and (not vsrcdb) and (not vbu_type) and (not vohome) and (not vstage):

  if vbu_type == "spfile":
    try:
      vtemp = spfile_bu()
      # for linecnt, item in vtemp:
      ansible_facts['rmanfacts']['spfile'] = vtemp
        #ansible_facts['rmanfacts']['spfile'] = vtemp
    except:
      err_msg = err_msg + ' Error: parsing spfile_bu return values: (%s)' % (sys.exc_info()[0])

  # Add any error messages caught before passing back
  if err_msg:
    msg = msg + err_msg

  if not err_msg:
    module.exit_json( msg=msg , ansible_facts=ansible_facts , changed="False")

    sys.exit(0)

  else:
    msg="\nError in rmanfacts module retrieving RMAN backup information for " + vbu_type
    msg = msg + err_msg
    module.fail_json( msg=msg )
    sys.exit(1)

# code to execute if this program is called directly
if __name__ == "__main__":
   main(sys.argv)
