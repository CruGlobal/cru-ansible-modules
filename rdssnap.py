#!/usr/bin/env python3

from ansible.module_utils.basic import *
from ansible.module_utils.facts import *
import commands
import json
import sys, errno
import os
import os.path
import subprocess
from subprocess import PIPE, Popen
import re
from signal import signal, SIGPIPE, SIG_DFL
import time

err_msg = ""
msg = ""
DebugMe = False


ANSIBLE_METADATA = {'status': ['stableinterface'],
                    'supported_by': 'Cru DBA team',
                    'version': '0.1'}

DOCUMENTATION = '''
---
module: rdssnap
short_description: Manually create an AWS RDS Snapshot.

'''

EXAMPLES = '''

    # Take manual snapshots of an AWS RDS database
    - local_action:
        module: rdssnap
          db_name: "{{ db_name }}"
          snapshot_name: "{{ snapshot_name }}"
          aws_region: "{{ aws_region }}"

    Notes:
        The only required input is db_name.

        If no snapshot_name is given the following default format will be used:
             db_name-twr-YYYY-MM-DD-HH-MM

        If no aws_region given the following default will be used:
             us-east-1


'''


# ==============================================================================
# =================================== MAIN =====================================
# ==============================================================================
def main ():
  """ Take a snapshot of an AWS RDS database """
  global msg
  global err_msg

  ansible_facts={}

  os.system("/usr/bin/scl enable python27 bash")

  module = AnsibleModule(
      argument_spec = dict(
        db_name         =dict(required=True),
        snapshot_name   =dict(required=False),
        aws_region      =dict(required=False)
      ),
      supports_check_mode=False,
  )

  # Get arguements passed from Ansible playbook
  vdb_inst_id = str(module.params.get('db_name'))
  vdb_snap_id = module.params.get('snapshot_name')
  vaws_region = module.params.get('aws_region')

  # See if a snapshot name was passed in
  # if not get the timestamp to create one
  if not vdb_snap_id:
      # get time string for snapshot name
      timestr = str(time.strftime("%Y-%m-%d-%H-%M"))
      vdb_snap_id = str(vdb_inst_id) + "-twr-" + timestr
      msg="Snapshot name not provided. Using default db_name + timestamp: %s " % (vdb_snap_id)

  if not vaws_region:
      if not msg:
          msg = "No region given. Using default region: us-east-1"
      else:
          msg = msg + "No region given. Using default region: us-east-1"

      vaws_region = "us-east-1"

  try:
      # command to create a manual AWS RDS snapshot
      tmp_cmd = "aws rds create-db-snapshot --db-instance-identifier " + vdb_inst_id + " --db-snapshot-identifier " + vdb_snap_id + " --region " + vaws_region
  except:
      err_msg = ' Error trying to concatenate the following: vdb_inst_id: [ %s ] and vdb_snap_id: [ %s ]' % (vdb_inst_id,vdb_snap_id)
      err_msg = err_msg + "%s, %s, %s, %s" % (sys.exc_info()[0], sys.exc_info()[1], err_msg, sys.exc_info()[2])
      raise Exception (err_msg)

  try:
    process = subprocess.Popen([tmp_cmd], stdout=PIPE, stderr=PIPE, shell=True)
    output, code = process.communicate()
  except:
    err_msg = ' Error [1]: orafacts module get_meta_data() output: %s' % (output)
    err_msg = err_msg + "%s, %s, %s, %s" % (sys.exc_info()[0], sys.exc_info()[1], err_msg, sys.exc_info()[2])
    raise Exception (err_msg)

  if "error" in output:
      err_msg="Error detected in output: %s" % (output)
      vchanged="False"
      module.fail_json(msg='ERROR: %s' % (err_msg), changed=False)

  if DebugMe:
      msg=output

  if not msg:
      msg="Snapshot created snapshot: " + vdb_snap_id + " " + output
  else:
      msg=msg + " Snapshot created snapshot: " + vdb_snap_id + " " + output

  # print json.dumps( ansible_facts_dict )
  module.exit_json( msg=msg, ansible_facts={} , changed=True)

# code to execute if this program is called directly
if __name__ == "__main__":
   main()
