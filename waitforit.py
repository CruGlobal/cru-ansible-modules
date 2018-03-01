#!/opt/rh/python27/root/usr/bin/python

from ansible.module_utils.basic import *
from ansible.module_utils.facts import *
import commands
import subprocess
import sys
import os
import json
import re                           # regular expression
import math
import time
from subprocess import (PIPE, Popen)

# Name to call facts dictionary being passed back to Ansible
# This will be the name you reference in Ansible. i.e. waitforit['result']
refname = 'waitforit'

# Reference links
# http://www.oracle.com/technetwork/articles/dsl/prez-python-queries-101587.html

# Notes: IAW this doc : http://docs.ansible.com/ansible/latest/dev_guide/developing_modules_general.html
# This module was setup to return a dictionary called "waitfor" which then makes those facts usable
# in the ansible playbook, and roles. The facts in this module are referenced by using the format:
#                    waifor['key'] which returns associated value - the ref name : "waitforit" was created in this module
#     example:    {{ waitfor['status'] }} => Running

ANSIBLE_METADATA = {'status': ['stableinterface'],
                    'supported_by': 'Cru DBA team',
                    'version': '0.1'}

DOCUMENTATION = '''
---
module: waitforit
short_description: This module will wait for a process before returning control to ansible

requirements: [ python2.7+ ]
author: "DBA Oracle module Team"
'''

EXAMPLES = '''

    # Waiting for a database to register with lsnrctl before proceeding
    - name: check db up
      waitforit:
        expression="ps -ef | /bin/grep -v grep | /bin/grep -m 1 samdb | /bin/awk -v x=8 '{print $x}'"
        state="ora_pmon_samdb1"
        wait_time: 10
        attempts: 5

    Note: attempts = -1 will wait indefinitely for the state

    This will return "True" when the condition is met, or "False" if the
    condition is not met by the wait time x attempts in seconds.
    if wait_time is not specified 5 seconds is the default.
    if attempts is not specified 1 attempt is the default.

'''

def envoke(mycommands):
    '''Invoke a command and return the result '''
    return Popen(mycommands, stdout=PIPE, shell=True).stdout.read()

# ==============================================================================
# =================================== MAIN =====================================
# ==============================================================================
def main ():
  """Wait for a state as specified by an expression and state (result)."""

  # os.system("/usr/bin/scl enable python27 bash")
  # os.system("scl enable python27 bash")

  module = AnsibleModule(
      argument_spec = dict(
        expression       =dict(required=True),
        state            =dict(required=True),
        wait_time        =dict(required=False, default=5),
        attempts         =dict(required=False, default=1)
      ),
      supports_check_mode=True,
  )

  # Get arguements passed from Ansible playbook
  v_expression = str(module.params.get('expression'))
  v_state      = module.params.get('state')
  v_wait_time  = int(module.params.get('wait_time'))
  v_attempts   = int(module.params.get('attempts'))

  # check required vars passed in are not NULL.
  if ( v_expression is not None) and (v_state is not None):

      v_attempt=0

      # expression: /app/oracle/11.2.0.4/dbhome_1/bin/lsnrctl status | /bin/grep samdb | /bin/grep -m 1 BLOCKED | /bin/awk -v x=4 '{print $x}'

      try:
        # t_state = os.system(v_expression)# str(commands.getstatusoutput(v_expression)[1])
        t_state = envoke(v_expression)
        if v_attempts == -1:
          # while str(commands.getstatusoutput(v_expression)[1]) != v_state:
          while t_state != v_state:
            time.sleep(v_wait_time)
            v_attempt+=1
            # t_state = os.system(v_expression # str(commands.getstatusoutput(v_expression)[1])
            t_state = envoke(v_expression)
        else:
          # while t_state != v_state and v_attempt < v_attempts:
          while t_state.find(v_state) == -1 and v_attempt <= v_attempts:
            time.sleep(v_wait_time)
            v_attempt+=1
            # t_state = os.system(v_expression # str(commands.getstatusoutput(v_expression)[1])
            t_state = envoke(v_expression)

      except:
        e = sys.exc_info()[0]
        module.fail_json(msg='waitforit Error : os info %s wait time %s attempts %s' % (e,v_wait_time,v_attempts), changed="False")


      if v_attempts == -1:
          msg="waitforit exiting indefinite wait: state reached"
      else:
          if v_attempt >= v_attempts:
                msg="waitforit exited: wait time reached pior to state " + "command output : expression: " + v_expression + " t_state : " + str(t_state)
          else:
                msg="waitforit exiting: state reached : " + str(t_state)

      # print json.dumps( ansible_facts_dict )
      module.exit_json( msg=msg, changed="False")

  else:

    msg="waitforit error : "

    if v_expression is None:
        msg=msg+"Required parameter EXPRESSION not provided"

    if v_state is None:
        msg=msg+" Required parameter STATE not provided"

    module.exit_json( msg=msg, changed="False")

# code to execute if this program is called directly
if __name__ == "__main__":
   main()
