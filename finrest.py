#!/opt/rh/python27/root/usr/bin/python

from ansible.module_utils.basic import *
from ansible.module_utils.facts import *
from ansible.module_utils.basic import AnsibleModule
import subprocess
import sys, os, json, re, time, commands
from subprocess import (PIPE, Popen)
import pexpect


ANSIBLE_METADATA = {'status': ['stableinterface'],
                    'supported_by': 'Cru DBA team',
                    'version': '0.1'}

DOCUMENTATION = '''
---
module: finrest
short_description: Finish an RMAN restore.

description: After an RMAN restore from tag to a point in time
  this module will finish the restore by running the commands
  from the SQL> prompt needed to complete the restore using
  pexpect.

requirements: [ python => 2.6 and pexpect >= 3.3 ]
author: "DBA Oracle module Team"
'''

EXAMPLES = '''

    # When restoring a database to a backup tag.
    # after RMAN completes the following commands are
    # run from a SQL> prompt:
           RECOVER DATABASE UNTIL CANCEL;
           CANCEL
           ALTER DATABASE OPEN RESETLOGS;
           SHUTDOWN IMMEDIATE;
           EXIT;
    # This module will expecute these commands.

    # Example of what a task in a playbook might look like:

    - name: Finish SQL part of database restore
      finrest:
        db_name: "{{ dest_db_name }}"
      when: master_node


'''
msg=""
vdebugme = False
vlogit = False
debug_log = "/home/oracle/.utils/debug.log"

def add_to_msg(in_str):
    """Add an input string to the global msg string"""
    global msg

    if msg:
        msg = msg + " " + in_str
    else:
        msg = in_str


def debugg(in_str):
    global vdebugme
    global vlogit
    global debug_log

    if vdebugme:
        add_to_msg(in_str)
    if vlogit:
        with open(debug_log,"a") as f:
            f.write("%s\n" % (in_str))
            f.close()


# ==============================================================================
# =================================== MAIN =====================================
# ==============================================================================
def main(argv):
    global msg
    global vdebugme
    global vlogit
    ansible_facts={}

    # os.system("/usr/bin/scl enable python27 bash")

    module = AnsibleModule(
      argument_spec = dict(
        oracle_sid     = dict(required=True),
        oracle_home    = dict(required=True),
        debug_mode     = dict(required=False)
      ),
      supports_check_mode=False,
    )

    # Get arguements passed from Ansible playbook
    voracle_sid  = module.params.get('oracle_sid')
    voracle_home = module.params.get('oracle_home')
    vdebug       = module.params.get('debug_mode')

    if vdebug or vdebug == "logit":
        vdebugme = True
        if vdebug == "logit":
            vlogit = True

    debugg("Checkpoint #1: oracle_sid=%s" % (voracle_sid))

    os.environ["ORACLE_HOME"] = voracle_home
    os.environ["ORACLE_SID"]  = voracle_sid
    # os.environ["ansible_python_interpreter"] = "/opt/rh/python27/root/usr/bin/python"
    os.environ["LD_LIBRARY_PATH"] = "/app/oracle/12.1.0.2/dbhome_1/lib:/lib:/opt/rh/python27/root/usr/lib64"

    add_to_msg("ORACLE_HOME=%s ORACLE_SID=%s" % (voracle_home,voracle_sid))

    child = pexpect.spawn('%s/bin/sqlplus / as sysdba' % (voracle_home))

    child.expect('SQL> ', timeout=30)

    debugg("child.expect : %s" % (str(child)))

    child.sendline('RECOVER DATABASE UNTIL CANCEL;')
    debugg("Checkpoint #2 RECOVER DB UNTIL")

    time.sleep(2)

    child.sendline('CANCEL')
    debugg("Checkpoint #3 CANCEL")

    time.sleep(2)

    child.sendline('ALTER DATABASE OPEN RESETLOGS;')
    debugg("Checkpoint #4 RESETLOGS")

    time.sleep(2)

    child.sendline('SHUTDOWN IMMEDIATE;')
    debugg("Checkpoint #5 SHUTDOWN")

    time.sleep(2)

    child.sendline('EXIT;')
    debugg("Checkpoint #6 EXIT")

    child.close()

    add_to_msg("finrest completed successfully.")

    module.exit_json( msg=msg , ansible_facts=ansible_facts , changed="False")

# code to execute if this program is called directly
if __name__ == "__main__":
    main(sys.argv)
