#!/opt/rh/python27/root/usr/bin/python

from ansible.module_utils.basic import *
from ansible.module_utils.facts import *
from ansible.module_utils.basic import AnsibleModule
import subprocess
import sys, os, json, re, time, commands, datetime, calendar
from subprocess import (PIPE, Popen)
from dateutil.relativedelta import relativedelta


ANSIBLE_METADATA = {'status': ['stableinterface'],
                    'supported_by': 'Cru DBA team',
                    'version': '0.1'}

DOCUMENTATION = '''
---
module: utilstimediff
short_description: Calculate diff between current time and ans_run_start.

description: This module will capture current time when called and if
    ans_run_start is defined, it will calculate the difference between
    current time and ans_run_start and output it to the user.

requirements: [ python => 2.6 ]
author: "DBA Oracle module Team"
'''

EXAMPLES = '''

    Utils* generates a variable called ans_run_start
    when the show command or run button is pressed.
    ans_run_start captures the current date and time
    as a starting point for timing the complete process.
    The time is captured just before the Ansible command is
    executed.

    Ex. of ans_run_start format generated by utils:
        ans_run_start="Wed Nov 13 13:47:13 EST"

    This module can be placed anywhere in the playbook.
    when called it will capture current time and output the difference
    between the current time and start time.

    Ex. of what a task in a playbook might look like using this module:

    - name: Run time check point
      utilstimediff:
        start_time: "{{ ans_run_start }}"
        refname: fin_time
        debugging: False
      when:
        - ans_run_start is defined

    # This module returns a dictionary that can be referenced by the default
      reference name for this module: 'utilstimediff' or by whatever reference
      name you provide with the optional 'refname' parameter.

      The dictionary looks like this
      { 'hrs': <hours> , 'min': <minutes>, 'sec': <seconds>, 'total': <00:00:00> }

    # Something like this task it optional:

    - pause:
      prompt: |
        "

            =====================================================================

                      Utils {{ fx }} run time:
                          ( HR:MI:SS.MS )
                            {{ '%02d' | format( timediff.hrs|int ) }}:{{ '%02d' | format( timediff.min|int ) }}:{{ '%02d' | format( timediff.sec|int ) }}

                       or

                            {{ timediff.total }}

                      ** From the time you hit the utils 'Run' button until now.

            =====================================================================

         "
      when:
        - master_node|bool
        - fin_time is defined
      tags: always

    * 'utils' is a GUI interface with Ansible play options used to launch
      the Ansible plays that call these modules.



'''

msg = ""
vdebugme = False
vlogit = False
refname = "timediff"
affirm = ['TRUE', 'True', 'true', 'YES', 'Yes', 'yes', 'T', 't', 'Y', 'y']


def add_to_msg(in_str):
    """Add an input string to the global msg string"""
    global msg

    if msg:
        msg = msg + " " + in_str
    else:
        msg = in_str

    return


def debugg(in_str):
    global vdebugme
    global vlogit

    if vdebugme:
        add_to_msg(in_str)

        if vlogit:
            with open("/u01/oracle/finrest.log","a") as f:
                f.write("%s\n" % (in_str))
                f.close()

    return


# ==============================================================================
# =================================== MAIN =====================================
# ==============================================================================
def main(argv):
    global msg
    global vdebugme
    global vlogit
    global refname
    global affirm

    ansible_facts={}

    # os.system("/usr/bin/scl enable python27 bash")

    module = AnsibleModule(
      argument_spec = dict(
        start_time     = dict(required=True),
        refname        = dict(required=False),
        debugging      = dict(required=False)
      ),
      supports_check_mode=False,
    )

    # Get arguements passed from Ansible playbook
    vstart_time  = module.params.get('start_time')
    vrefname     = module.params.get('refname')
    vdebug       = module.params.get('debugging')

    if vdebug in affirm:
        debugg("=====>>> vdebug: %s" % (vdebug))
        vdebugme = True
        if vdebug == "log":
            vlogit = True

    debugg("vdebug = {} | vdebugme = {} and vlogit = {} vdebugme = {}".format(vdebug or "None", vdebugme or "None", vlogit or "None", vdebugme or "None"))

    if vrefname:
        refname = vrefname

    # if a refname was passed in use it, else use the default ( 'timediff' )
    ansible_facts = { refname: { } }

    _elapsedtime = ( time.time() - vstart_time )
    _hours = int( ( _elapsedtime / 60.0 ) / 60.0 )
    _minutes = int( ( _elapsedtime / 60.0 ) - ( _hours * 60.0 ) )
    _seconds = int( _elapsedtime - ( _hours * 60 * 60 ) - ( _minutes * 60.0 ) )
    _hseconds = int( ( _elapsedtime - ( _hours * 60 * 60 ) - ( _minutes * 60.0 ) - _seconds ) * 100 )
    _tot_run = '%02d:%02d:%02d.%02d' % (_hours, _minutes, _seconds, _hseconds)

    ansible_facts[refname].update( { 'hrs': _hours, 'min': _minutes, 'sec': _seconds, 'total': _tot_run } )

    add_to_msg("Checkpoint time difference : %s " % (_tot_run))

    add_to_msg("utilstimediff completed successfully.")

    module.exit_json( msg=msg , ansible_facts=ansible_facts , changed="False")

# code to execute if this program is called directly
if __name__ == "__main__":
    main(sys.argv)
