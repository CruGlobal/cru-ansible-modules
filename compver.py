#!/usr/bin/python3

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

ANSIBLE_METADATA = {'status': ['stableinterface'],
                    'supported_by': 'Cru DBA team',
                    'version': '0.1'}

DOCUMENTATION = '''
---
module: compver
short_description: Compares two Oracle versions and returns the smaller of the two.
This is needed for Datapump when exporting and importing between dissimilar versions.
The parfile will require the "compatible={smaller value} parameter"

notes: Returned values are then available to use in Ansible.
requirements: [ python2.* ]
author: "DBA Oracle module Team"
'''

EXAMPLES = '''

    # When datapump exporting / importing between dissimilar database versions
    - local_action:
        module: compver
        export_ver: "{{ db1[version] }}"
        import_ver: "{{ db2[version] }}"

    "ansible_facts": {
        "compver": {
            "required": "true",         # can base conditional on this value
            "version": "11.2.0.4"
        }
    },
    "msg": "version 11.2.0.4 was found to be less than 12.1.0.2"

    # Use in datapump parfile:

        {% if compver['required'] %}
        compatible={{ compver['version'] }}
        {% endif %}

'''
#
#  Oracle version numbers explained (Doc ID 39691.1)
#
#   11.2.0.4.0
#    | | | | |_ Port Specific Maintenance Release
#    | | | |___ Port Specific Revision Level
#    | | |_____ Release Level
#    | |_______ Maintenance Release
#    |_________ Version Number
#

msg=""

def add_to_msg(in_str):
    """Add an input string to the global msg string"""
    global msg
    if msg:
        msg = msg + " " + in_str
    else:
        msg = in_str


def fin_msg(vdb1,vdb2):
    global msg

    if msg:
        msg = msg + "version %s was found to be less than %s" % (vdb1,vdb2)
    else:
        msg = "version %s was found to be less than %s" % (vdb1,vdb2)
# ===================================================================================================
#                                          MAIN
# ===================================================================================================

def main ():
    global msg
    ansible_facts={}
    vrefname = "compver"

    module = AnsibleModule(
      argument_spec = dict(
        ver_db1        = dict(required=True),        # database name to run srvctl against
        ver_db2        = dict(required=True),        # command to execute: start | stop
        refname        = dict(required=False)        # name the vars will be referenced by in Ansible
      ),
      supports_check_mode = False               # srvctl has '-eval' parameter. Use it to implement ???
    )

    # =============================== Start getting and checking module parameters ===================================
    # ** Note: parameters are passed as strings, even number parameters.

    vdb1      = module.params["ver_db1"]
    vdb2      = module.params["ver_db2"]
    prefname  = module.params["refname"]

    if prefname:
        vrefname = prefname

    # Return the lesser version number for use in datapump export
    for posit_db1 in vdb1.split("."):
        for posit_db2 in vdb2.split("."):
            if posit_db1 < posit_db2:
                fin_msg(vdb1,vdb2)
                ansible_facts[vrefname] = {'required':'true', 'version': vdb1}
                module.exit_json(msg=msg, ansible_facts=ansible_facts, changed="False")
            elif posit_db2 < posit_db1:
                fin_msg(vdb2,vdb1)
                ansible_facts[vrefname] = {'required':'true', 'version': vdb2}
                module.exit_json(msg=msg, ansible_facts=ansible_facts, changed="False")
            else:
                add_to_msg("Database versions are the same. Version parameter for datapump export not required.")
                ansible_facts[vrefname] = {'required':'false', 'version': vdb2}
                module.exit_json(msg=msg, ansible_facts=ansible_facts, changed="False")

    add_to_msg("Something strange hanppened. Call the module developer. Don't trust this data.")
    module.exit_json(msg=msg, ansible_facts=ansible_facts, changed="False")

if __name__ == '__main__':
    main()
