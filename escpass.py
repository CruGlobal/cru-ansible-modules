#!/usr/bin/env python3
# -*- coding: utf-8 -*-


# This module: escpass.py - escape password
#
# Created: Monday October 4th, 2021
# By: Sam Kohler
#
# Purpose: To look for special characters in a password and put an escape
#          character, back slash (\) in front of it. This will make it easier
#          to use since MOS has so many different configurations for different
#          special characters as listed here:
#             How to use RMAN to connect database when password having special characters (Doc ID 2315890.1)
#          This module seemed to simplify the process.
#


from ansible.module_utils.basic import *
from ansible.module_utils.facts import *
from ansible.module_utils._text import to_native
from ansible.module_utils._text import to_text

import subprocess
import sys
import os
import json
import re

sys.path.append(r'./library/pymods/')
from crumods import *

ANSIBLE_METADATA = {'status': ['stableinterface'],
                    'supported_by': 'Cru DBA team',
                    'version': '0.1'}

DOCUMENTATION = '''
---
module: escpass - Escape Password

short_description: When logging into RMAN and other Oracle applications
                   with passwords using special characters it makes it much
                   easier to use if you escape the special characters.

'''

EXAMPLES = '''

    # when standing up a new database using restore, or clone etc.
    # this will look in asm for a new spfile and create an alias to it.
    - name: Map new alias to spfile
      escpass:
        password: "{{ database_passwords[rman_db].rco }}"
        refname: newpass
        ignore: True/False
        debugme: True/False
      when: master_node

   Note:
       To access the new password use reference name and ['password']:
                refname[‘password’]

       The default reference name if none is passed in is escpass, the module name.

       The example above would be:
                newpass['password']

'''
#Global variables
affirm = [ 'True', 'TRUE', True, 'T', 't', 'true', 'Yes', 'YES', 'Y', 'y']

# chars that don't need escaped:  ^,
chars_to_esc = [")","!",";"]
err_msg = ""
msg = ""
debugme = True
host_debug_log = "/tmp/mod_debug.log"
default_refname = "escpass"

# ==============================================================================
# =================================== MAIN =====================================
# ==============================================================================

def main ():
    """
    Escape special characters in Oracle passwords that use special characters
    making it easier to use the password without special quoting
    """
    global msg
    global default_refname
    global debugme
    global def_ignore
    global chars_to_esc

    # host is REQUIRED to have domain
    module = AnsibleModule(
      argument_spec = dict(
        password        =dict(required=True),
        refname         =dict(required=False),
        ignore          =dict(required=False),
        debug           =dict(required=False)
      ),
      supports_check_mode=False,
    )

    # Get arguements passed from Ansible playbook
    vpass      = module.params.get('password')
    vrefname   = module.params.get('refname')
    vignore    = module.params.get('ignore')
    vdebug     = module.params.get('debugging')

    if vdebug in affirm:
        debugme = True
        debugg("escpass...debugging set to True")
    else:
        debugme = False

    if not vrefname:
        refname = default_refname
    else:
        refname = vrefname
    ansible_facts = { refname : {}}

    if not vpass:
        temp="A password is required to continue. Password string was empty!"
        add_to_msg(temp)
        module.fail_json(msg=temp,ansible_facts=ansible_facts,changed=False)

    # see if the password contains any of the special characters that need escaped
    match_list = [ c for c in chars_to_esc if c in vpass ]

    # check to see if any special escape chars found
    if len(match_list) == 0:
        ansible_facts[refname].update( {'success': True, 'password': vpass , 'notes': 'no characters required escaping.'} )
        module.exit_json( msg=msg, ansible_facts=ansible_facts , changed="False")

    for c in chars_to_esc:
        if c in vpass:
            idx = vpass.find(c)
            # Check to make sure its not already escaped.
            # you could always have the case where a '\' appears before a special char. That requires too much code to determine, so do a manual fix.
            idx_b4 = idx + 1
            # if the char before the special character to escape is a back slash, assume its there to escape the special char and move on.
            # Otherwise, if the char before the special char to escape is not a backslash, then add one
            if vpass[idx_b4] == "\\" and vpass[idx] in chars_to_esc:
                continue
            else:
                vpass = vpass[:idx] + "\\" + vpass[idx:]

    ansible_facts[refname].update( {'success': True, 'password': vpass , 'notes': 'password escaped.'} )
    module.exit_json( msg=msg, ansible_facts=ansible_facts , changed="False")

# code to execute if this program is called directly
if __name__ == "__main__":
    main()
