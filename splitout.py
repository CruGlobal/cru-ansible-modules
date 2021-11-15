#!/Library/Frameworks/Python.framework/Versions/3.7/bin/python3
# -*- coding: utf-8 -*-

from ansible.module_utils.basic import *
from ansible.module_utils.facts import *
import subprocess
import sys
import os
import json
import re
import math
# import commands
from subprocess import (PIPE, Popen)
# import importlib.util

sys.path.append(r'./library/pymods/')
from crumods import debugg

# default reference name
def_ref_name = "splitout"
msg = ""
affirm = ['True','TRUE', 'true', True, 'T', 't', 'Yes', 'YES', 'yes', 'y', 'Y']

ANSIBLE_METADATA = {'status': ['stableinterface'],
                    'supported_by': 'Cru DBA team',
                    'version': '0.1'}

DOCUMENTATION = '''
---
module: splitout
short_description: given a string and a char to divide on split out the n th item:

example:
    str: /app/oracle/11.2.0.4/dbhome_1
    char: /
    item: 3

returns:
    11.2.0.4

notes: Returned values are then available to use in Ansible.
requirements: [ python2.* ]
author: "DBA Oracle module Team"

'''

EXAMPLES = '''

    # etc_oratab = /app/oracle/11.2.0.4/dbhome_1

    - name: split an item out of a string and split that item again
      local_action:
        module: splitout
        split_str: "{{ etc_oratab }}"
        split_on_char: "/"
        return_num: 3
        split_again_on_char: "."
        return_num: 1
        refname: splitout
        ignore_err: True (2)
        debugging: False

      (1) str - string to be split

      (2) char - character to split the string on

      (3) item - which item to return
          example: splitting /app/oracle/11.2.0.4/dbhome_1 on '/'
              item #:
                0    1       2          3          4
              ['', 'app', 'oracle', '11.2.0.4', 'dbhome_1']

              return_num #: 3 would return:
              11.2.0.4
       (4) split again is not required, but if desired the results from first split can feed the second
       (5) what to split on

   NOTE: these modules can be run with the when: master_node statement.
         However, their returned values cannot be referenced in
         roles or tasks later. Therefore, when running fact collecting modules,
         run them on both nodes. Do not use the "when: master_node" clause.

'''

def msgg(info):
    """ Given a string (info) add it to the msg string """
    global msg

    if msg:
        msg = info
    else:
        msg = msg + " " + info


# ==============================================================================
# =================================== MAIN =====================================
# ==============================================================================

def main ():

    # Name to call facts dictionary being passed back to Ansible
    # This will be the name you reference in Ansible. i.e. source_facts['sga_target'] (source_facts)
    global def_ref_name
    global msg
    global affirm
    ansible_facts = {}
    err_flag = False

    os.system("/usr/bin/scl enable python27 bash")
    # os.system("scl enable python27 bash")

    module = AnsibleModule(
      argument_spec = dict(
        split_str             =dict(required=False),
        split_on_char         =dict(required=True),
        return_num            =dict(required=True),
        second_split_on_char  =dict(required=False),
        second_return_num     =dict(required=False),
        refname               =dict(required=False),
        ignore_err            =dict(required=False),
        debugging             =dict(required=False)
      ),
      supports_check_mode=False,
    )

    # Get arguements passed from Ansible playbook
    s_str          = module.params.get('split_str')
    s_on           = module.params.get('split_on_char')
    return_num     = module.params.get('return_num')
    sec_s_on       = module.params.get('second_split_on_char')
    sec_return_num = module.params.get('second_return_num')
    vrefname       = module.params.get('refname')
    vignore        = module.params.get('ignore')
    vdebug         = module.params.get('debugging')

    # if no string passed in exit. Sometimes it won't be depending on the run.
    if not s_str or s_str.find("/") == -1:
        msg = "Nothing to do with input: {}".format(s_str or "Empty!")
        module.exit_json( msg=msg, ansible_facts=ansible_facts , changed="False")

    # if a reference name was passed use it
    if vrefname:
        refname = vrefname
    else:
        refname = def_ref_name

    if vignore in affirm:
        vignore = True
    else:
        vignore = False

    ansible_facts = { refname: {} }

    # /app/oracle/11.2.0.4/dbhome_1
    if not s_str:
        err_flag = True
        # if no string, but don't fail:
        msgg("Required 'split_str' parameter not passed.")

    if not s_on:
        err_flag = True
        msgg("Required character to split 'split_str' on not passed.")

    if not return_num:
        err_flag = True
        msgg("No item number defined to return")

    if err_flag:
        msg = "SPLITOUT module ERROR: " + msg
        if vignore:
            module.exit_json( msg=msg, ansible_facts=ansible_facts , changed="False")
        else:
            module.fail_json( msg=msg )

    try:
        debugg("first split: s_str={} split on (s_on): {} return_num: {}".format(s_str, s_on, return_num))
        result = s_str.split(s_on)[int(return_num)]
    except:
        err_flag = True
        msgg("Error: splitting string: {str} on character: {char} and returning item: {item}".format(str=s_str or "No String!", char=s_on or "No char!", item=return_num or "No item #!"))
    debugg("result: {}".format(result))
    if result:
        debugg("if sec_s_on: {}".format(sec_s_on))
        if sec_s_on:
            debugg("result=[{}].split({})[{}]".format(result, sec_s_on, sec_return_num))
            # 12.1.0.2 split on "." because we're dealing with indexes which start at 0 its sec_return_num - 1
            result = result.split(sec_s_on)[int(sec_return_num ) - 1]
            debugg("splitout() :: second split result={} sec_s_on: {} sec_return_num {}".format(result, sec_s_on, sec_return_num))

        msgg("Success")
        ansible_facts[refname].update( { 'item': result } )
        # print json.dumps( ansible_facts_dict )
        module.exit_json( msg=msg, ansible_facts=ansible_facts , changed="False")
    else:
        if err_flag:
            msg = "SPLITOUT module ERROR: " + msg
            if vignore:
                module.exit_json( msg=msg, ansible_facts=ansible_facts , changed="False")
            else:
                module.fail_json( msg=msg )

# code to execute if this program is called directly
if __name__ == "__main__":
    main()
