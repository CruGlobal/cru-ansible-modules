#!/Library/Frameworks/Python.framework/Versions/3.7/bin/python3
# -*- coding: utf-8 -*-

# Written by: Sam Kohler
# Date: August 12, 2021
#
# Purpose: to take

from ansible.module_utils.basic import *
from ansible.module_utils.facts import *
from ansible.module_utils._text import to_native
from ansible.module_utils._text import to_text

import subprocess
import sys
import os
import json
import re                           # regular expression
import yaml
import fnmatch

sys.path.append(r'./library/pymods/')
from crumods import *

ANSIBLE_METADATA = {'status': ['stableinterface'],
                    'supported_by': 'Cru DBA team',
                    'version': '0.1'}

DOCUMENTATION = '''
---
module: xtractitem
short_description: This module will extract an item (word) relative to a match string.

notes: Returned values and results that are then available to use in Ansible.
requirements: [ python3 ]
author: "DBA Oracle module Team"
'''

EXAMPLES = '''

    Usage: Provide a string, match string and index relative to the
           match string to this module and it will extract the item.

    - name: extract scn needed for good restore
      local_action:
        module: xtractscn
    (1)    input_str: "{{ re_db_preview.stdout }}"
    (2)    start_match: "{{ string to match }}"
    (3)    end_match: "{{ second string to match }}"
    (4) refname: "{{ refname_str }}"
    (5) ignore_errors: False
        debugging: False
      when: master_node|bool

      (1) input_str - input a long string to search.

      (2) start_match - give a unique substring to start match within the input_str to get the item you want

      (3) end_match - this will be the ending unique string bracketing the item you want.

      (4) refname: variable name you want Ansible to reference results with.
            default: xtractitem

      (5) ignore - ignore errors. Optional. Default: False.
            If False and the module fails, the module will fail the play.
            If True, if the module fails, it will not fail the play.

    Example:

    input_str = """
          List of Archived Logs in backup set 311151490
      Thrd Seq     Low SCN    Low Time             Next SCN   Next Time
      ---- ------- ---------- -------------------- ---------- ---------
      1    5200    251137294505 12-aug-2021 06:21:43 251146718768 12-aug-2021 10:20:25
      1    5201    251146718768 12-aug-2021 10:20:25 251146718828 12-aug-2021 10:20:30
      2    5376    251146718780 12-aug-2021 10:20:27 251146718825 12-aug-2021 10:20:30
      2    5377    251146718825 12-aug-2021 10:20:30 251146721130 12-aug-2021 10:22:27
      1    5202    251146718828 12-aug-2021 10:20:30 251146721105 12-aug-2021 10:22:25
    validation succeeded for backup piece
    recovery will be done up to SCN 251137259022
    Media recovery start SCN is 251137259022
    Recovery must be done beyond SCN 251137287866 to clear datafile fuzziness
    validation succeeded for backup piece
    Finished restore at 12-aug-2021 10:36:00
    released channel: sbt1

    Recovery Manager complete.
    """

    start_match = "Recovery must be done beyond SCN"

    end_match = "to clear datafile fuzziness"

    start_match and end_match uniquely bracket an item and
    will cause this to be returned => 251137287866

'''

msg = ""
default_refname = "xtractitem"


def main ():
    """
    extract an item from a string
    """
    global debugme
    global affirm
    global default_refname
    temp = None

    module = AnsibleModule(
      argument_spec = dict(
        input_str        =dict(required=True),
        start_match      =dict(required=True),
        end_match        =dict(required=False),
        xtra_ops         =dict(required=False),
        refname          =dict(required=False),
        ignore_errors    =dict(required=False),
        debugging        =dict(required=False)
      ),
      supports_check_mode=False,
    )

    # Get arguements passed from Ansible playbook
    # user name / password and db to connect to on host
    v_in_str        = module.params.get('input_str')
    v_start_match   = module.params.get('start_match')
    v_end_match     = module.params.get('end_match')
    v_xtra_ops      = module.params.get('xtra_ops')
    v_refname       = module.params.get('refname')
    v_ignore_err    = module.params.get('ignore_errors')
    v_debug         = module.params.get('debugging')

    # init()
    if v_debug in affirm:
        debugme = True
        set_debug_log()
    else:
        debugme = False

    if v_ignore_err is None:
        vignore = default_ignore
    else:
        vignore = v_ignore_err

    if not v_refname:
        refname = default_refname
    else:
        refname = v_refname

    # This has to be here, cannot reference before setting refname
    ansible_facts = { refname : { } }
    debugg(v_in_str)

    pattern = "{wc}{match_str}{wc}".format(wc="*", match_str=v_start_match.strip())
    for line in v_in_str.split("\n"):
        if not line.strip():
            continue
        match = fnmatch.fnmatch(line, pattern)
        if match:
            matched_str = line
            break

    debugg("\n\nFOUND THE LINE: {}\n\n".format(matched_str or "Empty!"))
    try:
        start_idx = matched_str.find(v_start_match)
    except:
        e = sys.exc_info()[0]
        module.fail_json(msg='xtractitem start_idx Error : os info %s' % (e), ansible_facts=ansible_facts, changed="False")

    start_idx = int(start_idx) + int(len(v_start_match))
    debugg("\nstart_idx={}\n".format(start_idx))
    try:
        end_idx = int(matched_str.find(v_end_match))
    except:
        e = sys.exc_info()[0]
        module.fail_json(msg='xtractitem end_idx Error : os info %s' % (e), ansible_facts=ansible_facts, changed="False")

    debugg("########### START_IDX={} END_IDX={} ###########".format(start_idx,end_idx))
    item = matched_str[start_idx:end_idx]

    if v_xtra_ops:
        debugg("v_xtra_ops = {}".format(str(v_xtra_ops)))
        if "+" == v_xtra_ops[:-1]:
            item = int(item) + int(v_xtra_ops[1:].strip())
        elif "-" == v_xtra_ops[:-1]:
            item = int(item) - int(v_xtra_ops[1:].strip())
        debugg("post v_xtra_ops item = {}".format(item))

    if not item:
        item = "SCN UNKNOWN"

    ansible_facts = { refname : item , refname : { "status" : "success", "results" : item } }
    msg = "xtractitem finished successfully. Returned item: {}".format(item)
    debugg(msg)
    module.exit_json( msg=msg, ansible_facts=ansible_facts , changed=False)

# code to execute if this program is called directly
if __name__ == "__main__":
    main()
