#!/opt/rh/python27/root/usr/bin/python

from ansible.module_utils.basic import *
from ansible.module_utils.facts import *
# import commands
import subprocess
import sys
import os
import json
import re
import math

try:
    import cx_Oracle
    cx_Oracle_found = True
except ImportError:
    cx_Oracle_found = False

ANSIBLE_METADATA = {'status': ['stableinterface'],
                    'supported_by': 'Cru DBA team',
                    'version': '0.1'}

DOCUMENTATION = '''
---
module: rcatdbid
short_description: Query the rman catalog repository for info not availabe from RMAN.
( specifically to return the database id (dbid) for a given database.)

notes: Returned values are then available to use in Ansible.
requirements: [ python2.* ]
author: "DBA Oracle module Team"
'''

EXAMPLES = '''

    # Retrieve the dbid of a given database.
    - name: retrieve the dbid of the db to restore
      local_action:
          module: rmandbid
          db_user: sys, system, rco etc.
          user_password: "{{ database_passwords['rcat'].system }}"
          db_name: "crmp"                 (1)
          cdb: "cat"                       *
          pdb: "catcdb"                    *
          rman_db: "rcat"                 (2)
          schema_owner: rco
          host: "{{ rman_host }}"
          refname: your_reference_name    (3) *
      become_user: "{{ utils_local_user }}"

    Notes:

        (1) db_name: name of the db to get the dbid for.

        (2) use either cdb / pdb OR rman_db to specify the rman catalog to connect to, but not both.

        (3) refname (optional) - any name you want to use to referene the data later in the play
                                 defualt reference name (refname) is 'rmandbid'

        * optional

'''

msg = ""
debugme = False
debug_log = "" # os.path.expanduser("cru-ansible-oracle/bin/.utils/debug.log")
# defaults
d_schema_owner = "rco"
d_cdb = "cat"
d_pdb = "catcdb"
d_domain = ".ccci.org"
d_refname = "rmandbid"
d_tns_str = "rcat.ccci.org"
default_ignore = False
affirm = ['True','TRUE', True, 'true', 'T', 't', 'Yes', 'YES', 'yes', 'y', 'Y']
v_ignore = False
ansible_facts = {}


def add_to_msg(a_msg):
    """Add the arguement to the msg to be passed out"""
    global msg

    if msg:
        msg = msg + " " + a_msg
    else:
        msg = a_msg


def debugg(db_msg):
    """if debugging is on add this to msg"""
    global debug_log
    global debugme
    global affirm
    # print("debugging log: {}".format(debug_log or "Empty!"))
    if debugme not in affirm:
        return()

    add_to_msg(db_msg)

    if not debug_log:
        set_debug_log()
        if not debug_log:
            add_to_msg("Error setting debug log. No debugging will be available.")
        return()

    try:
        with open(debug_log, 'a') as f:
            f.write(db_msg + "\n")
    except:
        pass

    return()


def set_debug_log():
    """
    Set the debug_log value to write debugging messages
    Debugging will go to: cru-ansible-oracle/bin/.utils/debug.log
    """
    global utils_settings_file
    global debug_log
    global debugme

    if not debugme or debug_log:
        return()

    try:
        with open(utils_settings_file, 'r') as f1:
            line = f1.readline()
            while line:
                if 'ans_dir' in line:
                    tmp = line.strip().split("=")[1]
                    debug_log = tmp + "/bin/.utils/debug.log"
                    return()

                line = f1.readline()
    except:
        print("ans_dir not set in ~/.utils unable to write debug info to file")
        pass


def add_to_msg(tmpmsg):
    """Add some info to the ansible_facts output message"""
    global msg
    global debug_msg

    if msg:
        msg = msg + tmpmsg
    else:
        msg = tmpmsg


def fail_handler(v_module, v_msg, v_changed):
    """
    Do failing here to cut down on code
    """
    global v_ignore
    global ansible_facts
    global affirm

    if v_ignore in affirm:
        v_module.exit_json( msg=v_msg, ansible_facts=ansible_facts , changed=v_changed)
    else:
        v_module.fail_json( msg=v_msg, ansible_facts=ansible_facts , changed=v_changed)


# ==============================================================================
# =================================== MAIN =====================================
# ==============================================================================
def main ():
    """
    Get DBID from RMAN catalog directly
    """
    global msg
    global d_refname
    global d_tns_str
    global d_domain
    global default_ignore
    global affirm
    global ansible_facts
    cdb_flag = False

    module = AnsibleModule(
      argument_spec = dict(
        db_user           =dict(required=True),
        user_password     =dict(required=True),
        db_name           =dict(required=True),
        cdb               =dict(required=False),
        pdb               =dict(required=False),
        rman_db           =dict(required=False),
        schema_owner      =dict(required=True),
        host              =dict(required=True),
        refname           =dict(required=False),
        ignore_errors     =dict(required=False)
      ),
      supports_check_mode=True,
    )

    # Get arguements passed from Ansible playbook
    v_db_user       = module.params.get('db_user')
    v_user_password = module.params.get('user_password')
    p_db_name       = module.params.get('db_name')
    p_cdb           = module.params.get('cdb')
    p_pdb           = module.params.get('pdb')
    p_rman_db       = module.params.get('rman_db')
    p_schema_owner  = module.params.get('schema_owner')
    v_host          = module.params.get('host')
    p_refname       = module.params.get('refname')
    p_ignore_errors = module.params.get('ignore_errors')

    # p_ = parameter, what was passed in
    # v_ = variable, value that is used in the program
    # d_ = default, what is defined in the header

    # CHECK PARAMETERS ===== START =============================================
    if p_refname:
        v_refname = p_refname
    else:
        v_refname = d_refname

    ansible_facts = { v_refname : {}}

    if p_ignore_errors is None:
        v_ignore = default_ignore
    else:
        v_ignore = p_ignore_errors

    if not cx_Oracle_found:
        ansible_facts = { v_refname : { "status": "Fail" } }
        # def fail_handler(v_module, v_msg, v_changed):
        fail_handler(module, "Error: cx_Oracle module not found. Exiting.", False)

    # container db is cat
    if p_cdb is None:
        v_cdb = d_cdb
    else:
        v_cdb = p_cdb
        cdb_flag = True

    # pluggable db [cat] if working with cdb [catcdb] our old rman db.
    if p_pdb is None and cdb_flag in affirm:
        v_pdb = d_pdb
    else:
        v_pdb = None

    # schema owner is rco
    if p_schema_owner is None:
        v_schema_owner = d_schema_owner
    else:
        v_schema_owner = p_schema_owner

    # name of the db to get dbid for
    if p_db_name:
        v_db_name = p_db_name

    # check required parameter values are not NULL. If any required are missing exit.
    if ( v_db_user and v_user_password is None ) or ( v_host is None ) or (v_db_name is None) or (v_schema_owner is None):
        tmp = "db_user: {} ,user_password: {}, host: {}, db_name: {}".format(v_db_user or "None!", v_user_password or "None!", v_host or "None!", v_db_name or "None!" )
        ansible_facts = { v_refname : { "called with parameters": tmp } }
        # def fail_handler(v_module, v_msg, v_changed):
        fail_handler(module, "Error: Required parameter missing", False)
    # CHECK PARAMETERS ===== FINISH =============================================

    # If cdb / pdb type connection:
    if (cdb_flag in affirm) or (v_pdb and v_cdb):
        if v_pdb[-1].isdigit():
            v_pdb = v_pdb[:-1]

    # prep host name: if domain not included add it.
    if d_domain not in v_host:
        v_host = "{}{}".format(v_host, d_domain)

    # choose which db you're connecting to
    if v_pdb and cdb_flag in affirm:
        v_db = vpdb
    elif p_rman_db:
        v_db = p_rman_db

    try:
        dsn_tns = cx_Oracle.makedsn(v_host, '1521', v_db)
    except cx_Oracle.DatabaseError as exc:
        error, = exc.args
        err_msg = "TNS generation error: {}, db name: {} host: {}".format(error.message, v_db or "Empty!", v_host or "Empty!")
        ansible_facts = { v_refname : { "dsn_tns": "FAILED" } }
        # def fail_handler(v_module, v_msg, v_changed):
        fail_handler(module, err_msg, False)

    try:
        con = cx_Oracle.connect(v_db_user, v_user_password, dsn_tns)
    except cx_Oracle.DatabaseError as exc:
        error, = exc.args
        ansible_facts = { v_refname : { "connection": "FAILED" } }
        msg="Database connection error: {}, tnsname: {}, db: {}".format(error.message or "Empty!", dsn_tns or "failed to create.", v_db or "Empty!")
        fail_handler(module, err_msg, False)

    try:
        cur = con.cursor()
    except:
        ansible_facts = { v_refname : { "cur creation": "FAILED" } }
        msg="Failed to generate cur from cx_Oracle connection: {}, tnsname: {}, db: {}".format(error.message or "Empty!", dsn_tns or "failed to create.", v_db or "Empty!")
        fail_handler(module, err_msg, False)


    if cdb_flag in affirm:
        try:
            cmd_str = "SELECT CASE WHEN MAX(object_name) IS NULL THEN 'False' ELSE 'True' END table_exists FROM dba_objects WHERE object_name = '{}'".format('v$pdbs')
            cur.execute(cmd_str)
        except cx_Oracle.DatabaseError as exc:
            error, = exc.args
            ansible_facts = { v_refname : { "cmd_str": "FAILED" } }
            err_msg = "Error determining if v_pdbs table exists, Error: {}".format(error.message)
            fail_handler(module, err_msg, False)

        vtemp = cur.fetchall()
        vpuggable_db = vtemp[0][0]

        if vpuggable_db:
            # set container database
            try:
                cmd_str = "alter session set container = {}".format(v_cdb.upper())
                cur.execute(cmd_str)
            except cx_Oracle.DatabaseError as exc:
                error, = exc.args
                module.fail_json(msg='Error setting container, Error: %s container: %s' % (error.message,v_cdb.upper()), changed=False)

    # Get dbid of database given in arguments
    try:
        cmd_str = "select dbid from {}.RC_DATABASE_INCARNATION where name = '{}' and current_incarnation = 'YES'".format(v_schema_owner,v_db_name.upper())
        cur.execute(cmd_str)
    except cx_Oracle.DatabaseError as exc:
        error, = exc.args
        module.fail_json(msg='Error selecting host_name from v$instance, Error: %s' % (error.message), changed=False)

    vtemp = cur.fetchall()
    vtemp = vtemp[0][0]
    vdbid = vtemp
    ansible_facts[v_refname]= { v_db_name: {'dbid': vdbid} }

    try:
        cur.close()
    except cx_Oracle.DatabaseError as exc:
      error, = exc.args
      module.fail_json(msg='Error getting status of BCT, Error: %s' % (error.message), changed=False)

    msg="Custom module rcatdbid succeeded in retrieving current dbid: %s for %s database from RMAN catalog." % (vdbid,v_db_name)

    vchanged="False"

    # print json.dumps( ansible_facts_dict )
    module.exit_json( msg=msg, ansible_facts=ansible_facts , changed=vchanged)

  # if parameters were NULL return informative error.
  else:

    msg="Custom module rmancat Failed"

    if module.params['system_pwd'] is None:
        ansible_facts['system_pwd'] = 'missing'
    else:
        ansible_facts['system_pwd'] = 'ok'

    if module.params['cdb'] is None:
        ansible_facts['cdb'] = 'missing'
    else:
        ansible_facts['cdb'] = 'ok'

    if module.params['pdb'] is None:
        ansible_facts['pdb'] = 'missing'
    else:
        ansible_facts['pdb'] = 'ok'

    if module.params['host'] is None:
        ansible_facts['host'] = 'missing'
    else:
        ansible_facts['host'] = 'ok'

    vchanged="False"

  # print json.dumps( ansible_facts_dict )
  module.exit_json( msg=msg, ansible_facts=ansible_facts , changed=vchanged)

# code to execute if this program is called directly
if __name__ == "__main__":
   main()
