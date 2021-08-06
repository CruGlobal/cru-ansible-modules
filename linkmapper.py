#!/usr/bin/env pyhton3
# -*- coding: utf-8 -*-

# Written: Tuesday August 3, 2021
# By: Sam Kohler
# Purpose: More powerful way to edit files and remap database links
#
# Make coding more python3-ish
from __future__ import (absolute_import, division, print_function)
__metaclass__ = type

from ansible.module_utils.basic import *
from ansible.module_utils.facts import *
from ansible.module_utils._text import to_native
from ansible.module_utils._text import to_text
# from ansible.error import AnsibleError

import subprocess
import sys
import os
import json
import re                           # regular expression
import yaml
# import math
# import time
# import pexpect
# from datetime import datetime, date, time, timedelta
from subprocess import (PIPE, Popen)

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
module: linkmapper
short_description: module that will remap database links and synonyms that use those links
                   when deployed against a database with mapping information.

notes: Returned values and/or results are then available to use in Ansible.
requirements: [ python3 ]
author: "DBA Oracle module Team"
'''

EXAMPLES = '''

    Usage: provide a database name, host and link mapping information.
           This module expects a dictionary input with mappings.
           key names must be kept the same as shown here:

    # To run a sql command in a database during ansible play.
    - name: remap db links and synonyms that use them
      local_action:
        module: linkmapper
        user_id: sys
        user_password: "{{ database_passwords[db_name].sys }}"
        proxy_user: system
        proxy_pwd: "{{ database_passwords[db_name].system }}"
        db_name: "{{ db_sid }}"
    (1) host: "{{ host }}" (2)
        is_rac: "{{ orafacts['is_rac'] }}"
    (2) link_filter: "{{ item.filter }}"
    (3) map_to: "{{ item.to }}"
    (4) link_owner: "{{ item.owner }}" # if you want a link owner to have something different than the general database list the owner name here else leave blank
    (5) expect_results: yes "{{ expect_records }}" True / False or Yes / No - If results expected db link tests will be returned if db doesn't exist yet, make this false
        refname: "{{ refname_str }}"
    (6) ignore_errors: False
        debugging: False
      with_items:
        - { filter: "fscm", "to": "fscmtmp.dr.cru.org", "owner": "all" }
        - { filter: "hcm",  "to": "hcmtmp.dr.cru.org", "owner": "finadm" }
      when: master_node|bool

      Note: either use mapping or link_filter/map_to ( not both ) one or the other is required.

      (1) host must be fully qualified ( including domain ) ie. ploradr.dr.cru.org not ploradr.

      (2) link_filter - unique general matching string to compare to db links:
           i.e. for PS Fin use: fscm
                for PS HR use: hcm
                for Siebel use: crm

      (3) map_to: database name must be fully qualified: i.e. fscmtmp.dr.cru.org not fscmtmp.

      (4) link owner - only needed if a schema owner will map a db link to a different database.
          if querying a table for records : True
          if dropping restore points: False

      (5) expect results - optional - if yes/true will return db link test results.

      (6) ignore - ignore errors. Optional. Default: False
          If you know the database may be down set ignore: True.
          If connection to the database fails the module will not throw a
          fatal error and allow the play to continue.
          If set to False this module will fail if an error occurs.


'''



# Debugging will go to: cru-ansible-oracle/bin/.utils/debug.log
msg = ""
module = None
vignore = ""
default_ignore = False
debugme = False
default_refname = "linkmapper"
vault_file = ""
affirm = ['True','TRUE', True, 'true', 'T', 't', 'Yes', 'YES', 'yes', 'y', 'Y']
db_home_name = "dbhome_1"
debug_log = ""
debug_filename = "debug.log"
utils_settings_file = os.path.expanduser("~/.utils")
cru_domain = ".ccci.org"
dr_domain = ".dr.cru.org"
non_rac_dbs = [ "dw", "dr" ]
proxy_user = "system"
cur = None
nasty_list = [
            "truncate",
            "drop"
            "drop database"
            ]

exemption_list = [ "drop restore point" ]
p_dict = None


def pkg_pass(db, user, pri_filter=None, sec_filter=None):
    """
        ====================================
        Given a vault name ( v_name ):
            aws      or  aws_vault.yml
            tower    or  tower_vault.yml
            mysql    or  mysql_vault.yml
            postgres or  postgres_vault.yml
        and a primary filter ( i.e. f1 database name )
        and a secondary filter ( i.e. f2 user name )
        example:
            v_name: aws_vault.yml or just aws
            primary filter f1: [ database_passwords, asm_passwords]
                secondary filter f2: [ dbname ]
                    teritary_filter f3: [ user name ]
        or
        just a primary filter:
        example:
            vault: postgres_vault.yml
            f1: [ ploemomr01_pdb_admin_pass, scp_user, temppass, osb_password, datadog_oracle_password, datadog_api_key/datadog_app_key ]
        return the password from an AWS ansible-vault

    Attempting new method to retrieve ansible vault passwords when this code is packaged.

    """
    debug_passwords = False
    global affirm
    global p_dict
    new_yml_str = ""
    if not pri_filter:
        pri_filter = "database_passwords"

    debugg("\nGlobal pkg_pass()....starting...\nparameters:\n\tdb={}\n\tuser={}\n\tpri_filter={}\n\tsec_filter={}".format(db or "Empty!", user or "Empty!", pri_filter or "Empty!", sec_filter or "Empty!"))

    filter_count = count_filters(db, user, pri_filter, sec_filter)

    if not p_dict:
        unlock_lpass()

        v_loc = get_vault_location()

        debugg("\nGlobal pkg_pass():: \n\tv_loc = {}".format(v_loc))

        # /Users/samk/.pyenv/shims/ansible-vault if needed
        cmd_str = "ansible-vault view {}".format(v_loc)

        if debug_passwords in affirm: debugg("\nGlobal pkg_pass() :: CALLING SUBPROCESS...\n\tcmd_str = {}\n\toutput={}".format(cmd_str, output))
        output = run_cmd(cmd_str)

        if debug_passwords in affirm: debugg("\nGlobal pkg_pass():: ...after communicate() ... \n\touput = {} ".format(output or "Empty!") ) #), code or "Empty!"))

        for item in output.split("\n"):
            if debug_passwords in affirm: debugg("for loop() === line={}".format(item))
            if item[:1] == "#" in ['#','---']:
                continue
            else:
                new_yml_str = new_yml_str + item + "\n"

        if debug_passwords in affirm: debugg("\nGlobal :: pkg_pass() :: \n\tcleaned up dictionary => new_dict={}".format(str(new_yml_str)) )

        # good_dict = find_this_item(output)
        pwd_dict = yaml.safe_load( new_yml_str )
        if debug_passwords in affirm: debugg("\nGlobal :: pkg_pass() :: \n\tfilter_count = {} after yaml.safe_load(pwd_dict) => {}".format(
            filter_count or "Empty!", str(pwd_dict)))
        p_dict = pwd_dict
    else:
        if debug_passwords in affirm: debugg("\nGlobal :: pkg_pass() :: p_dict already populated {}".format(str(p_dict)))
        pwd_dict = p_dict

    if debug_passwords in affirm: debugg("\nGlobal :: pkg_pass() :: filter_count = {} attempting password retrieval....from {}".format(filter_count or "Empty!", str(pwd_dict)))
    try:
        if filter_count == 1:
            the_passwd = pwd_dict.get(db, None)
        elif filter_count == 2:
            the_passwd = pwd_dict[pri_filter][db].get(user, None)
        elif filter_count == 3:
            the_passwd = pwd_dict[pri_filter][db].get(user, None)
        elif filter_count == 4:
            the_passwd = pwd_dict[pri_filter][sec_filter][db].get(user, None)
        else:
            return(None)
    except:
        # It could have been a top level password:
        try:
            the_passwd = pwd_dict[db].get(user, None)
        except:
            debugg("\nGlobal :: pkg_pass() :: SECOND ATTEMPT: pwd_dict[{}].get({})\n".format(
                db or "Empty!",
                user or "Empty!"))
            return (None)
        if not the_passwd:
            add_to_msg("Password for {}  {}@{} not found in ansible vault.".format(pri_filter, user or "Empty!", db or "Empty!"))
            debugg("\nGlobal :: pkg_pass() :: password for user={}@db={} not found in ansible vault.\n".format(user or "Empty!",                                                                                              db or "Empty!"))
            return (None)

    debugg("\nGLOBAL :: pkg_pass() :: exiting\n\treturning password = {} for {}@{}\n".format(the_passwd,user,db))
    return the_passwd


def get_vault_location():
    """
       Read the ~/.utils settings file and
       retrieve the Anisble vault file location
       another piece of the puzzle needed to unlock the vault to get passwords
    """
    global vault_file
    debugg("\nGlobal :: utils :: get_vault_location() ...starting...\nvault_file={}".format(vault_file or "Empty!"))

    if vault_file:
        return(vault_file)

    utils_settings_file = os.path.expanduser("~/.utils")
    debugg("\nGlobal :: utils :: get_vault_location()\n\tutils_settings_file={}".format(utils_settings_file))
    try:
        cmd_str = "cat {} | grep ans_vault".format(utils_settings_file)
        output = run_cmd(cmd_str)
    except:
        # print("Error: reading ~/.utils to determine vault file location cmd_str = {}".format(cmd_str))
        debugg("Global :: utils :: Error: reading ~/.utils to determine vault file location cmd_str = {}".format(cmd_str))
        return

    debugg("\nGlobal :: utils :: get_vault_location()\n\toutput={}".format(output))

    vault_file = output.split("=")[1].strip()  # output.decode('utf-8').split("=")[1]
    debugg("\nGlobal :: utils :: get_vault_location()...exiting...\n\treturn={}".format(vault_file))
    if not os.path.isfile(vault_file):
        debugg("\nVault location defined\nHowever, file does not exist!\n{}".format(vault_file or "Empty!"))
        return(None)
    else:
        debugg("\nVault location defined\nreturning vault_file={}\n".format(vault_file))
        return(vault_file)


def count_filters(f1=None, f2=None, f3=None, f4=None):
    """ Called by "get_cloud_passwd() to count the number
        of filters passed
    """
    debugg("\nGlobal :: count_filters() :: ...starting....\nwith paramters:\n\tf1={}\n\tf2={}\n\tf3={}\n\tf4={}".format(f1 or "None", f2 or "None", f3 or "None", f4 or "None"))
    args = locals()
    count = 0
    for k, v in args.items():
        if v is not None:
            count += 1

    return(count)


def unlock_lpass():
    '''Passwords have moved to lpass, check status and unlock before trying to retrieve passwords.'''
    debugg("unlock_lpass().....starting......")
    cmd_str="lpass status"
    results = run_cmd(cmd_str)
    debugg("unlock_lpass()  results={}".format(results))
    # if not logged in result should be: "Not logged in." else "Logged in as sam.kohler@cru.org."
    if "Not logged in." == results:
        debugg("Not logged in! Asking for user password.")
        debugg("LastPass is locked.\n Go to iterm and unlock your account using:\n lpass login bob.user@cru.org \nExit this app and try again.")
        return("LOCKED")
    else:
        return("UNLOCKED")


def set_debug_log():
    """
    Set the debug_log value to write debugging messages
    Debugging will go to: cru-ansible-oracle/bin/.utils/debug.log
    """
    global utils_settings_file
    global debug_log
    global debug_filename
    global debugme

    if not utils_settings_file:
        return()

    cmd_str = "cat $HOME/.utils | grep ans_dir"
    tmp = run_cmd(cmd_str)
    output = tmp.decode("utf-8")

    try:
        tmp_path = output.strip().split("=")[1]
        debug_log = tmp_path + "/bin/.utils/{}".format(debug_filename)
    except:
        print("ans_dir not set in ~/.utils unable to write debug info to file")
        pass
    return()


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


def israc(host_str=None):
    """
    Determine if a host is running RAC or Single Instance
    """
    global err_msg
    global cru_domain
    global dr_domain

    if host_str is None:
        return()

    if "org" in host_str:
        host_str = host_str.replace(cru_domain,"")
        host_str = host_str.replace(dr_domain, "")

    if "dr" in host_str or "dw" :
        return(False)

    # if the last digits is 1 or 2 ( something less than 10) and not 0 (60) return True
    if host_str[-1:].isdigit() and int(host_str[-1:]) < 10 and int(host_str[-1:]) != 0:
        return(True)
    else:
        return(False)


def run_cmd(cmd_str):
    """
    Encapsulate all error handline in one fx.
    Run cmd_str on the local machine.
    """

    try:
        process = subprocess.Popen([cmd_str], stdout=PIPE, stderr=PIPE, shell=True)
        output, code = process.communicate()
    except:
        pt1 = "Error :: run_cmd() :: running cmd_str: {}".format(cmd_str)
        add_to_msg(pt1)
        pt2 = " {}, {}, {}".format(sys.exc_info()[0], sys.exc_info()[1], sys.exc_info()[2])
        add_to_msg(pt2)
        return("")

    if output:
        return(output)
    else:
        return("")


def fail_module(fail_no=None):
    """
    Do all the fail logic here.
    Expecting module to be passed, along with a msg
    also mode = hard/soft
    p_mode=True  - ignore errors. Exit soft.
    p_mode=False - don't ignore errors. Exit hard. Derail play.
    """
    global msg
    global affirm
    global default_refname
    global module
    global vignore
    ansible_facts = {}
    ansible_facts.update( { default_refname : { 'results': {'success': False, 'output': fail_no or "None" } } } )

    debugg("linkmapper :: fail_hard_or_soft()...starting....fail_no={}".format(fail_no or "None"))

    try:
        debugg(msg)
        debugg(fail_no)
        add_to_msg(fail_no)
        if vignore in affirm:
            module.exit_json(msg=msg, ansible_facts=ansible_facts, changed=False)
        else:
            module.fail_json(msg=msg, ansible_facts=ansible_facts, changed=False)
    except:
        module.fail_json(msg="ERROR: fail_no: {}".format(fail_no or "None"))


def create_dsn(vhost, vdb, vport="1521"):
    """
    Create dsn_tns object and pass it back
    """

    try:
        dsn_tns = cx_Oracle.makedsn(vhost, vport, vdb)
    except cx_Oracle.DatabaseError as exc:
        error, = exc.args
        temp_msg = "Failed to create dns_tns: error: {} host={} db={}".format(error.message or "Empty!", vhost or "Empty!", vdb or "Empty!" )
        add_to_msg(tmp_msg)
        fail_module("#2")

    debugg("dsn_tns created: {}".format(str(dsn_tns)))
    return(dsn_tns)


def create_conn(p_dsn, p_uid, p_pwd):
    """
    Create a cx_Oracle connection and pass it back.
    """

    try:
        if p_uid == "sys":
            con = cx_Oracle.connect(dsn=p_dsn,user='sys',password=p_pwd,mode=cx_Oracle.SYSDBA)
        else:
            con = cx_Oracle.connect(p_uid, p_pwd, p_dsn)
    except cx_Oracle.DatabaseError as exc:
        error, = exc.args
        temp_msg = "DB CONNECTION FAILED : {} dsn={} user={} passwd={}".format(error.message, str(p_dsn) or "Empty!", )
        add_to_msg(temp_msg)
        debugg(temp_msg)
        fail_module("#3")

    if con:
        cur = con.cursor()
    else:
        add_to_msg("cx_Oracle connection failed to create.")
        fail_module("#4")

    debugg("Cursor successfully created.")
    if cur:
        return(cur)
    else:
        return(None)


def execute_cmd(p_cur, p_cmd_str, p_expect):
    """
    Given a cursor and command string
    execute the command
    """
    global affirm
    debugg("\nexecute_cmd()...starting....\n")

    try:
        p_cur.execute(p_cmd_str)
    except cx_Oracle.DatabaseError as exc:
        error, = exc.args
        temp_msg = "Error {} executing command: {}".format(error.message, p_cmd_str)
        add_to_msg(temp_msg)
        debugg(temp_msg)
        # Fail hard or soft
        fail_module("#5")

    debugg("\nexecute_cmd()...\n\tcmd_str={}\ncommand successfully executed.\n".format(p_cmd_str))

    if p_expect in affirm:
        return_list = []
        vtemp = p_cur.fetchall()
        debugg("\nexecute_cmd()...command successfully executed.\nfetchall()\n\treturned={}".format(str(vtemp)))
        for item in vtemp:
            return_list.append(str(item[0]).strip())
        return(return_list)
    else:
        return()


def exit_module():
    """
    successfully exit the module
    """
    pass


def convert_link(p_filter, p_cre_stmt, p_to, p_db):
    """
    Convert a single db link
    expecting parameters input:
    example:

        p_filter: crm
        p_cre_stmt: CREATE DATABASE LINK "CRMTMP.DR.CRU.ORG" CONNECT TO "INTF_FIN" IDENTIFIED BY {pwd} USING 'CRMTMP.DR.CRU.ORG'
        p_to: cmp.ccci.org
        p_db: fscmtmp

    Return the new create string: ( with password filled in )
        CREATE DATABASE LINK "CRMP.CCCI.ORG" CONNECT TO "INTF_FIN" IDENTIFIED BY {pwd} USING 'CRMP.CCCI.ORG'
    """
    debugg("\nconvert_link()...starting....with parameters....\n\tp_filter={}\n\tp_cre_stmt={}\n\tp_to={}\n\tp_db={}".format(p_filter, p_cre_stmt, p_to, p_db))

    # convert p_to: cmp.ccci.org into usable slices db ( crmp ) and domain ( .ccci.org )
    to_db_name = p_to.split(".")[0]
    to_domain = p_to[p_to.find("."):]

    debugg("\nIf to_db_name: {} not in p_cre_stmt: {} return.......".format(p_filter.upper(), p_cre_stmt))
    if p_filter.upper() not in p_cre_stmt.upper():
        debugg("\nto_db_name-{} not in pre_cre_stmt={}\nconvert_link() doing nothing...\nreturning(None)....".format(to_db_name, p_cre_stmt))
        return(None)

    # convert values in the create statement.
    # output:
    #   FINADM:
    #       - CREATE DATABASE LINK "CRMTMP.DR.CRU.ORG" CONNECT TO "INTF_FIN" IDENTIFIED BY {pwd} USING 'CRMTMP.DR.CRU.ORG'
    #       - CREATE DATABASE LINK "HCMTMP.DR.CRU.ORG" CONNECT TO "INTF_FIN" IDENTIFIED BY {pwd} USING 'HCMTMP.DR.CRU.ORG'
    #  PUBLIC:
    #       - CREATE PUBLIC DATABASE LINK "CRMTMP.DR.CRU.ORG" USING 'CRMTMP.DR.CRU.ORG'
    #
    # if CRM (filter) in CREATE DATABASE LINK "CRMTMP.DR.CRU.ORG" CONNECT TO "INTF_FIN" IDENTIFIED BY {pwd} USING 'CRMTMP.DR.CRU.ORG'. process it

    # may have to replace domain too ( .dr.cru.org => .ccci.org ), so get the whole value between quotes
    # re.findall returns  ['CRMTMP.DR.CRU.ORG', 'INTF_FIN'] so only take index [0]
    debugg("\nsubstring {} found in pre_cre_stmt".format(p_filter.upper()))
    list_of_quoted_substrings = re.findall('"([^"]*)"', p_cre_stmt)
    link_with_domain = list_of_quoted_substrings[0]
    debugg("\nexisting link_with_domain={} to {}\n".format(str(link_with_domain).upper(),p_to.upper()))
    # if link_with_domain.upper() == p_to.upper():
    #     return(None)
    # if re.findall returned more than one item its an interconnect. Insert the interconnect password.
    if len(list_of_quoted_substrings) > 1:
        vinterconnect = list_of_quoted_substrings[1]
        debugg("\nconvert_link()...vinterconnect={}".format(vinterconnect))
        # get vinterconnect ( INTF_FIN ) password : pkg_pass(remote_db, vinterconnect)
        vinter_pass = pkg_pass(to_db_name.lower(), vinterconnect.lower())
        debugg("\nconvert_link()...vinterconnect={} pass={}".format(vinterconnect, vinter_pass))
        if p_cre_stmt.find("{pwd}") != -1 and vinter_pass:
            ins_pwd = "\"{}\"".format(vinter_pass)
            p_cre_stmt = p_cre_stmt.replace("{pwd}",ins_pwd)
            debugg("\nNEW CREATE STMT WITH INTERCONNECT PASSWORD:\n\t{}".format(p_cre_stmt))
        else:
            temp_msg = "ERROR RETRIEVING INTER CONNECT PASSWORD FOR {} PASSWORD {}.".format(vinterconnect or "Empty!", vinter_pass or "Empty!")
            add_to_msg(temp_msg)
            debugg(temp_msg)
            fail_module("#6")

    # get domain : by finding first period in link with domain and taking the period and everything after that : .DR.CRU.ORG
    vdomain = link_with_domain[link_with_domain.find("."):]
    # CRMTMP
    vdb_name = link_with_domain.split(".")[0]
    debugg("")
    # now it should be easy to replace values in the string
    # ========== conversion happens here ==========
    if vdomain.upper() != to_domain.upper():
        # This replaces every occurrance of the domain
        p_cre_stmt = p_cre_stmt.replace(vdomain.upper(), to_domain.upper())

    if vdb_name.upper() != to_db_name.upper():
        # this replaces every occurrance of the db name
        p_cre_stmt = p_cre_stmt.replace(vdb_name.upper(), to_db_name.upper())

    return(p_cre_stmt)


def convert_to_drop(p_cre_stmt):
    """
    Create a drop statement from a create statement
        p_cre_stmt = "CREATE DATABASE LINK \"CRMTMP.DR.CRU.ORG\" CONNECT TO \"INTF_FIN\" IDENTIFIED BY {pwd} USING 'CRMTMP.DR.CRU.ORG'"
    convert to drop and return:
        DROP DATABASE LINK CRMTMP.DR.CRU.ORG
        or
        DROP PUBLIC DATABASE LINK CRMTMP.DR.CRU.ORG
    """
    debugg("convert_to_drop...starting...p_cre_stmt={}".format(p_cre_stmt))

    v_second_quote = find_nth(p_cre_stmt, '"', 2)
    working_stmt = p_cre_stmt[:v_second_quote]
    # working_stmt = 'CREATE DATABASE LINK "CRMTMP.DR.CRU.ORG'
    tmp_stmt = working_stmt.replace('"',"")
    final_stmt = tmp_stmt.upper().replace("CREATE","DROP")
    return(final_stmt)


def find_nth(haystack, needle, n):
    start = haystack.find(needle)
    while start >= 0 and n > 1:
        start = haystack.find(needle, start+len(needle))
        n -= 1
    return start


def redo_synonyms_using_links(cur, oldlink, newlink, owner):
    """

    now recompile all the associated synonyms:
    use_cur:
    oldlink = "CRMP.CCCI.ORG"
    newlink = "CRMTMP.DR.CRU.ORG"
    owner: FINADM or all
    ** if owner is all. Redo all synonyms that match regardless of owner.

    """
    debugg(20*"-")
    debugg("\nredo_synonyms_using_links()...\n")
    debugg("\nredo_synonyms_using_links()...starting...\noldlink={}\nnewlink={}".format(oldlink, newlink))

    if owner.lower() != "all":
        cmd_str = """select 'create or replace synonym {owner}.'||synonym_name||' for '||table_owner||'.'||table_name||'@{new}' from dba_synonyms where db_link = '{old}' and owner = '{owner}' order by db_link""".format(owner=owner.upper(), new=newlink.upper(), old=oldlink.upper())
    else:
        cmd_str = """select 'create or replace synonym '||owner||'.'||synonym_name||' for '||table_owner||'.'||table_name||'@{new}' from dba_synonyms where db_link = '{old}' order by db_link""".format(new=newlink.upper(), old=oldlink.upper())

    debugg("\nredo_synonyms_using_links().....\n\tcmd_str={}".format(cmd_str))
    cre_new_syns_cmds = execute_cmd(cur, cmd_str, "yes")
    # debugg("execute_cmd returned:\n\tcre_new_syns_cmds={}".format(str(cre_new_syns_cmds)))
    for each_syn in cre_new_syns_cmds:
        debugg("calling execute_cmd() with each_syn={}".format(each_syn))
        execute_cmd(cur, each_syn, "no")

    return()


def get_old_syn_link(cur, owner, vlink_filter, map_to):
    """
    Sometimes some of the links area already processed.
    get the ones that aren't
    you need the filter, cur and map_to

    """
    debugg("\nget_old_syn_link()....starting...with parameters:\n\tvlink_filter={}\n\tmap_to={}\n".format(vlink_filter, map_to))

    if owner.lower() != "all":
        cmd_str = "select unique(db_link) from dba_synonyms where owner = '{owner}' and db_link like '%{linkfilter}%' and db_link != '{newlink}'".format(owner=owner.upper(), linkfilter=vlink_filter.upper(), newlink=map_to.upper())
    else:
        cmd_str = "select unique(db_link) from dba_synonyms where db_link like '%{linkfilter}%' and db_link != '{newlink}'".format(linkfilter=vlink_filter.upper(), newlink=map_to.upper())

    debugg("get_old_syn_link()...passing cmd_str={} to execute_cmd()".format(cmd_str))
    output = execute_cmd(cur, cmd_str, "yes")
    debugg("\nget_old_syn_link()...\n\toutput={} len={}".format(str(output), len(output)))

    if len(output) == 1:
        return(output[0])
    else:
        debugg("get_old_syn_link()...output={} len={}".format(str(output), len(output)))
        # fail_module("#7")
        return([])


def do_synonyms(cur, owner, vlink_filter, vmap_to):
    """
    wrapper for redo_synonyms_using_links()
    """
    # if owner not Public there are synonyms to process
    if owner.upper() == "PUBLIC":
        return()

    # recompile/deploy all synonyms using the old db links
    # quick way to extract the db link string get_old_syn_link(cur, owner, vlink_filter, map_to):
    oldlink = get_old_syn_link(cur, owner, vlink_filter, vmap_to)
    if not oldlink:
        return()
    # oldlink = re.findall('"([^"]*)"', item)[0]
    newlink = vmap_to
    debugg("\noldlink={}\nnewlink={}\n".format(oldlink, newlink))
    debugg("\ndo_synonyms() :: calling redo_synonyms_using_links()\nwith owner={}\noldlink={}\nnewlink={}".format(owner or "Empty!", oldlink or "Empty!", newlink or "Empty!"))
    # if these 3 things are not None you have what you need to process synonyms
    if owner and oldlink and newlink:
        # using sys cursor since select hits dba_synonyms table.
        debugg("MAIN::calling redo_synonyms_using_links() with oldlink={} newlink={} and owner={}".format(oldlink, newlink, owner))
        redo_synonyms_using_links(cur, oldlink, newlink, owner)
        debugg("MAIN :: back from redo_synonyms_using_links()")
    else:
        temp_msg = "ERROR or Nothing to process: error doing synonyms for non-public owner {}. paramter missing: owner: {} oldlink: {} newlink: {}".format(owner or "Empty!", oldlink or "Empty!", newlink or "Empty!")
        debugg()

    return("success")
# ==============================================================================
# =================================== MAIN =====================================
# ==============================================================================

def main ():
    """
    Run a sql command string
    """
    global msg
    global default_refname
    global debugme
    global db_home_name
    global cru_domain
    global dr_domain
    global affirm
    global default_ignore
    global module
    global vignore
    global proxy_user
    is_rac = None
    ignore_err_flag = False
    return_values = []
    owner_cur = None


    # print("here")

    module = AnsibleModule(
      argument_spec = dict(
        user_id         =dict(required=True),
        user_password   =dict(required=True),
        proxy_user      =dict(required=True),
        proxy_pwd       =dict(required=True),
        db_name         =dict(required=True),
        host            =dict(required=True),
        is_rac          =dict(required=True),
        link_filter     =dict(required=False),
        map_to          =dict(required=False),
        link_owner      =dict(required=False),
        expect_results  =dict(required=False),
        refname         =dict(required=False),
        ignore          =dict(required=False),
        debugging       =dict(required=False)
      ),
      supports_check_mode=False,
    )

    # Get arguements passed from Ansible playbook
    # user name / password and db to connect to on host
    vuid           = module.params.get('user_id')
    vpass          = module.params.get('user_password')
    vproxyid       = module.params.get('proxy_user')
    vproxypwd      = module.params.get('proxy_pwd')
    vdb            = module.params.get('db_name')
    vhost          = module.params.get('host')
    visrac         = module.params.get('is_rac')
    vlink_filter   = module.params.get('link_filter')
    vmap_to        = module.params.get('map_to')
    vlink_owner    = module.params.get('link_owner')
    vxpect         = module.params.get('expect_results')
    vrefname       = module.params.get('refname')
    vignore        = module.params.get('ignore')
    vdebug         = module.params.get('debugging')

# ========= BEGIN CHECKING PARAMETERS ===========

    vuid = "sys"

    if vdebug in affirm:
        debugme = True
        set_debug_log()
    else:
        debugme = False
    debugg(50*"\n")
    debugg(100*"=")
    debugg("\nSTART DEBUGGING SET: debugme={}\n".format(debugme))

    if vignore is None:
        vignore = default_ignore
    else:
        vignore = vignore

    if not vrefname:
        refname = default_refname
    else:
        refname = vrefname

    # This has to be here, cannot reference before setting refname
    ansible_facts = { refname : { } }

    if "org" in vhost:
        abbr_host = vhost.replace(cru_domain,"").replace(dr_domain, "")
    else:
        add_to_msg("Fully qualified hosts are required. Please add the correct domain to the hostname. {}".format(vhost))
        # fail_module(p_module, p_ansible_facts, p_mode=None):
        fail_module("#8")

    if not visrac:
        visrac = israc(vhost)

    if not cx_Oracle_found:
        tmp_msg="Error: cx_Oracle module not found. Unable to proceed. Please install using pip, brew or other python package manager and try again."
        debugg(tmp_msg)
        add_to_msg(tmp_msg)
        fail_module("#9")

    debugg("\nvignore={}\nabbr_host={}\nhost={}\nisrac={}\ndebugme={}\n".format(vignore or "None", abbr_host or "None", vhost or "None", visrac or "None", debugme or "None"))

    # check required vars passed in are not NULL.
    if (vdb is None) or (vhost is None) or (vuid is None or vpass is None) or (vproxyid is None or vproxypwd is None) or (visrac is None):
        temp_msg = "\ndb name: {}\nhost: {}\nuser name: {}\nproxy_user = {}\ncmd = {}\nisrac = {}".format( vdb  or "Empty!", vhost  or "Empty!", vuid  or "Empty!", vproxyid or "Empty!", vcmd_str or "Empty!", visrac or "Empty!")
        # add_to_msg(temp_msg)
        debugg("\nrequired parameter missing...exiting....\n\t{}".format(temp_msg))
        fail_module("#10")

# ========= END CHECKING PARAMETERS ===========

# ========= BEGIN CREATING SYS cx_Oracle DB CONNECTION ===========

    # create dsn_tns
    dsn_tns = create_dsn(vhost, vdb)
    # pkg_pass(db, user, pri_filter=None, sec_filter=None)
    vpass = pkg_pass(vdb.lower(), vuid.lower())
    debugg("linkmapper :: MAIN :: dsn_tns created: {} vuid {} vpass {}".format(str(dsn_tns), vuid or "Empty!", vpass or "Empty!"))

    # create cx_Oracle connection create_conn(p_dsn, p_uid, p_pwd):
    cur = create_conn(dsn_tns, vuid, vpass)
    debugg("cx_oracle connection created.")

# ========= END CREATING SYS cx_Oracle DB CONNECTION ===========

# ========= BEGIN DB LINK COMMANDS ===========
    # execute_cmd(p_cur, p_cmd_str, p_expect):
    # Get all schema owners with db links
    if vlink_owner.lower() == "all":
        cmd_str = "select unique(owner) from dba_db_links where owner not in ('SYS') order by owner"
    else:
        cmd_str = "select unique(owner) from dba_db_links where owner = '{owner}'".format(owner=vlink_owner.upper())

    # def execute_cmd(db conn cursor, cmd_str to execute, expect results?) => ['FINADM', 'PUBLIC']
    debugg("\ncalling execute_cmd with cmd_str={}".format(cmd_str))
    results = execute_cmd(cur, cmd_str, "yes")
    debugg("\nMAIN :: results = {}".format(results))
    owners_and_cmds = { "syns_only_owners" : None }

    # Check for synonym only owners which may use PUBLIC LINK and need remapped.
    debugg("\nChecking for synonym only owners if vlink_owner: [{}] == 'all' or vlink_owner [{}] not in db_link_owners [{}]\n".format(vlink_owner, vlink_owner, str(results) or "None"))
    if vlink_owner.lower() == "all" or vlink_owner not in results:
        debugg("CK PASSED: Looking for synonym only owners.")
        cmd_str = """select unique(owner) from dba_synonyms where db_link like '%{filter}%' and db_link IS NOT NULL""".format(filter=vlink_filter.upper())
        syn_results = execute_cmd(cur, cmd_str, "yes")
        if results and syn_results:
            syn_only_owners = list(set(syn_results) - set(results))
        elif syn_results:
            syn_only_owners = syn_results
            add_to_msg("User(s) {} not found in database link owners but in synonym owners only.".format(str(syn_only_owners)))
        if syn_only_owners:
            debugg("syn_only_owners={}".format(str(syn_only_owners)))
            owners_and_cmds["syns_only_owners"] = syn_only_owners

    debugg("\nstarting FOR LOOP iterating over results = {}".format(str(results) or "None!"))
    # For each db link owner and each db link, generate creation scripts
    for owner in results:
        debugg("\nfor owner: {} if vlink_owner {} not None and vlink_owner being processed is != owner: {} and vlink_owner!=all skip".format(owner, vlink_owner or "None", owner))
        # if a link owner was specified and it's not 'all' and its not the one we're processing move on to the next
        if ( vlink_owner and vlink_owner.lower() != owner.lower() ) and vlink_owner.lower() != "all":
            debugg("\nSKIPPING...{}\n".format(owner))
            continue

        # if owner cursor is defined it's from last run, close it.
        if owner_cur:
            try:
                owner_cur.close()
            except:
                pass
            owner_cur = None

        debugg("TOP OF THE FOR LOOP OWNER=>{}".format(owner))
        if owner not in owners_and_cmds.keys():
            owners_and_cmds.update( { owner : [] } )
            debugg("\nFirst time for {} ... \n\tadding owner to owners_and_cmds dictionary [{}]".format(owner, str(owners_and_cmds)))
            if owner.upper() != "PUBLIC":
                cmd_str = "alter user {owner} grant connect through {proxy}".format(owner=owner, proxy=vproxyid)
                debugg("executing cmd to grant connect to {} through user {}".format(owner, vproxyid))
                execute_cmd(cur, cmd_str, "no")
                debugg("----->>> connect to {} as proxy through {} granted...".format(owner, vproxyid))
                # system_pass = pkg_pass(vdb.lower(), "system")
                vproxy_con_str = "{prxy}[{owner}]".format(prxy=vproxyid, owner=owner)
                owner_cur = create_conn(dsn_tns, vproxy_con_str, vproxypwd)

        debugg("\nLOOPING FOR OWNER: {}\nowners_and_cmds = {}\nmap_to={}".format(owner, str(owners_and_cmds), vmap_to))
        if owner != "PUBLIC":
            if vlink_filter:
                # NON-PUBLIC
                cmd_str = """select 'CREATE DATABASE LINK "'||db_link||'" CONNECT TO "'||username||'" IDENTIFIED BY {{pwd}} USING '''||db_link||'''' from dba_db_links where owner = '{owner}' and db_link like '%{filter}%'""".format(owner=owner.upper(),filter=vlink_filter.upper())
        elif owner == "PUBLIC":
            # PUBLIC
            cmd_str = """select 'CREATE PUBLIC DATABASE LINK "'||db_link||'" USING '''||db_link||'''' from dba_db_links where owner = 'PUBLIC' and db_link like '%{}%'""".format(vlink_filter.upper())
        else:
            # remapping synonyms only ?
            synonyms_only = True
        debugg("\nexecuting owner=>{}\ncmd_str=>{}".format(owner, cmd_str))
        # Execute the command to create db links for existing db links
        output = execute_cmd(cur, cmd_str, "yes")
        debugg("\n\toutput={}".format(output))
        # run the script to generate a create script and append it to a dictionary ( owners_and_cmds ) under the users name
        for item in output:
            debugg("\nFOR LOOP WITH ITEM: \n\t{}".format(item))
            # convert_link(p_filter, p_cre_stmt, p_to, p_db):
            converted_cre_stmt = convert_link(vlink_filter, item, vmap_to, vdb)
            debugg("MAIN :: convert_link()\nreturned {}\nIf converted_cre_stmt..append to owners_and_cmds[{}]".format(str(converted_cre_stmt),owner))

            # if convert_link returns None its already converted from a previous run or something.
            if converted_cre_stmt:
                debugg("INSIDE THE IF converted_cre_stmt: statment where DROP AND CREATE HAPPEN")
                owners_and_cmds[owner].append(converted_cre_stmt)
                drop_stmt = convert_to_drop(item)
                owners_and_cmds[owner].append(drop_stmt)
                debugg("\nDROP AND CREATE STATEMENTS:\nDROP=>{}\nCREATE=>{}\nowners_and_cmds={}".format(drop_stmt or "Empty!", converted_cre_stmt or "Empty!", str(owners_and_cmds)))
                # if owner other than public drop and create as schema owner otherwise sys ok.
                if owner.upper() != "PUBLIC":
                    use_cur = owner_cur
                else:
                    # else just use sys cursor
                    use_cur = cur
                    # now that we have both drop and create:
                execute_cmd(use_cur, drop_stmt, "no")          # don't expect rows back from a drop.
                debugg("MAIN :: POST DROP STATMENT")
                execute_cmd(use_cur, converted_cre_stmt, "no") # don't expect rows back from a create.
                debugg("MAIN :: POST CREATE STATMENT")
            else:
                debugg("**** SKIPPED **** THE INSIDE THE IF converted_cre_stmt: statment where DROP AND CREATE HAPPEN")

            # SYNONYMS ========================================
            # if owner not Public there are synonyms to process
            # def do_synonyms(cur, owner, vlink_filter, map_to):
            do_synonyms(cur, vlink_owner, vlink_filter, vmap_to)

        debugg("============ BOTTOM OF THE FOR LOOP FINISHED PROCESSING: {} ============".format(owner))

    if owners_and_cmds["syns_only_owners"] and vlink_owner.lower() != "all":
        debugg("processing synonym only users...")
        for soo in owners_and_cmds["syns_only_owners"]:
            do_synonyms(cur, soo, vlink_filter, vmap_to)

    debugg("REMAP COMPLETE! ....owners_and_cmds = {}".format(owners_and_cmds))
    ansible_facts.update({ refname : return_values, 'results': {'cmd': cmd_str, 'success': True, 'output': owners_and_cmds } } )
    m = "module: success. query output: {}".format(str(owners_and_cmds))
    module.exit_json( msg=m, ansible_facts=ansible_facts , changed=True)

# code to execute if this program is called directly
if __name__ == "__main__":
    main()
