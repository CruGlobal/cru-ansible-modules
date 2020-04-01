#!/usr/bin/env python3

from ansible.module_utils.basic import *
from ansible.module_utils.facts import *
import subprocess
import sys
import os
import json
import re
import math
import string
import commands
from subprocess import (PIPE, Popen)

try:
    import cx_Oracle
    cx_Oracle_found = True
except ImportError:
    cx_Oracle_found = False

# Created by: S Kohler
# Date: May 20, 2019

ANSIBLE_METADATA = {'status': ['stableinterface'],
                    'supported_by': 'Cru DBA team',
                    'version': '0.1'}

DOCUMENTATION = '''
---
module: sectblcnt
short_description: PS Admin Security Table Count

notes: Returned the value of security tables in the PS Admin schema
requirements: [ python2.* ]
author: "DBA Oracle module Team"
'''

EXAMPLES = '''

    # if refreshing this will check that the defined list of PS security tables
    # was saved off to the ps_admin (Asiu) for datapump export prior to
    # deleting the database, so that they can be restored after refresh.

    - local_action:
        module: sectblcnt
        ps_admin: "{{ ps_admin }}" (1)
        table_list: "{{ security_table_list }}" (1)
        systempwd: "{{ database_passwords[source_db_name].system }}"
        db_name: "{{ dest_db_name }}"
        host: "{{ dest_host }}"
        rac: "{{ is_rac }}" or "{{ sourcefacts['cluster_database'] }}"
        refname: "{{ refname_str }}" (2)
        ignore: True (3)
        debugmode: True
      become: yes
      become_user: "{{ utils_local_user }}"
      register: sec_tbl_count
      when: master_node

      (1) ps_admin, table_list and num_sec_tables - are defined in
          vars/utils/utils_env.yml
          num_sec_tables is used after the count is obtained to fail if
          the count is less than expected.
          Fail when:
          - sectblcount[ps_admin]['security_table_count'] < num_sec_tables

      (2) refname - can be defined to refer to the output later. The default
          is 'sectblcount' ( see above Fail when statement )
          but the user can define anything.

      (3) ignore - (connection errors) is optional. If you know the source
          database may be down set ignore: True. If connection to the
          source database fails the module will not throw a fatal error
          to stop the play and continue. However, not if the result is critical.


'''


default_refname = "sectblcount"
msg = ""
errmsg = ""
debugme = False
debug_log = ""
cru_domain = ".ccci.org"
affirm = [ 'True', 'TRUE', 'true', True, 'T', 't', 'true', 'Yes', 'YES', 'Y', 'y', 'yes']
utils_settings_file = os.path.expanduser("~/.utils")
new_hw = ['pldataw' + cru_domain,
          'sldataw' + cru_domain,
          'slrac1' + cru_domain,
          'slrac2' + cru_domain,
          'tlrac1' + cru_domain,
          'tlrac2' + cru_domain]


def whichsam(host):
    """ given a database host
        decide which user id to use sam or samk
        for ssh commands
        could also compare the last digit of the hostame
        rac uses 01, or new uses 1
        dw uses 60 or no number for new hw.
    """
    debugg("whichsam()...starting...")
    global new_hw
    global cru_domain

    if cru_domain not in host:
        host = host + cru_domain

    if host in new_hw:
        debugg("sam :: host={} in new_hw={}".format(host,str(new_hw)))
        return("sam")
    else:
        debugg("samk :: host={} not in new_hw={}".format(host, str(new_hw)))
        return("samk")


def msgg(info):
    """ Given a snippet of info as a string add it to the msg string """
    global msg
    if msg:
        msg = msg + " " + info
    else:
        msg = info


def host_is_reachable(host):
    """ PING the host and determine if its reachable """
    global cru_domain

    if cru_domain not in host:
        host = host + cru_domain

    cmd_str = "/sbin/ping -c 1 %s" % (host)
    output = run_local(cmd_str)
    if 'PING' in output:
        return(True)
    else:
        return(False)


def errmsgg(info):
    """ add this info to the errmsg string """
    global errmsg
    global debugme

    if debugme:
        msgg(info)

    if errmsg:
        errmsg = errmsg + " " + info
    else:
        errmsg = info


def set_debug_log():
    """ Set the debug_log value to write debugging messages to """
    global utils_settings_file
    global debug_log

    try:
        with open(utils_settings_file, 'r') as f1:
            line = f1.readline()
            while line:
                if 'ans_dir' in line:
                    tmp = line.strip().split("=")[1]
                    debug_log = tmp + "/bin/.utils/debug.log"
                    return(debug_log)

                line = f1.readline()
    except:
        print("ans_dir not set in ~/.utils unable to write debug info to file")
        pass

    debugg("set_debug_log()...returning....debug_log={}".format(debug_log))
    return("")


def debugg(inStr):
    """If debugme is True add debugging info to msg"""
    global debugme

    if debugme:
        msgg(inStr)
        write_to_file(inStr)


def write_to_file(info):
    """ Write this info to the out_file """
    global debug_log
    global debugme

    if not debug_log:
        return

    try:
        with open(debug_log, 'a') as f:
            f.writeline(info + "\n")
    except:
        pass

    return


def convertToTuple(inStr):
    """Convert table_list parameter string to tuple"""
    tmp = inStr.split(",")
    return(tuple(tmp))


def whoami():
    """Run whoami on the localhost to
       get the username for tailing the RMAN log later or
       and tasks that require a local username
    """
    cmd_str = "whoami"
    output = run_local(cmd_str)
    return(output)


def run_remote(cmd_str, host):
    """ given a command and host string, run the command on the remote host
    """
    global cru_domain
    global errmsg

    debugg("run_remote()...starting....")

    if not host_is_reachable(host):
        debugg("run_remote() :: run_remote :: Error: host {} is not reachable".format(host))
        msgg("Error: run_remote() Host {} not reachable.".format(host))
        return

    _whoami = whoami()
    if 'sam' in _whoami:
        sshUser = whichsam(host)
    else:
        sshUser = _whoami
    debugg(" run_remote() .. sshUser={} ".format(sshUser))

    if cru_domain not in host:
        host = host + cru_domain

    debugg("#1 run_remote() .. host={} ".format(host))
    try:
        cmd = "ssh %s@%s %s" % (sshUser, host, cmd_str)
        debugg("#2 cmd={} host={} sshUser={} ".format(cmd, host, sshUser))
        output = commands.getstatusoutput(cmd)
        # output = subprocess.run(["ssh", sshUser + "@" + host, cmd_str], shell=False, stdout=subprocess.PIPE,
        #                         stderr=subprocess.PIPE)
        # output, code = process.communicate()
    except:
        errmsgg("Error: run_remote(%s) :: cmd_str=%s" % (sys.exc_info()[0], cmd_str))
        errmsgg("Meta:: %s, %s, %s %s" % (sys.exc_info()[0], sys.exc_info()[1], msg, sys.exc_info()[2]))
        raise Exception(errmsg)

    output = output[1].strip()
    debugg("#3 run_remote()...output={}".format(output))

    if str(output):
        debugg("#4 redologs :: run_remote() ...returning output = {}".format(output))
        return(output)
    else:
        return("")


def is_rac(host):
    """Determine if the host this is running on is
       part of a RAC installation with other nodes
       or a single instance host.
       return True or False
    """
    global israc

    debugg("is_rac_host() ...starting...")
    cmd_str = "ps -ef | grep lck | grep -v grep | wc -l"
    debugg("is_rac() cmd_str = {}".format(cmd_str))
    # results = run_local(cmd_str)
    results = run_remote(cmd_str, host)
    debugg("is_rac_host() results = %s".format(results))
    print("is_rac_host() results = %s".format(results))

    if results and int(results) > 0:
        debugg("is_rac_host()...exiting....returning True")
        israc = True
        return(True)
    else:
        debugg("is_rac_host()...exiting....returning False")
        israc = False
        return(False)


def run_local(cmd_str):
    """
       Encapsulate the error handling in this function.
       Run the command (cmd_str) on the remote host and return results.
    """
    global errmsg

    debugg("run_local()....start...cmd_str=%s" % (cmd_str))

    if cmd_str:

        try:
          process = subprocess.Popen([cmd_str], stdout=PIPE, stderr=PIPE, shell=True)
          output, code = process.communicate()
        except:
           errmsgg(' Error: srvctl module get_node_num() error - retrieving node_number excpetion: %s' % (sys.exc_info()[0]))
           errmsgg("%s, %s, %s" % (sys.exc_info()[0], sys.exc_info()[1], sys.exc_info()[2]))
           debugg(errmsg)
           raise Exception (errmsg)

        debugg("run_local()....exiting....output=%s" % (str(output)))
        return(output.strip())

    else:
        debugg("run_local()....exiting....return=None")
        return(None)


# ==============================================================================
# =================================== MAIN =====================================
# ==============================================================================
def main ():
    """ Return the number of security tables owned by the PS Admin schema"""
    global msg
    global default_refname
    global debugme
    global debug_log
    global cru_domain
    global affirm

    vchanged = False
    ansible_facts={}

    # Name to call facts dictionary being passed back to Ansible
    # This will be the name you reference in Ansible. i.e. source_facts['sga_target'] (source_facts)
    refname = ""

    # os.system("/usr/bin/scl enable python27 bash")
    # os.system("scl enable python27 bash")

    module = AnsibleModule(
      argument_spec = dict(
        ps_admin        =dict(required=True),
        systempwd       =dict(required=True),
        table_list      =dict(required=True),
        db_name         =dict(required=True),
        host            =dict(required=True),
        rac             =dict(required=True),
        refname         =dict(required=False),
        ignore          =dict(required=False),
        debugmode      =dict(required=False)
      ),
      supports_check_mode=False,
    )

    # Get arguements passed from Ansible playbook
    vpsadmin  = module.params.get('ps_admin')
    vdbpass   = module.params.get('systempwd')
    vtblList  = module.params.get('table_list')
    vdb       = module.params.get('db_name')
    vdbhost   = module.params.get('host')
    vrac      = module.params.get('rac')
    vrefname  = module.params.get('refname')
    vignore   = module.params.get('ignore')
    vdebugme  = module.params.get('debugmode')


    if vdebugme in affirm:
        debugme = True
        debug_log = set_debug_log()
    else:
        debugme = False

    if vrac in affirm:
        rac = True
    else:
        rac = False

    if vignore in affirm:
      vignore = True
    else:
      vignore = False

    if '.org' in vdbhost:
        vdbhost = vdbhost.replace(cru_domain,'')

    if not cx_Oracle_found:
        module.fail_json(msg="Error: cx_Oracle module not found")

    if not vrefname:
        refname = default_refname
    else:
        refname = vrefname

    if vtblList is None:
        module.fail_json(msg="Error: a string containing tables must be provided: \"table1,table2\" etc. ")
    else:
        secTblList = convertToTuple(vtblList)

    debugg("secTblList = %s" % (str(secTblList)))

    # check vars passed in are not NULL. All are needed to connect to source db
    if ( vdbpass is not None) and (vdb is not None) and (vdbhost is not None) and (vpsadmin is not None):

        # vdb = vdb + vdbhost[-1:]
        # can use: service_name = db.ccci.org or sid = db1
        if is_rac(vdbhost):
            vsid = vdb + vdbhost[-1:]
        else:
            vsid = vdb

        if cru_domain not in vdbhost:
            vdbhost = vdbhost + cru_domain

        try:
            dsn_tns = cx_Oracle.makedsn(vdbhost, '1521', vsid)
        except cx_Oracle.DatabaseError as exc:
            error, = exc.args
            if vignore:
                msg = "Failed to create dns_tns: %s" %s (error.message)
            else:
                module.fail_json(msg='TNS generation error: %s, db name: %s host: %s' % (error.message, vsid, vdbhost), changed=False)

        try:
          con = cx_Oracle.connect('system', vdbpass, dsn_tns)
        except cx_Oracle.DatabaseError as exc:
            error, = exc.args
            if vignore:
                msgg("DB CONNECTION FAILED : %s" % (error.message))
                if debugme:
                    msgg(" vignore: %s " % (vignore))
                    module.exit_json(msg=msg, ansible_facts=ansible_facts, changed="False")
            else:
                msgg('Database connection error: %s, tnsname: {} host: {}'.format(error.message, vsid, vdbhost))
                module.fail_json(msg=msg, changed=False)

        cur = con.cursor()

        debugg("about to run select for secTblList = %s parameter table list vtblList = %s " % (secTblList, vtblList))
        # select source db version
        try:
            cmd_str = 'select count(*) from dba_objects where owner = \'%s\' and object_type = \'TABLE\' and object_name in %s' % (vpsadmin.upper(),str(secTblList))
            debugg("cmd_str = %s" % (cmd_str))
            cur.execute(cmd_str)
        except cx_Oracle.DatabaseError as exc:
            error, = exc.args
            msgg('Error selecting version from v$instance, Error: %s' % (error.message))
            module.fail_json(msg=msg, changed=False)

        dbver =  cur.fetchall()
        vsectblecnt = dbver[0][0]
        ansible_facts[refname] = { vpsadmin: { 'security_table_count':vsectblecnt } }

        msgg("module completed successfully.")

        module.exit_json( msg=msg, ansible_facts=ansible_facts , changed=vchanged)

    else:

        if vdb is None:
            msgg("Required parameter database name not defined.")

        if vdbhost is None:
            msgg("Required parameter host not defined.")

        if vdbpass is None:
            msgg("Required database password not defined.")

        if vpsadmin is None:
            msgg("Required PS Admin not defined.")

        msgg('Error closing cursor: Error: %s' % (msg))

        module.fail_json(msg=msg, changed=False)

# code to execute if this program is called directly
if __name__ == "__main__":
   main()
