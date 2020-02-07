#!/opt/rh/python27/root/usr/bin/python
# # -*- coding: utf-8 -*-

# Created: April 27, 2019
# by: Sam Kohler
# Purpose:
#   Getting inconsistant backups from utils backup even though the database
#   is being shutdown and restarted mount before backup. Creating this module
#   to flush redo logs to archivelogs before running backup to ensure archives
#   are written to disk for the backup to ensure recovery to the current point
#   in time.
#
#   Also giving the module the ability to resize redo logs if needed since the
#   processes are similar and a logical place to put both.
#
#   ansible-playbook test.yml -i cru_inventory --extra-vars="hosts=test_rac dest_db_name=tstdb dest_host=tlorad01" --step -vvv
#
#   To run this module the following variables must be defined:
#           dest_db_name, dest_host, function (flush, resize), local_user
#
#  Errors complaining that module not defined usually come from functions that
#  don't have access to the "module" that was created in the main body of the
#  program
#

import ansible.module_utils
from ansible.module_utils.basic import *
from ansible.module_utils.facts import *
from ansible.module_utils._text import to_native
from ansible.module_utils._text import to_text
from ansible.module_utils.basic import AnsibleModule
import subprocess
import sys
import os
import json
import re
import math
import string
import time
import copy

try:
    import cx_Oracle
    cx_Oracle_found = True
except ImportError:
    cx_Oracle_found = False


ANSIBLE_METADATA = {'status': ['stableinterface'],
                    'supported_by': 'Cru DBA team',
                    'version': '0.3'}

DOCUMENTATION = '''
---
module: redologs
short_description: Two functions: 1) flush and 2) resize
notes: Flush used to force all redo logs to write before backup to ensure
       recovery to point in time is possible.
       Resize to resize redo logs when needed.
requirements: [ python2.* ]
author: "DBA Oracle module Team"
'''

EXAMPLES = '''

  - name: Flush redo logs
    local_action:
        module: redologs
        connect_as: system
        userpwd: "{{ database_passwords[dest_db_name].system }}"
        db_name: "{{ dest_db_name }}"
        db_host: "{{ dest_host }}"
        function: flush
        size:
        units:
	    israc: "{{ destfacts['israc'] }}"
        ignore: true
        refname:
	    debug_mode:
    become_user: "{{ local_user }}"
    register: redo_run

  - name: Resize redo logs
    local_action:
        module: redologs
        userpwd: "{{ database_passwords[dest_db_name].system }}"
        db_name: "{{ dest_db_name }}"
        db_host: "{{ dest_host }}"
        function: resize
        size: 500
        units: m
        israc: "{{ destfacts['israc'] }}"
        ignore: false
        refname:
	debugmode:
    become_user: "{{ local_user }}"
    register: redo_run

  Notes:

    connect_as - Not required. default system.
    size and units are not required for "flush" but are for resize.
    units are single letter: k (kilobytes), m (megabytes), g (gigabytes) etc.
    ignore - tells the module whether to fail on error and raise it or pass on error
             and continue with the play. Default is to fail.

'''
# Global Vars:
itemsToMatch = 0
msg = ""
errmsg = ""
debugme = False
debugFile = ""
g_vignore = False
ansible_facts = {}
module_fail = False
module_exit = False
israc = False
def_reference_name = 'redologs'
affirm = ['True', 'TRUE', 'true', True, 'YES', 'Yes', 'yes',  't', 'T', 'y', 'Y', 'On', 'ON', 'on']
cru_domain = ".ccci.org"
new_hw = ['pldataw' + cru_domain,
          'sldataw' + cru_domain,
          'slrac1' + cru_domain,
          'slrac2' + cru_domain,
          'tlrac1' + cru_domain,
          'tlrac2' + cru_domain]

#      THREAD#	   GROUP#      SIZE_MB       STATUS		  ARC     MEMBER
# ------------ ------------ ------------ ---------------- ---   ----------------------------------------------------------------------
#	   1		    1	          50         ACTIVE		  YES   +FRA/TSTDB/ONLINELOG/group_1.28504.1006772175
#
# So a dictionary like this should be passed into class redoLog
# { 'thread':1,'GROUP': 1, 'SIZE_MB':50, 'STATUS':'INACTIVE','ARCHIVED': 'YES','MEMBER': '+FRA/TSTDB/ONLINELOG/group_1.28504.1006772175'}
class redoLogClass:
    def __init__(self, redo_t):
        self.__redoThread__ = redo_t[0]
        self.__redoGroup__ = redo_t[1]
        self.__redoStatus__ = redo_t[3]
        self.__prevStatus__ = ""
        self.__startingStatus__ = redo_t[3]
        self.__redoSize__ = redo_t[2]
        self.__redoUnits__ = 'm'
        self.__archivedStatus__ = redo_t[4]
        self.__member__ = redo_t[5]
        self.__startObj__ = False
        self.setStartStatus()

    def setStartStatus(self):
        if self.__startingStatus__.lower() == "current" and self.__archivedStatus__.lower() == "no":
            self.__startObj__ = True
        else:
            self.__startObj__ = False

    def startingObj(self):
        return(self.__startObj__)

    def getStatus(self):
        return(self.__redoStatus__)

    def getPrevStatus(self):
        return(self.__prevStatus__)

    def getStartingStatus(self):
        return(self.__startingStatus__)

    def setStatus(self,newStatus):
        self.__prevStatus__ = self.getStatus()
        self.__redoStatus__ = newStatus

    def getThread(self):
        return(self.__redoThread__)

    def getGroup(self):
        return(self.__redoGroup__)

    def getSize(self):
        return(self.__redoSize__)

    def getUnits(self):
        return(self.__redoUnits__)

    def getArchived(self):
        return(self.__archivedStatus__)


def add_snippet(_snip, _str):
    """ Given a snippet of information as a string add it to another, longer,
        string and return the longer string.
    """

    if _str:
        _str = _str + " " + _snip
    else:
	_str = _snip

    return(_str)


def debugg(tidbit):
    global msg
    global errmsg
    global debugme
    global debugFile

    if debugme:
        msgg(tidbit)
        if debugFile:
            write_to_file(tidbit, debugFile)


def write_to_file(tidbit, afile):
    """ Write the string to the debug log if defined """

    if afile:
        try:
            with open(afile, 'a') as f:
                f.writeline(tidbit + "\n")
        except:
            print("Error: {} writing debugging information to log: {}")
            pass

    return


def msgg(tidbit):
    """Passed a string add it to the msg to pass back to the user"""
    global msg

    msg = add_snippet(tidbit, msg)


def errmsgg(tidbit):
    global errmsg

    errmsg = add_snippet(tidbit, errmsg)


def create_tns(vdbhost, vsid):
    global msg
    global g_vignore
    global module_fail
    global israc
    global cru_domain
    global affirm

    debugg("create_tns db={} host={} israc={}".format(vsid, vdbhost, israc))

    if cru_domain not in vdbhost:
        vdbhost = vdbhost + cru_domain

    debugg("creating dns with sid={} host={}".format(vsid, vdbhost))
    try:
      dsn_tns = cx_Oracle.makedsn(vdbhost, '1521', vsid)
    except cx_Oracle.DatabaseError as exc:
      error, = exc.args
      if g_vignore:
          msgg("create_tns() : Failed to create dns_tns: %s" %s (error.message))
          module_exit = True
      else:
          msgg('create_tns() : TNS generation error: %s, db name: %s host: %s' % (error.message, vsid, vdbhost))
          module_fail = True

    debugg("exit create_tns with : %s " % (dsn_tns))
    return(dsn_tns)


def create_con(vdbpass, dsn_tns, vconn_as):
    global msg
    global g_vignore
    global ansible_facts
    global module_fail
    global module_exit

    debugg("Connecting as : %s" % (vconn_as))

    try:
        if vconn_as != "sys":
            con = cx_Oracle.connect(dsn=dsn_tns, user=vconn_as, password=vdbpass)
        elif vconn_as == "sys":
            con = cx_Oracle.connect(dsn=dsn_tns,user='sys',password=vdbpass,mode=cx_Oracle.SYSDBA)
    except cx_Oracle.DatabaseError as exc:
        error, = exc.args
        if g_vignore:
            msgg("DB CONNECTION FAILED : %s" % (error.message))
            if debugme:
                msgg(" g_vignore: %s " % (g_vignore))
            module_exit = True
        else:
            msgg('Database connection error: %s, tnsname: %s host: %s' % (error.message, vdb, vdbhost))
            module_fail = True

    if not module_exit and not module_fail:
        cur = con.cursor()
        return(cur)


def host_is_reachable(host):
    """Ping the remote host to see if it's reachable (VPN check)"""

    debugg("redologs :: host_is_reachable() ...starting...")

    try:
        cmd_str = "/sbin/ping -c 1 %s" % (host)
        process = subprocess.Popen([cmd_str], stdout=PIPE, stderr=PIPE, shell=True)
        output, code = process.communicate()
    except OSError as e:
        metainfo = "Error [{} - {}] attempting to reach host {}. \n Please check your network connection and vpn".format(str(e.errno), e.strerror, self.env_host_hash[self.dbComboBox.currentText()])
        debugg("host_is_reachable() Error: running cmd_str={} meta: {}".format(cmd_str, metainfo))
        sys.exit()

    output = output.decode('utf-8')

    debugg("redologs :: host_is_reachable() :\n output={}".format(output))

    if 'PING' in str(output):
        debugg("redologs :: host_is_reachable() : ...exiting....returning True")
        return(True)
    else:
        debugg("redologs :: host_is_reachable() : ...exiting....returning False")
        return(False)


def whichsam(host):
    """ given a database host
        decide which user id to use sam or samk
        for ssh commands
        could also compare the last digit of the hostname
        rac uses 01, or new uses 1
        dw uses 60 or no number for new hw.
    """
    debugg(debugfile,"whichsam()...starting...")
    global new_hw
    global cru_domain

    if cru_domain not in host:
        host = host + cru_domain

    if host in new_hw:
        if debugfile: debugg(debugfile, "sam :: host={} in new_hw={}".format(host,str(new_hw)))
        return("sam")
    else:
        if debugfile: debugg(debugfile, "samk :: host={} not in new_hw={}".format(host, str(new_hw)))
        return("samk")


def ckrac(host):
    """ Determine if a host is running RAC or Single Instance
        This function broken out of bkpMain and passed the class' self.
        Other utils classes can call this and pass it self to determine if
        envComboBox is a rac db or not.
    """
    debugg("bkpMain :: ckrac() : ....starting....")

    cmd_str = "/bin/ps -ef | /bin/grep lck | /bin/grep -v grep | wc -l"

    output = run_remote(cmd_str, host)

    if int(output) > 0:
        # if > 0 "lck" processes running, it's RAC
        debugg("israc() returning True")
        return (True)
    else:
        debugg("israc() returning False")
        return (False)


def whoami():
    """Run whoami on the localhost to
       get the username for tailing the RMAN log later or
       and tasks that require a local username
    """
    global errmsg
    cmd_str = "whoami"
    output = run_local(cmd_str)
    return(output)


def run_remote(cmd_str, host):
    """ given a command and host string, run the command on the remote host
    """

    if not host_is_reachable(host):
        debugg("redologs :: run_remote :: Error: host {} is not reachable".format(host))
        msgg("Error: redologs :: run_remote() Host {} not reachable.".format(host))
        return

    _whoami = whoami()
    if 'sam' in _whoami:
        sshUser = whichsam(host)
    else:
        sshUser = _whoami
    debugg("redologs :: run_remote() .. sshUser={}")

    try:

        debugg("cmd_str={} host={} sshUser={}".format(cmd_str, host, sshUser))
        output = subprocess.run(["ssh", sshUser + "@" + host, cmd_str], shell=False, stdout=subprocess.PIPE,
                                stderr=subprocess.PIPE)
        # output, code = process.communicate()
    except:
        errmsgg("Error: israc({}) :: cmd_str={}".format(sys.exc_info()[0], cmd_str))
        errmsgg("Meta:: {}, {}, {} {}".format(sys.exc_info()[0], sys.exc_info()[1], msg, sys.exc_info()[2]))
        raise Exception(errmsg)

    if str(output):
        output = output.stdout.decode('utf-8')
        debugg("redologs :: run_remote() ...returning output = {}".format(output))
        return(output)
    else:
        return("")


def run_local(cmd_str):
    """ Run a command on the local host using the subprocess module.
    """

    debugg("run_local() ...starting... with cmd_str={}".format(cmd_str))

    try:
        process = subprocess.Popen([cmd_str], stdout=PIPE, stderr=PIPE, shell=True)
        output, code = process.communicate()
    except subprocess.CalledProcessError as e:
        errmsgg("redologs :: run_local() : [ERROR]: output = {}, error code = {}\n".format(e.output, e.returncode))
        debugg("redologs :: run_local() :: Error running cmd_str={} Error: {}".format(cmd_str,errmsg))

    results = output.decode('ascii').strip()
    debugg("redologs run_local()...exiting....output={} code={}".format(results, code))
    return(results)


# ==============================================================================
def redoFlushMain(cur):
    global msg
    global module_fail
    global module_exit
    global affirm
    num_laps = 2
    startingPoint = []
    statusNow = []

    debugg("redoFlushMain")

    startingPoint = curStatus(cur)
    at_start = backToStartingPoint(statusNow, startingPoint)
    # start forcing redo changes until this same redo thread group is current again.
    while not at_start and not module_exit and not module_fail and int(num_laps) != 0:
        advanceLogs(cur)
        time.sleep(1)
        statusNow = curStatus(cur)
        if at_start in affirm:
            num_laps -= 1
    return
    

def curStatus(cur):
    """Get current status of all redo logs and pass back a list of redoLogClass objects"""
    global g_vignore
    global module_fail
    global module_exit
    redoLog_l = []

    debugg("curStatus()")
    try:
        cmd_str = 'select l.thread#,l.group#,l.bytes/1024/1024 SIZE_MB,l.status,l.archived,lf.member from v$logfile lf, v$log l where lf.group#=l.group# order by l.thread#,group#'
        cur.execute(cmd_str)
    except cx_Oracle.DatabaseError as exc:
        error, = exc.args
        msgg('curStatus() : Error redo logs and status, Error: %s' % (error.message))
        response = { 'status':'Fail', 'Error': error.message, 'changed':'False'}
        if g_vignore:
            module_exit = True
        else:
            module_fail = True

    if not module_exit and not module_fail:
        allRedoLogs_l =  cur.fetchall() # Returns list of tuples

        # allRedoLogs_d = convert_to_dict(allRedoLogs_l)  # Dicts will provide flexiblity loading class objects

        # create a list of redoLog Objects
        for oneLog in allRedoLogs_l:
            # debugg("%s type: %s" % (str(oneLog),type(oneLog)))
            redoLog_l.append(redoLogClass(oneLog))

        debugg("exit curStatus() list of objects: %s" % (str(len(redoLog_l))))
        return(redoLog_l)


def advanceLogs(cur):
    """Advance redo thread to flush redo logs"""
    debugg("AdvanceLogs()")
    global g_vignore
    global module_fail
    global module_exit
    curStatus

    try:
        cmd_str = 'ALTER SYSTEM ARCHIVE LOG CURRENT'
        cur.execute(cmd_str)
    except cx_Oracle.DatabaseError as exc:
        error, = exc.args
        msgg('advanceLogs(): Error redo logs and status, Error: %s' % (error.message))
        response = { 'status':'Fail', 'Error': error.message, 'changed':'False'}
        if g_vignore:
            module_exit = True
        else:
            module_fail = True
    debugg("exiting AdvanceLogs()")


def backToStartingPoint(statusNow_l, origStartingPoint_l):
    """startingPoint is list of dictionaries containing the Thread#,Group#'s that had the status ARC=NO, STATUS=CURRENT at start
       force archive will continue until the starting point is reached. ( A complete circle is made and all redo logs
       are flushed. )"""
    global itemsToMatch
    global module_fail
    global module_exit

    itemsThatMatch = 0
    debugg("backToStartingPoint()")

    if not module_fail and not module_exit:
        # startFlag skips first run when objects would not have changed.
        if len(statusNow_l) > 0:
            # on two node rac with min redo of 2 should have to match 4 objects
            if itemsToMatch == 0:
                for item in origStartingPoint_l:
                    if item.startingObj():
                        debugg("startObject found => Group: %s" % (str(item.getGroup())))
                        itemsToMatch += 1

            for item in origStartingPoint_l:
                if item.startingObj():
                    curGroupState = sameGroupNow(item, statusNow_l)   # Find same item in statusNow_list of redoLog objects
                    if item.getStartingStatus() == curGroupState.getStatus():
                        debugg("")
                        itemsThatMatch += 1

            if itemsThatMatch == itemsToMatch:
                debugg("Exiting backToStartingPoint() itemsThatMatch: %s == itemsToMatch: %s returning: True" % (itemsThatMatch, itemsToMatch))
                return(True)
            else:
                debugg("Exiting backToStartingPoint() itemsThatMatch: %s == itemsToMatch: %s returning: False" % (itemsThatMatch, itemsToMatch))
                return(False)


def sameGroupNow(originalItem, curStatusList):

    for item in curStatusList:
        if item.getGroup() == originalItem.getGroup():
            debugg("sameGroupNow returning matching Group from curStatusList Group: %s" % (item.getGroup()))
            return(item)


def findCurrentThread(redoLogObj_list):
    startingPoint = []

    for redolog in redoLogObj_list:
        if redolog.getArchived() == "NO" and redolog.getStatus() == "CURRENT":
            startingPoint.append({'thread': redolog.getThread(), 'group': redolog.getGroup() })

    return(startingPoint)


def convert_to_dict(redoLogs_l):
    newRedoLogDict = {}
    # (1, 1, 52428800, 'INACTIVE', 'YES', '+FRA/TSTDB/ONLINELOG/group_1.28504.1006772175')
    # thread, group, size(bytes),status,archived,member
    for item in redoLogs_l:
        newRedoLogDict.update({'thread': item[0],'group':item[1],'size':item[2],'status':item[3],'archived':item[4],'member':item[5]})
        tmp = hbytes(newRedoLogDict['size'])
        newRedoLogDict['size'], newRedoLogDict['units'] = tmp.split("_")

    return(newRedoLogDict)


def hbytes(num):
    for x in ['bytes','KB','MB','GB']:
        if num < 1024.0:
            return "%d_%s" % (round(num), x)
        num /= 1024.0
    return "%d_%s" % (round(num), 'TB')

# ==============================================================================
def redoResizeMain(cur, arg_size, arg_units):
    """ Resize redo logs to value passed in
        arg_size = 3
        arg_units = m
        resize to 3m
        return msg with success or fail
    """
    msgg("redo_resize() function called..not implemented yet..exiting.. ")
    pass


def prep_host(vhost):
    """ Given a host string add cru domain:
            tlrac1 => tlrac1.ccci.org
    """
    debugg("prep_host({})".format(vhost))
    if "." in vhost:
        tmp = vhost.split(".")[0]
        debugg("prep_host exiting with {}".format(tmp))
        return(tmp)
    else:
        debugg("prep_host exiting with {}".format(vhost))
        return(vhost)


def prep_sid(vdb,vhost):
    """ Given a db string create a sid:
        Determine if the host is RAC
            if rac:
                hcms => hcms1
            else:
                hcms
        return(sid)
    """
    global affirm
    global israc

    if not israc and israc not in [ True, False]:
        israc = ckrac(vhost)

    if cru_domain in vhost:
        vhost = vhost.replace(cru_domain, "")

    if israc:
        if vhost[-1:].isdigit():
            node_num = vhost[-1:]

        if not vdb[-1:].isdigit():
            sid = vdb + vhost[-1:]
    else:
        sid = vdb

    debugg("prep_db({},{}) sid={}".format(vdb, vhost, sid))
    return(sid)


# ==============================================================================
# =================================== MAIN =====================================
# ==============================================================================

def main ():
    """ Return Oracle database parameters from a database not in the specified group"""
    global msg
    global debugme
    global g_vignore
    global israc
    global affirm
    global def_reference_name

    ansible_facts={}

    os.system("/usr/bin/scl enable python27 bash")
    # os.system("scl enable python27 bash")

    module = AnsibleModule(
        argument_spec = dict(
            connect_as      =dict(required=False),
            userpwd         =dict(required=True),
            db_name         =dict(required=True),
            db_host         =dict(required=True),
            function        =dict(required=True),
            size            =dict(required=False),
            units           =dict(required=False),
            israc           =dict(required=False),
            ignore          =dict(required=False),
            refname         =dict(required=False),
            debugmode       =dict(required=False)
        ),
       supports_check_mode=False,
    )

    # Get arguements passed from Ansible playbook
    vconnect_as    = module.params.get('connect_as')
    vdbpass        = module.params.get('userpwd')
    vdb            = module.params.get('db_name')
    vdbhost        = module.params.get('db_host')
    vfx            = module.params.get('function')
    vsize          = module.params.get('size')
    vunits         = module.params.get('units')
    visrac         = module.params.get('israc')
    vignore        = module.params.get('ignore')
    vrefname       = module.params.get('refname')
    vdebugmode     = module.params.get('debugmode')


    if vdebugmode in affirm:
        debugme = True
    else:
        debugme = False

    debugg("Start parameter checks")
    if not visrac:
        israc = ckrac(vdbhost)

    if visrac in affirm:
        israc = True
    else:
        israc = False
    debugg(" israc={} ".format(israc))

    # if the user passed a reference name use it
    if vrefname:
        refname = vrefname
    else:
        refname = def_reference_name

    if vconnect_as:
        vconas = vconnect_as
    else:
        vconas = "system"

    if not vdbpass:
        errmsg = 'REDOLOGS MODULE ERROR: No password provided.' %s (arg_param_name)
        response = { 'status':'Fail', 'errmsg': errmsg, 'Error': error.message, 'changed':'False'}
        if g_vignore:
            module.exit_json( msg=msg, ansible_facts=response , changed=False)
        else:
            module.fail_json(msg=msg, meta=response)

    if not vdb:
        errmsg = 'REDOLOGS MODULE ERROR: No db_name provided.' %s (arg_param_name)
        response = { 'status':'Fail', 'errmsg': errmsg, 'Error': error.message, 'changed':'False'}
        if g_vignore:
            module.exit_json( msg=msg, ansible_facts=response , changed=False)
        else:
            module.fail_json(msg=msg, meta=response)

    if not vdbhost:
        errmsg = 'REDOLOGS MODULE ERROR: No databae host provided for required function parameter.'
        response = { 'status':'Fail', 'errmsg': errmsg, 'Error': error.message, 'changed':'False'}
        if g_vignore:
            module.exit_json( msg=msg, ansible_facts=response , changed=False)
        else:
            module.fail_json(msg=msg, meta=response)

    if not vfx:
        errmsg = 'REDOLOGS MODULE ERROR: No function provided.'
        response = { 'status':'Fail', 'errmsg': errmsg, 'Error': error.message, 'changed':'False'}
        if g_vignore:
            module.exit_json( msg=msg, ansible_facts=response , changed=False)
        else:
            module.fail_json(msg=msg, meta=response)

    if vignore:
        g_vignore = vignore

    if not vsize and vfx == "resize":
        errmsg = 'REDOLOGS MODULE ERROR: No size provided. Required for resize function.'
        response = { 'status':'Fail', 'errmsg': errmsg, 'Error': error.message, 'changed':'False'}
        if g_vignore:
            module.exit_json( msg=msg, ansible_facts=response , changed=False)
        else:
            module.fail_json(msg=msg, meta=response)

    if not vunits and vfx == "resize":
        errmsg = 'REDOLOGS MODULE ERROR: No units provided. Required for resize function.'
        response = { 'status':'Fail', 'errmsg': errmsg, 'Error': error.message, 'changed':'False'}
        if g_vignore:
            module.exit_json( msg=msg, ansible_facts=response , changed=False)
        else:
            module.fail_json(msg=msg, meta=response)

    if vfx.lower() not in ("resize", "flush"):
        errmsg = 'REDOLOGS MODULE ERROR: Unknown function: %s. Function must be resize or flush' % (vfx)
        response = { 'status':'Fail', 'errmsg': errmsg, 'Error': error.message, 'changed':'False'}
        if g_vignore:
            module.exit_json( msg=msg, ansible_facts=response , changed=False)
        else:
            module.fail_json(msg=msg, meta=response)

    vdbhost = prep_host(vdbhost)

    debugg("before call to prep_db({},{})".format(vdb, vdbhost))
    vdb = prep_sid(vdb, vdbhost)

    debugg("before calling create_tns(%s,%s)" % (vdbhost,vdb))
    dsn_tns = create_tns(vdbhost,vdb)

    if not module_fail and not module_exit:
        cur = create_con(vdbpass, dsn_tns, vconas)
        if not module_fail and not module_exit:
            debugg("finished creating cursor")
            if vfx.lower() == "flush":

                # CHECK DB IS ARCHIVELOG MODE IF NOT EXIT ================
                cmd_str = "select LOG_MODE from v$database"
                try:
                    cur.execute(cmd_str)
                except:
                    pass
                output = cur.fetchall()
                if output:
                    arch_ck = output[0][0]
                    if 'ARCHIVELOG'.lower() != arch_ck.lower():
                        msgg("\n** Cannot flush redo logs if db not in archivelog mode. v$database log_mode={} **\n".format(arch_ck))
                        module.exit_json( msg=msg, ansible_facts={} , changed=False)
                # ARCHIVELOG MODE CK END ================================

                redoFlushMain(cur)
            elif vfx.lower() == "resize":
                redoResizeMain(cur, vsize, vunits)
            else:
                msgg('REDOLOG MODULE ERROR: choosing function')
                response = { 'status':'Fail', 'errmsg': errmsg, 'Error': error.message, 'changed':'False'}
                if g_vignore:
                    module.exit_json( msg=msg, ansible_facts=response , changed=False)
                else:
                    module.fail_json(msg=msg, meta=response)

            # Close the cursor before exit
            try:
                cur.close()
            except cx_Oracle.DatabaseError as exc:
              error, = exc.args
              msgg("Error closing cursor during redologs module %s META: %s" % (vfx, error.message))
              response = { 'status':'Fail', 'errmsg': errmsg, 'Error': error.message, 'changed':'False'}
              if g_vignore:
                  module.exit_json( msg=msg, ansible_facts=response , changed=False)
              else:
                  module.fail_json(msg=msg, meta=response)

            msgg("Custom module dbfacts succeeded for %s database." % (vdb))

            vchanged="False"

    if module_fail:
        response = { 'status':'Fail', 'errmsg': errmsg, 'Error': error.message, 'changed':'False'}
        if g_vignore:
            module.exit_json( msg=msg, ansible_facts=response , changed=False)
        else:
            module.fail_json(msg=msg, meta=response)
    elif module_exit:
        msgg("An Error occurred and the module is exiting without stopping the play since ignore was set to True")
        module.exit_json( msg=msg, ansible_facts=ansible_facts , changed=False)
    else:
        module.exit_json( msg=msg, ansible_facts=ansible_facts , changed=vchanged)

# code to execute if this program is called directly
if __name__ == "__main__":
   main()
