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
        system_password: "{{ database_passwords[dest_db_name].system }}"
        dest_db: "{{ dest_db_name }}"
        dest_host: "{{ dest_host }}"
        function: flush
        size:
        units:
        ignore: true
        refname:
    become_user: "{{ local_user }}"
    register: redo_run

  - name: Resize redo logs
    local_action:
        module: redologs
        system_password: "{{ database_passwords[dest_db_name].system }}"
        dest_db: "{{ dest_db_name }}"
        dest_host: "{{ dest_host }}"
        function: resize
        size: 500
        units: m
        ignore: false
        refname:
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
error_msg = ""
debugme = False
g_vignore = False
ansible_facts = {}
module_fail = False
module_exit = False


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


def debugg(a_str):
    global msg
    global error_msg

    if debugme:
        add_to_msg(a_str)


def add_to_msg(add_string):
    """Passed a string add it to the msg to pass back to the user"""
    global msg

    if msg:
        msg = msg + " " + add_string
    else:
        msg = add_string


def create_tns(vdbhost,vdb):
    global msg
    global g_vignore
    global module_fail

    debugg("Connecting to %s on host %s" % (vdb,vdbhost))

    try:
      dsn_tns = cx_Oracle.makedsn(vdbhost, '1521', vdb)
    except cx_Oracle.DatabaseError as exc:
      error, = exc.args
      if g_vignore:
          add_to_msg("create_tns() : Failed to create dns_tns: %s" %s (error.message))
          module_exit = True
      else:
          add_to_msg('create_tns() : TNS generation error: %s, db name: %s host: %s' % (error.message, vdb, vdbhost))
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
            add_to_msg("DB CONNECTION FAILED : %s" % (error.message))
            if debugme:
                add_to_msg(" g_vignore: %s " % (g_vignore))
            module_exit = True
        else:
            add_to_msg('Database connection error: %s, tnsname: %s host: %s' % (error.message, vdb, vdbhost))
            module_fail = True

    if not module_exit and not module_fail:
        cur = con.cursor()
        return(cur)

# ==============================================================================

def redoFlushMain(cur):
    global msg
    global module_fail
    global module_exit
    startingPoint = []
    statusNow = []

    debugg("redoFlushMain")

    startingPoint = curStatus(cur)

    # start forcing redo changes until this same redo thread group is current again.
    while not backToStartingPoint(statusNow, startingPoint) and not module_exit and not module_fail:
        advanceLogs(cur)
        time.sleep(2)
        statusNow = curStatus(cur)


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
        add_to_msg('curStatus() : Error redo logs and status, Error: %s' % (error.message))
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
        add_to_msg('advanceLogs(): Error redo logs and status, Error: %s' % (error.message))
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


def redo_resize(cur, arg_size, arg_units):
    pass


def prep_host(vhost):
    debugg("prep_host(%s)" % (vhost))
    if "." in vhost:
        tmp = vhost.split(".")[0]
        debugg("prep_host exiting with %s" % (tmp))
        return(tmp)
    else:
        debugg("prep_host exiting with %s" % (vhost))
        return(vhost)


def prep_db(vdb,vhost):
    debugg("prep_db(%s,%s)" % (vdb,vhost))
    if vdb[-1:].isdigit():
        debugg("prep_db exiting with vdb: %s" % (vdb))
        return(vdb)
    else:
        dbinst = vdb + vhost[-1:]
        debugg("prep_db exiting with vdb: %s" % (dbinst))
        return(dbinst)

# ==============================================================================
# =================================== MAIN =====================================
# ==============================================================================

def main ():
    """ Return Oracle database parameters from a database not in the specified group"""
    global msg
    global debugme
    global g_vignore

    ansible_facts={}

    # Name to call facts dictionary being passed back to Ansible
    # This will be the name you reference in Ansible. i.e. source_facts['sga_target'] (source_facts)
    refname = "redologs"

    os.system("/usr/bin/scl enable python27 bash")
    # os.system("scl enable python27 bash")

    module = AnsibleModule(
        argument_spec = dict(
            connect_as      =dict(required=False),
            systempwd       =dict(required=True),
            db_name         =dict(required=True),
            db_host         =dict(required=True),
            function        =dict(required=True),
            size            =dict(required=False),
            units           =dict(required=False),
            ignore          =dict(required=False),
            refname         =dict(required=False),
            debugme         =dict(required=False)
        ),
       supports_check_mode=False,
    )

    # Get arguements passed from Ansible playbook
    vconnect_as    = module.params.get('connect_as')
    vdbpass        = module.params.get('systempwd')
    vdb            = module.params.get('db_name')
    vdbhost        = module.params.get('db_host')
    vfx            = module.params.get('function')
    vsize          = module.params.get('size')
    vunits         = module.params.get('units')
    vignore        = module.params.get('ignore')
    vrefname       = module.params.get('refname')
    vdebugme       = module.params.get('debugme')

    if vdebugme:
        debugme = vdebugme

    debugg("Start parameter checks")
    # if the user passed a reference name use it
    if vrefname:
        refname = vrefname

    if vconnect_as:
        vconas = vconnect_as
    else:
        vconas = "system"

    if not vdbpass:
        error_msg = 'REDOLOGS MODULE ERROR: No password provided.' %s (arg_param_name)
        response = { 'status':'Fail', 'error_msg': error_msg, 'Error': error.message, 'changed':'False'}
        if g_vignore:
            module.exit_json( msg=msg, ansible_facts=response , changed=False)
        else:
            module.fail_json(msg=msg, meta=response)

    if not vdb:
        error_msg = 'REDOLOGS MODULE ERROR: No db_name provided.' %s (arg_param_name)
        response = { 'status':'Fail', 'error_msg': error_msg, 'Error': error.message, 'changed':'False'}
        if g_vignore:
            module.exit_json( msg=msg, ansible_facts=response , changed=False)
        else:
            module.fail_json(msg=msg, meta=response)

    if not vdbhost:
        error_msg = 'REDOLOGS MODULE ERROR: No databae host provided for required function parameter.'
        response = { 'status':'Fail', 'error_msg': error_msg, 'Error': error.message, 'changed':'False'}
        if g_vignore:
            module.exit_json( msg=msg, ansible_facts=response , changed=False)
        else:
            module.fail_json(msg=msg, meta=response)

    if not vfx:
        error_msg = 'REDOLOGS MODULE ERROR: No function provided.'
        response = { 'status':'Fail', 'error_msg': error_msg, 'Error': error.message, 'changed':'False'}
        if g_vignore:
            module.exit_json( msg=msg, ansible_facts=response , changed=False)
        else:
            module.fail_json(msg=msg, meta=response)

    if vignore:
        g_vignore = vignore

    if not vsize and vfx == "resize":
        error_msg = 'REDOLOGS MODULE ERROR: No size provided. Required for resize function.'
        response = { 'status':'Fail', 'error_msg': error_msg, 'Error': error.message, 'changed':'False'}
        if g_vignore:
            module.exit_json( msg=msg, ansible_facts=response , changed=False)
        else:
            module.fail_json(msg=msg, meta=response)

    if not vunits and vfx == "resize":
        error_msg = 'REDOLOGS MODULE ERROR: No units provided. Required for resize function.'
        response = { 'status':'Fail', 'error_msg': error_msg, 'Error': error.message, 'changed':'False'}
        if g_vignore:
            module.exit_json( msg=msg, ansible_facts=response , changed=False)
        else:
            module.fail_json(msg=msg, meta=response)

    if vfx.lower() not in ("resize", "flush"):
        error_msg = 'REDOLOGS MODULE ERROR: Unknown function: %s. Function must be resize or flush' % (vfx)
        response = { 'status':'Fail', 'error_msg': error_msg, 'Error': error.message, 'changed':'False'}
        if g_vignore:
            module.exit_json( msg=msg, ansible_facts=response , changed=False)
        else:
            module.fail_json(msg=msg, meta=response)

    vdbhost = prep_host(vdbhost)

    debugg("before call to prep_db(%s,%s)" % (vdb,vdbhost))
    vdb = prep_db(vdb,vdbhost)

    debugg("before calling create_tns(%s,%s)" % (vdbhost,vdb))
    dsn_tns = create_tns(vdbhost,vdb)

    if not module_fail and not module_exit:
        cur = create_con(vdbpass, dsn_tns, vconas)
        if not module_fail and not module_exit:
            debugg("finished creating cursor")
            if vfx.lower() == "flush":
                redoFlushMain(cur)
            elif vfx.lower() == "resize":
                redoResizeMain(cur)
            else:
                add_to_msg('REDOLOG MODULE ERROR: choosing function')
                response = { 'status':'Fail', 'error_msg': error_msg, 'Error': error.message, 'changed':'False'}
                if g_vignore:
                    module.exit_json( msg=msg, ansible_facts=response , changed=False)
                else:
                    module.fail_json(msg=msg, meta=response)

            # Close the cursor before exit
            try:
                cur.close()
            except cx_Oracle.DatabaseError as exc:
              error, = exc.args
              add_to_msg("Error closing cursor during redologs module %s META: %s" % (vfx, error.message))
              response = { 'status':'Fail', 'error_msg': error_msg, 'Error': error.message, 'changed':'False'}
              if g_vignore:
                  module.exit_json( msg=msg, ansible_facts=response , changed=False)
              else:
                  module.fail_json(msg=msg, meta=response)

            add_to_msg("Custom module dbfacts succeeded for %s database." % (vdb))

            vchanged="False"

    if module_fail:
        response = { 'status':'Fail', 'error_msg': error_msg, 'Error': error.message, 'changed':'False'}
        if g_vignore:
            module.exit_json( msg=msg, ansible_facts=response , changed=False)
        else:
            module.fail_json(msg=msg, meta=response)
    elif module_exit:
        add_to_msg("An Error occurred and the module is exiting without stopping the play since ignore was set to True")
        module.exit_json( msg=msg, ansible_facts=ansible_facts , changed=False)
    else:
        module.exit_json( msg=msg, ansible_facts=ansible_facts , changed=vchanged)

# code to execute if this program is called directly
if __name__ == "__main__":
   main()
