#!/opt/rh/python27/root/usr/bin/python
# -*- coding: utf-8 -*-

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
from ansible.module_utils.basic import *
from ansible.module_utils.facts import *
from ansible.module_utils._text import to_native
from ansible.module_utils._text import to_text
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

    size and units are not required for "flush" but are for resize.
    units are single letter: k (kilobytes), m (megabytes), g (gigabytes) etc.
    ignore - tells the module whether to fail on error and raise it or pass on error
             and continue with the play. Default is to fail.

'''
# Global Vars:
itemsToMatch = 0


#      THREAD#	   GROUP#      SIZE_MB       STATUS		  ARC     MEMBER
# ------------ ------------ ------------ ---------------- ---   ----------------------------------------------------------------------
#	   1		    1	          50         ACTIVE		  YES   +FRA/TSTDB/ONLINELOG/group_1.28504.1006772175
#
# So a dictionary like this should be passed into class redoLog
# { 'thread':1,'GROUP': 1, 'SIZE_MB':50, 'STATUS':'INACTIVE','ARCHIVED': 'YES','MEMBER': '+FRA/TSTDB/ONLINELOG/group_1.28504.1006772175'}
class redoLogClass:
    def __init__(self, redo_dict):
        self.__redoThread__ = redo_dict['thread']
        self.__redoGroup__ = redo_dict['group']
        self.__redoStatus__ = redo_dict['status']
        self.__prevStatus__
        self.__startingStatus__ = redo_dict['status']
        self.__redoSize__, self.__redoUnits__ = redo_dict['size_mb'].split("_")
        self.__redoStatus__ = redo_dict['archived']
        self.__archivedStatus__ = redo_dict['member']
        self.__startLog__
        self.setStartStatus()

    def setStartStatus(self):
        if self.__startingStatus__.lower() == "current" and self.__archivedStatus__.lower() == "no":
            self.__startLog__ = True
        else:
            self.__startLog__ = False

    def get_startObjStatus(self):
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


def add_to_msg(add_string):
    """Passed a string add it to the msg to pass back to the user"""
    global msg

    if msg:
        msg = msg + add_string
    else:
        msg = add_string


def debugg(add_string):
    """If debugme is True add this debugging information to the msg to be passed out"""
    global debugme
    global msg

    if debugme == "True":
        msgg(add_string)


def ck_required_param(arg_param_name, arg_param):
    if not arg_param:
        error_msg = 'REDOLOGS MODULE ERROR: No %s provided for required function parameter.' %s (arg_param_name)
        module.fail_json(msg=error_msg, ansible_facts=ansible_facts, changed=False)


def ck_req_fx_param(arg_param_name, arg_fx, arg_param):
    if not arg_param:
        error_msg = 'REDOLOGS MODULE ERROR: No %s provided for required %s function operation.' % (arg_param_name, arg_fx)
        module.fail_json(msg=error_msg, ansible_facts=ansible_facts, changed=False)


def prep_host(arg_dbhost):
    """If the host was passed in long form (tlorad01.ccci.org) trim it"""
    if "." in arg_dbhost:
        return(arg_dbhost.split(".")[0])


def prep_db(arg_db, arg_dbhost):
    """If db name has no instance number, add it"""
    if not arg_db[-1:].isdigit():
        return(arg_db + arg_dbhost[-1:])


def create_tns(vdbhost,vdb):
    global msg

    try:
      vdb = vdb + vdbhost[-1:]
      dsn_tns = cx_Oracle.makedsn(vdbhost, '1521', vdb)
    except cx_Oracle.DatabaseError as exc:
      error, = exc.args
      if vignore:
          add_to_msg("Failed to create dns_tns: %s" %s (error.message))
          module.exit_json( msg=msg, ansible_facts=ansible_facts , changed=vchanged)
      else:
          add_to_msg('TNS generation error: %s, db name: %s host: %s' % (error.message, vdb, vdbhost))
          module.fail_json(msg=msg, changed=False)

    return(dsn_tns)


def create_con(vdbpass, dsn_tns)
    global msg

    try:
      con = cx_Oracle.connect('system', vdbpass, dsn_tns)
    except cx_Oracle.DatabaseError as exc:
      error, = exc.args
      if vignore:
          add_to_msg("DB CONNECTION FAILED : %s" % (error.message))
          if debugme:
              add_to_msg(" vignore: %s " % (vignore))
          module.exit_json(msg=msg, ansible_facts=ansible_facts, changed="False")
      else:
          add_to_msg('Database connection error: %s, tnsname: %s host: %s' % (error.message, vdb, vdbhost))
          module.fail_json(msg=msg, changed=False)

    cur = con.cursor()

    return(cur)

# ==============================================================================

def redoFlushMain(cur):
    global msg
    startingPoint = []
    statusNow = []
    startFlag = 1

    startingPoint = curStatus(cur)

    # start forcing redo changes until this same redo thread group is current again.
    while not backToStartingPoint(statusNow, startingPoint, startFlag):
        advanceLogs(cur)
        time.sleep(3)
        statusNow = curStatus(cur)
        startFlag = 0


def curStatus(cur):
    """Get current status of all redo logs and pass back a list of redoLogClass objects"""

    try:
        cmd_str = 'select l.thread#,l.group#,l.bytes/1024/1024 SIZE_MB,l.status,l.archived,lf.member from v$logfile lf, v$log l where lf.group#=l.group# order by l.thread#,group#'
        cur.execute(cmd_str)
    except cx_Oracle.DatabaseError as exc:
        error, = exc.args
        add_to_msg('Error redo logs and status, Error: %s' % (error.message))
        module.fail_json(msg=msg, ansible_facts={}, changed=False)

    allLogs_l =  cur.fetchall()

    allLogs_d = convert_to_dict(allLogs_l)

    # create a list of redoLog Objects
    for oneLog in allLogs_d:
        redoLog_l.append(redoLogClass(oneLog))

    return(redoLog_l)


def advanceLogs(cur):
    """Advance redo thread to flus redo logs"""

    try:
        cmd_str = 'ALTER SYSTEM ARCHIVE LOG CURRENT'
        cur.execute(cmd_str)
    except cx_Oracle.DatabaseError as exc:
        error, = exc.args
        add_to_msg('Error redo logs and status, Error: %s' % (error.message))
        module.fail_json(msg=msg, ansible_facts={}, changed=False)


def backToStartingPoint(statusNow_l, startingPoint_l, startFlag):
    """startingPoint is list of dictionaries containing the Thread#,Group#'s that were ARC=NO, STATUS=CURRENT at start
       force archive will continue until the starting point is reached. ( A complete circle is made and all redo logs
       are flushed. )"""
    global itemsToMatch
    itemsThatMatch = 0

    # startFlag skips first run when objects would match
    if startFlag == 0:
        # on two node rac with min redo of 2 should have to match 4 objects
        if itemsToMatch == 0:
            for item in redoLog_l:
                if item.getStatus().lower() == "current" and item.getArchived().lower() == "no":
                    itemsToMatch += 1

        for item in startingPoint_l:
            if item.getStartObjStatus():
                curState = sameItemNow(item, statusNow_l)
                if item.getStartingStatus() == curState.getStatus():
                    itemsThatMatch += 1

        if itemsThatMatch == itemsToMatch:
            return(True)
        else:
            return(False)


def sameItemNow(originalItem, curStatusList):

    for item in curStatusList:
        if item.getThread() == originalItem.getThread() and
           item.getGroup() == originalItem.getGroup():
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
        newRedoLogDict.update({'thread': item[0],'group':item[1],'size':item[2],'status':item[3],'archived':item[4],'member':item[5])
        newRedoLogDict['size'] = hbytes(newRedoLogDict['size'])

    return(newRedoLogDict)


def hbytes(num):
    for x in ['bytes','KB','MB','GB']:
        if num < 1024.0:
            return "%d_%s" % (round(num), x)
        num /= 1024.0
    return "%d_%s" % (round(num), 'TB')


def redo_resize(cur, arg_size, arg_units):
    pass


# ==============================================================================
# =================================== MAIN =====================================
# ==============================================================================

def main ():
    """ Return Oracle database parameters from a database not in the specified group"""
    global msg
    ansible_facts={}

    # Name to call facts dictionary being passed back to Ansible
    # This will be the name you reference in Ansible. i.e. source_facts['sga_target'] (source_facts)
    refname = "redologs"

    os.system("/usr/bin/scl enable python27 bash")
    # os.system("scl enable python27 bash")

    module = AnsibleModule(
       argument_spec = dict(
         systempwd       =dict(required=True),
         db_name         =dict(required=True),
         host            =dict(required=True),
         function        =dict(required=True),
         size            =dict(required=False),
         units           =dict(required=False),
         ignore          =dict(required=False),
         refname         =dict(required=False),
       ),
       supports_check_mode=False,
    )

    # Get arguements passed from Ansible playbook
    vdbpass   = module.params.get('systempwd')
    vdb       = module.params.get('db_name')
    vdbhost   = module.params.get('host')
    vfx       = module.params.get('function')
    vsize     = module.params.get('size')
    vunits    = module.params.get('units')
    vignore   = module.params.get('ignore')
    vrefname  = module.params.get('refname')

    # if the user passed a reference name use it
    if vrefname:
        refname = vrefname

    ck_required_param("passord", vdbpass)

    ck_required_param("database", vdb)

    ck_required_param("destination host", vdbhost)

    ck_required_param("function", vfx)

    if vfx.lower() == "resize":
        ck_req_fx_param("size", "resize", vsize)

    if vfx.lower() == "resize":
        ck_req_fx_param("units", "resize", vunits)

    if vfx.lower() not in ("resize", "flush"):
        error_msg = 'REDOLOGS MODULE ERROR: Unknown function: %s. Function must be resize or flush' % (vfx)
        module.fail_json(msg=error_msg, ansible_facts=ansible_facts, changed=False)

    vdbhost = prep_host(vdbhost)

    vdb = prep_db(vdb,vdbhost)

    dsn_tns = create_tns(vdbhost,vdb)

    cur = create_con(vdbpass, dsn_tns)

    if vfx.lower() == "flush":

        redoFlushMain(cur)

    elif vfx.lower() == "resize":

        redoResizeMain(cur)

    else:

        add_to_msg('REDOLOG MODULE ERROR: choosing function')

        module.fail_json(msg=msg, ansible_facts=ansible_facts, changed=False)


    # Close the cursor before exit
    try:
        cur.close()
    except cx_Oracle.DatabaseError as exc:
      error, = exc.args
      add_to_msg("Error closing cursor during redologs module %s META: %s" % (vfx, error.message))
      module.fail_json(msg=msg, ansible_facts=ansible_facts, changed=False)

    msg="Custom module dbfacts succeeded for %s database." % (vdb)

    vchanged="False"

    module.exit_json( msg=msg, ansible_facts=ansible_facts , changed=vchanged)

# code to execute if this program is called directly
if __name__ == "__main__":
   main()
