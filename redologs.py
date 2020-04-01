#!/usr/bin/env python3
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
#  Revisions:
#       January 17, 2020 - When running against JFD using flush function
#       the module looped continuously for over 7 min and had to be killed.
#       This version implements a new way of comparing starting state
#       to current state and terminates when complete.
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
import struct

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
        cycles: 2
        size:
        units:
	      israc: "{{ destfacts['israc'] }}"
        ignore: true
        refname:
        debugmode: False
        debuglog: 
    become_user: "{{ local_user }}"
    register: redo_run

  - name: Resize redo logs
    local_action:
        module: redologs
        userpwd: "{{ database_passwords[dest_db_name].system }}"
        db_name: "{{ dest_db_name }}"
        db_host: "{{ dest_host }}"
        function: resize
        cycles:
        size: 500
        units: m
	      israc: "{{ destfacts['israc'] }}"
        ignore: false
        refname:
        redo_fix
        debugmode:
        debuglog:
    register: redo_run

  Notes: Used to flush or resize redo logs.

 connect_as - Not required. default system.
     cycles - Only works with FLUSH function.
              The number of times to force log switches and flush redo logs.
              It captures starting status and cycles through to that picture
              this number of times.
              size and units are not required for "flush" but are for resize.
       size - Required for resize and is a number i.e. size: 2 units: m = 2MB
      units - Required for resize.
              are single letter: k (kilobytes), m (megabytes), g (gigabytes) etc.
     ignore - tells the module whether to fail on error and raise it or pass on error
              and continue with the play. Default is to fail.
  debugmode and debuglog - optional, but required if debugging.
              all debugging information will be written to debuglog if provided.
              otherwise if debugmode is True it, but no debuglog provided it will
              be written to the output msg if the module runs to completion.

'''
# Global Vars:
itemsToMatch = 0
msg = ""
errmsg = ""
debugme = False
debuglog = ""
debugFile = ""
g_vignore = False
ansible_facts = {}
module_fail = False
module_exit = False
israc = False
# Name to call facts dictionary being passed back to Ansible
# This will be the name you reference in Ansible. i.e. source_facts['sga_target'] (source_facts)
refname = "redologs"
def_con_as_user = "system"
num_start_objs = 0
num_cycles = 1
#
affirm = ['True', 'TRUE', 'true', True, 'YES', 'Yes', 'yes',  't', 'T', 'y', 'Y', "On', 'ON', 'on']
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
# A dictionary like this should be passed into class redoLog
# { 'thread':1,'GROUP': 1, 'SIZE_MB':50, 'STATUS':'INACTIVE','ARCHIVED': 'YES','MEMBER': '+FRA/TSTDB/ONLINELOG/group_1.28504.1006772175'}
class redoLogClass:
    def __init__(self, redo_t, cycles=None):
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
        self.__changeflag__ = False
        self.__finished__ = False
        self.__laps_to_finish__ = 2
        self.__lap_count__ = 0
        self.__cycles__ = cycles
        self.setStartStatus()

    def setStartStatus(self):
        """ Set the startStatus flag here. If this object meets the criteria
            for a start object: status: current archived: no
            then make note of the ASM member FRA or DATA#
        """
        if self.__startingStatus__.lower() == "current" and self.__archivedStatus__.lower() == "no":
            self.__startObj__ = True
        else:
            self.__startObj__ = False
        # member will be +FRA or +DATA1 etc. '+FRA/TSTDB/ONLINELOG/group_1.28504.1006772175'
        self.__member__ = self.__member__.split("/")[0]
        # if number of laps were passed in set it. Otherwise default is 2
        if self.__cycles__:
            self.__laps_to_finish__ = int(self.__cycles__)

    def startingObj(self):
        """ Return to whatever is calling whether this is a start object
        """
        return(self.__startObj__)

    def getStatus(self):
        return(self.__redoStatus__)

    def getPrevStatus(self):
        return(self.__prevStatus__)

    def getStartingStatus(self):
        """ Return status of this object when it was instantiated:
                current
                inactive
        """
        return(self.__startingStatus__)

    def setStatus(self,newStatus):
        self.__prevStatus__ = self.getStatus()
        self.__redoStatus__ = newStatus

    def getThread(self):
        """ Return thread# for this object
        """
        return(self.__redoThread__)

    def getGroup(self):
        """ Return group# for this object
        """
        return(self.__redoGroup__)

    def getSize(self):
        """ Return redo size for this object
        """
        return(self.__redoSize__)

    def getUnits(self):
        """ Return unit size for this object MB, GB
        """
        return(self.__redoUnits__)

    def getArchived(self):
        """ Return archive status
        """
        return(self.__archivedStatus__)

    def same_obj(self, obj_info):
        """ Given a dictionary containing thread, group and member see if its a match
            for this object:
            checking that the obj_info is for this object:
            { 'thread': Thread#, 'group': Group#, 'member': Member ( +DATA or +FRA ), 'status': current or inactive, 'arc': no or yes }
        """
        if obj_info.get('thread', None) == self.__redoThread__:
            if obj_info.get('group') == self.__redoGroup__:
                if obj_info.get('member', None) == self.__member__:
                    return(True)
                else:
                    return(False)
            else:
                return(False)
        else:
            return(False)

    def changed(self, obj_state):
        """ current state of this obj passed in:
            { 'thread': Thread#, 'group': Group#, 'member': Member ( +DATA or +FRA ), 'status': current or inactive, 'arc': no or yes }
            if __startObj__ and current_state != __startingStatus__
            change is reached. So next time current_state == __startingStatus__
            the cycle is finished. This fx only tracks change.
        """
        # if obj_state is not for this obj return
        if not self.same_obj(obj_state):
            return

        # if this is a start obj and its finished, nothing to do
        if self.__startObj__ and self.__finished__:
            return

        # if start obj and change hasnt happened yet..
        if self.__startObj__ and not self.__changeflag__:
            if self.__startingStatus__ != obj_state.get('status', None):
                self.__changeflag__ = True
        # elif start obj and current state == start state and change flag is set
        elif self.__changeflag__ and obj_state.get('status', None) == self.__startingStatus__:
            # To ensure all data is flushed from redo logs, reach start state twice! self.__laps_to_finish__ = 2
            if self.__lap_count__ == self.__laps_to_finish__:
                self.__finished__ = True
            else:
                self.__lap_count__ += 1

    def finished(self):
        """ This returns finished state
        """
        return(self.__finished__)

    def debugg(self, _debugfile=None, _num=None):
        """ If debugfile passed flush contents of this
            object to debugfile
        """
        if _debugfile:
            with open(_debugfile, 'a') as f:
                f.write("====================================\n")
                f.write(" _num: {} \n \
                            redoThread={}\n \
                            redoGroup={}\n \
                            redoStatus={}\n \
                            prevStatus={}\n \
                            startingStatus={}\n \
                            redoSize={}\n \
                            redoUnits={}\n \
                            archivedStatus={}\n \
                            member={}\n \
                            startObj={}\n <<<<<< \
                            changeflag={}\n \
                            finished={}\n \
                         ".format(_num,
                                    self.__redoThread__,
                                    self.__redoGroup__,
                                    self.__redoStatus__,
                                    self.__prevStatus__,
                                    self.__startingStatus__,
                                    self.__redoSize__,
                                    self.__redoUnits__,
                                    self.__archivedStatus__,
                                    self.__member__,
                                    self.__startObj__,
                                    self.__changeflag__,
                                    self.__finished__
                                     ))


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
    global error_msg
    global debuglog
    global debugme

    if debugme:
        if not debuglog:
            add_to_msg(a_str)
        else:
            with open(debuglog, 'a') as f:
                f.write(a_str+"\n")


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

    if msg:
        msg = msg + " " + add_string
    else:
        msg = add_string + " "
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
    cur_count = 0
    startingPoint = []
    statusNow = []

    debugg("redoFlushMain()...starting...")

    start_state = curStatus(cur)
    final_count = start_obj_count(start_state) # get the number of items that have to finish

    debugg("redoFlushMain() ..while loop starting....cur_count={} final_count={}".format(cur_count, final_count))
    while cur_count != final_count:
        debugg("redoFlushMain()...advanceLogs()...")
        advanceLogs(cur)
        # get the current redo log status ( list of dictionaries )
        cur_state = getstatusnow(cur)
        # compare it with the original and see how many have finished
        cur_count = compare_states(start_state, cur_state)
        time.sleep(1)


def compare_states(start_state, cur_state):
    """ compare the starting state with the current state
        see if the threads to watch have finished
    """
    fin_count = 0
    debugg("compare_states()....starting.....")

    for i in range(len(start_state)):
        start_state[i].changed(cur_state[i])
        if start_state[i].finished():
            debugg("compare_states()..index {} returning True ( actual {} )".format(str(i), start_state[i].startingObj()))
            fin_count += 1

    debugg("compare_states()....finished.....returning fin_count={}".format(fin_count))
    return(fin_count)


def start_obj_count(orig_obj_list):
    """ Return the number of objects that are start Objects
    """
    global num_start_objs
    count = 0
    debugg("start_obj_count()...starting...num_start_objs={}".format(num_start_objs))

    if int(num_start_objs) != 0:
        return(num_start_objs)

    debugg("start_obj_count()...count loop..len(orig_obj_list) = {}".format(str(len(orig_obj_list))))
    for obj in orig_obj_list:
        debugg("start_obj_count()...checking obj={}".format(str(count)))
        if obj.startingObj():
            count += 1
            debugg("     startobj={} count = {}".format(str(obj.startingObj()), str(count)))

    num_start_objs = count
    debugg("start_obj_count()...exiting...returning num_start_objs={}".format(str(num_start_objs)))
    return(num_start_objs)


def getstatusnow(cur):
    """ Implementing new way to terminate log switches.
        observed log switching going on for 7 min.
        cycling over and over without terminating.
        Initial state is captured by curStatus()
    """
    global g_vignore
    global module_fail
    global module_exit
    redoLog_l = []
    temp_list = []
    temp_dict = {}

    debugg("getstatusnow()...starting....")

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
        # [(1, 1, 50, 'CURRENT', 'NO', '+DATA2/JFD/ONLINELOG/group_1.289.1024229571'), ... ]
        # create a list of dictionary objects and prep it to pass to orig_obj_list
        for oneLog in allRedoLogs_l:
            debugg("oneLog = {}".format(str(oneLog)))
            # (1, 1, 50, 'CURRENT', 'NO', '+DATA2/JFD/ONLINELOG/group_1.289.1024229571')
            temp_membr = oneLog[5].split("/")[0]
            temp_dict = { 'thread': oneLog[0], 'group': oneLog[1], 'member': temp_membr, 'status': oneLog[3], 'arc': oneLog[4]}
            temp_list.append(temp_dict)

    debugg("getstat()...exiting....returning = {}".format(str(temp_list)))
    return(temp_list)


def curStatus(cur):
    """ Get current status of all redo logs and pass back a list of redoLogClass objects
        Used to capture the starting state before looping.
    """
    global g_vignore
    global module_fail
    global module_exit
    global debuglog
    global num_cycles
    local_debugging = True

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
        debug_idx = 0
        # create a list of redoLog Objects
        for oneLog in allRedoLogs_l:
            # debugg("%s type: %s" % (str(oneLog),type(oneLog)))
            redoLog_l.append(redoLogClass(oneLog, num_cycles))
            if local_debugging:
                # def debugg(self, _debugfile=None, _num=None):
                redoLog_l[debug_idx].debugg(debuglog, debug_idx+1 )
                debug_idx += 1

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
    """ See if the host is RAC or SI and return the appropriate db sid
            dws ( no number in the sid ) or hcmd1
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
    global debuglog
    global g_vignore
    global israc
    global affirm
    global refname
    global def_con_as_user
    global num_cycles

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
            cycles          =dict(required=False),
            size            =dict(required=False),
            units           =dict(required=False),
            israc           =dict(required=False),
            ignore          =dict(required=False),
            refname         =dict(required=False),
            debugmode       =dict(required=False),
            debuglog        =dict(required=False)
        ),
       supports_check_mode=False,
    )

    # Get arguements passed from Ansible playbook
    vconnect_as    = module.params.get('connect_as')
    vdbpass        = module.params.get('userpwd')
    vdb            = module.params.get('db_name')
    vdbhost        = module.params.get('db_host')
    vfx            = module.params.get('function')
    vcycles        = module.params.get('cycles')
    vsize          = module.params.get('size')
    vunits         = module.params.get('units')
    visrac         = module.params.get('israc')
    vignore        = module.params.get('ignore')
    vrefname       = module.params.get('refname')
    vdebugme       = module.params.get('debugmode')
    vdebuglog      = module.params.get('debuglog')

    if vdebugmode in affirm:
        debugme = True
    else:
        debugme = False

    if vdebuglog:
        debuglog = vdebuglog

    debugg("Start parameter checks...this pyhton code is {} bit...python {}".format(struct.calcsize("P") * 8, sys.executable))

    if visrac in affirm:
        israc = True
    else:
        israc = False
    debugg(" israc={} ".format(israc))

    # if the user passed a reference name use it else use default ( redologs )
    if vrefname:
        refname = vrefname

    # if user name to connect as was passed, use it, else default ( system )
    if vconnect_as:
        vconas = vconnect_as
    else:
        vconas = def_con_as_user

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

    # ========= START MODULE MAJOR FUNCTIONS: FLUSH or RESIZE ==================
    if not module_fail and not module_exit:
        cur = create_con(vdbpass, dsn_tns, vconas)
        if not module_fail and not module_exit:
            debugg("finished creating cursor")
            if vfx.lower() == "flush":
                if vcycles:
                    # if flushing and a number of cycles was passed set it
                    num_cycles = int(vcycles)
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

            msgg("Custom module dbfacts succeeded for %s database.%s function" % (vdb, vfx.lower()))

            vchanged="False"

    if module_fail:

        response = { 'status':'Fail', 'error_msg': error_msg, 'Error': error.message or "Not available", 'changed':'False'}

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
