#!/opt/rh/python27/root/usr/bin/python
# -*- coding: utf-8 -*-

# scl enable python27 bash
# export LD_LIBRARY_PATH=$ORACLE_HOME/lib:$LD_LIBRARY_PATH
#
# Module for Ansible to retrieve Oracle facts from a host.
#
#
# Written by : Cru Ansible Module development team
#
#  To use your cutom module pass it in to the playbook using:
#  --module-path custom_modules
#
# This module will get Oracle information from an Oracle database server
#
# For programming:
# ansible-playbook clone_database.yml -i cru_inventory --extra-vars="hosts=test_rac source_db_name=fscm9xu dest_db_name=testdb source_host=tlorad01 adupe=ss" --tags "orafacts" --step -vvv
#
# The Data collection to include: (to be checked off when implemented)
#  [X]  1) all hosts on the cluster
#  [X]  2) listeners being used
#             listener home
#
#  [X]  3) grid home and version
#  [X]  4) database homes and versions
#  [ ]  5) ASM or local files
#  [ ]      if ASM diskgroup names
#  [X]  6) tnsnames file location
#  [X]  7) database information
#  [ ]  8) hugepages information <<== cannot be done with the sudo error we have
#  [ ]  9) crsctl version
#  [X]  10) srvctl version for each home : srvctl -V
#  [ ]  11) log location
#               - scan listeners
#               - db logs
#  [X]  12) lsnrctl info
#  [ ]  13) agent_home - i.e. /app/oracle/agent12c/agent_inst
#
# This file is part of Ansible
#
# Ansible is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# Ansible is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with Ansible.  If not, see <http://www.gnu.org/licenses/>.
# [..] various imports
# from ansible.module_utils.basic import AnsibleModule
#
# Last updated August 28, 2017    Sam Kohler
#

# Make coding more python3-ish
from __future__ import (absolute_import, division, print_function)
__metaclass__ = type

from ansible.module_utils.basic import *
from ansible.module_utils.facts import *
from ansible.module_utils._text import to_native
from ansible.module_utils._text import to_text
# from ansible.error import AnsibleError
import commands
import subprocess
import sys
import os
import json
import re                           # regular expression
# import math
# import time
# import pexpect
# from datetime import datetime, date, time, timedelta
from subprocess import (PIPE, Popen)
from __builtin__ import any as exists_in  # exist_in(word in x for x in mylist)


ANSIBLE_METADATA = {'status': ['stableinterface'],
                    'supported_by': 'Cru DBA team',
                    'version': '0.3'}

DOCUMENTATION = '''
---
module: orafacts
short_description: Collect Oracle database metadata on a remote host.
notes: Returned values are then available to use in Ansible.
requirements: [ python2.* ]
author: "DBA Oracle module Team"
'''

EXAMPLES = '''

  # for playbooks against one environment
  - name: Gather Oracle facts
    orafacts:

  # Gathers Oracle installation information on target hosts
    - name: Gather Oracle facts on destination servers
      orafacts:
      register: target_host
      tags: orafacts

   WARNING: These modules can be run with the when: master_node statement.
            However, their returned values cannot be referenced later.

'''

debugme = True
host_debug_path="/tmp/debug.log"
ora_home = ""
global_ora_home = ""
err_msg = ""
v_rec_count = 0
grid_home = ""
err_msg = ""
node_number = ""
node_name = ""
msg = ""
grid_home = ""
oracle_base = "/app/oracle"
os_path = "PATH=/app/oracle/agent12c/core/12.1.0.3.0/bin:/app/oracle/agent12c/agent_inst/bin:/app/oracle/11.2.0.4/dbhome_1/OPatch:/app/oracle/12.1.0.2/dbhome_1/bin:/usr/lib64/qt-3.3/bin:/usr/local/bin:/bin:/usr/bin:/usr/local/sbin:/usr/sbin:/sbin:/usr/local/rvm/bin:/opt/dell/srvadmin/bin:/u01/oracle/bin:/u01/oracle/.emergency_space:/app/12.1.0.2/grid/tfa/slorad01/tfa_home/bin"
israc = None
spcl_case_dbs = ['orcl11g','orcl12c','orcl19']


def msgg(add_string):
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

    if debugme:
        msgg(add_string)
        debug_to_log(add_string)


def debug_to_log(debug_str):
    """write debugging strings to log file on remote host"""
    global host_debug_path

    try:
        with open(host_debug_path, 'a') as f:
            f.write(debug_str + '\n')
    except:
        pass


def run_remote_cmd(cmd_str):
    """execute command on remote host"""
    debugg("run_remote_cmd() :: cmd_str={}".format(cmd_str))
    try:
        p = subprocess.Popen([cmd_str], stdout=PIPE, stderr=PIPE, shell=True)
        output, code = p.communicate()
    except:
       debugg("%s, %s, %s" % (sys.exc_info()[0], sys.exc_info()[1], sys.exc_info()[2]))
       debugg("run_remote_cmd() :: Error running cmd_str={} on remote host = {}".format(cmd_str,remote_host))
       return

    debugg("run_cmd() :: returning output = %s" % (str(output)))

    if output:
        return(output.strip())
    else:
        return("")


def get_field(fieldnum, vstring):
    """Simple fuction to return a field from a string of items"""
    x = 1
    for i in vstring.split():
      if fieldnum == x:
        return i
      else:
        x += 1


def get_dbhome(local_vdb):
    """Return database home as recorded in /etc/oratab"""
    global my_msg
    global ora_home

    cmd_str = "cat /etc/oratab | grep -m 1 " + local_vdb + " | grep -o -P '(?<=:).*(?<=:)' |  sed 's/\:$//g'"

    try:
        process = subprocess.Popen([cmd_str], stdout=PIPE, stderr=PIPE, shell=True)
        output, code = process.communicate()
    except:
       my_msg = my_msg + ' Error [1]: srvctl module get_orahome() error - retrieving oracle_home excpetion: %s' % (sys.exc_info()[0])
       my_msg = my_msg + "%s, %s, %s %s" % (sys.exc_info()[0], sys.exc_info()[1], my_msg, sys.exc_info()[2])
       raise Exception (my_msg)

    ora_home = output.strip()

    if not ora_home:
        my_msg = ' Error[2]: srvctl module get_orahome() error - retrieving oracle_home excpetion: %s' % (sys.exc_info()[0])
        my_msg = my_msg + "%s, %s, %s %s" % (sys.exc_info()[0], sys.exc_info()[1], my_msg, sys.exc_info()[2])
        raise Exception (my_msg)

    return(ora_home)


def get_nth_item(vchar, vfieldnum, vstring): # This can be done with python string.split('<char>')[3]
    """given a character vchar to deliniate a field return field number n from string vstring"""
    # ex /app/oracle/12.1.0.2/dbhome_1 return field 4 (12.1.0.2) assume EOL a vchar
    letter_counter = 0
    vfield_counter = 0
    vreturn_item = ""

    while vfield_counter < (vfieldnum + 1):
        if vstring[letter_counter] == vchar:
            vfield_counter += 1
        elif vfield_counter >= vfieldnum:
            vreturn_item = vreturn_item + vstring[letter_counter]
        letter_counter += 1

    return(vreturn_item)


def get_node_num():
    """Return current node number to ensure that srvctl is only executed on one node (1)"""
    global grid_home
    global err_msg
    global node_number
    global node_name
    global msg
    tmp_cmd = ""

    if not grid_home:
        grid_home = get_gihome()

    try:
      tmp_cmd = grid_home + "/bin/olsnodes -l -n | awk '{ print $2 }'"
      process = subprocess.Popen([tmp_cmd], stdout=PIPE, stderr=PIPE, shell=True)
      output, code = process.communicate()
    except:
       err_msg = err_msg + ' Error: srvctl module get_node_num() error - retrieving node_number excpetion: %s' % (sys.exc_info()[0])
       err_msg = err_msg + "%s, %s, %s %s" % (sys.exc_info()[0], sys.exc_info()[1], err_msg, sys.exc_info()[2])
       raise Exception (err_msg)

    node_number = int(output.strip())

    return(node_number)


def get_nodes(vstring):
  """Return the number of nodes in a RAC cluster and their names"""
  x = 1 # This counter counts node/line numbers
  tmp = {}
  for vline in vstring.splitlines():
    for token in vline.split():
        tmp.update({'node'+str(x) : token})
        break
    x += 1
  return tmp


def get_gihome():
    """Determine the Grid Home directory"""
    global grid_home

    try:
      process = subprocess.Popen(["/bin/ps -eo args | /bin/grep ocssd.bin | /bin/grep -v grep | /bin/awk '{print $1}'"], stdout=PIPE, stderr=PIPE, shell=True)
      output, code = process.communicate()
    except:
        err_msg = ' get_gihome() retrieving GRID_HOME : (%s,%s)' % (sys.exc_info()[0],code)
        module.fail_json(msg='ERROR: %s' % (err_msg), changed=False)

    grid_home = (output.strip()).replace('/bin/ocssd.bin', '')

    if not grid_home:
        err_msg = ' Error:  get_gihome() error - retrieving grid_home : %s output: %s' % (grid_home, output)
        err_msg = err_msg + "%s, %s, %s %s" % (sys.exc_info()[0], sys.exc_info()[1], err_msg, sys.exc_info()[2])
        raise Exception (err_msg)

    return(grid_home)


def get_installed_ora_homes2():
    """Using OUI installer information get Oracle Homes for a server """
    # taken from https://docs.oracle.com/cd/E11857_01/em.111/e12255/oui2_manage_oracle_homes.htm#CJAEHIGJ

    # Get inventory location from the Central Inventory pointer file
    # Linux location:
    try:
      process = subprocess.Popen(["/bin/cat /etc/oraInst.loc | /bin/grep inventory_loc | /bin/cut -d '=' -f 2"], stdout=PIPE, stderr=PIPE, shell=True)
      output, code = process.communicate()
    except:
        err_msg = ' get_installed_ora_homes2() retrieving inventory_loc : (%s,%s)' % (sys.exc_info()[0],code)
        module.fail_json(msg='ERROR: %s' % (err_msg), changed=False)

    inventory_loc = output.strip()

    # get oracle homes from the inventory.xml file in the inventory_loc/ContentsXML directory
    try:
      process = subprocess.Popen(["/bin/cat " + inventory_loc + "/ContentsXML/inventory.xml | grep OraD | awk -F '=' '{print $3}' | grep -o '.*' | sed 's/\"//g' | awk '{print $1}'"], stdout=PIPE, stderr=PIPE, shell=True)
      output, code = process.communicate()
    except:
        err_msg = err_msg + ' get_installed_ora_homes2() retrieving vorahomes : (%s,%s)' % (sys.exc_info()[0],code)
        module.fail_json(msg='ERROR: %s' % (err_msg), changed=False)

    vorahomes=output.strip().split('\n')

    #clean up the output and store results
    for item in vorahomes:
        if "11" in item:
            home11g = item.strip()
        elif "12" in item:
            home12c = item.strip()

    if home12c:
        return (home12c)
    else:
        return (home11g)


def strip_version(vorahome):
    """Strip the oracle version from an oracle_home entry"""
    debugg("strip_version().....called with : [ %s ] " % (vorahome))

    all_items = vorahome.split("/")

    for item in all_items:
        if item and item[0].isdigit():
            return(item)

    return 1


def get_db_home_n_vers(local_db):
    """Using /etc/oratab return the Oracle Home for the database"""
    global err_msg
    global spcl_case_dbs
    return_info = {}

    if local_db[-1].isdigit() and local_db not in spcl_case_dbs:
        local_db = local_db[:-1]

    try:
      process = subprocess.Popen(["/bin/cat /etc/oratab | /bin/grep -m 1 " + local_db + " | /bin/cut -d ':' -f 2"], stdout=PIPE, stderr=PIPE, shell=True)
      output, code = process.communicate()
    except:
      err_msg = err_msg + " Error: orafacts module get_db_home_n_vers() - retrieving oracle_home and version"
      err_msg = err_msg + "%s, %s, %s, %s" % (sys.exc_info()[0], sys.exc_info()[1], err_msg, sys.exc_info()[2])
      raise Exception (err_msg)

    vhome = output # .strip()
    debugg("    get_db_home_n_vers() local_db = %s  =>> vhome = %s" % ( local_db, vhome))

    if vhome:
        debugg("    get_db_home_n_vers() %s has a vhome = %s" % (local_db, vhome))
        try:
            vversion = get_nth_item("/", 3, vhome) #  get_nth_item(vchar, vfieldnum, vstring)
        except:
            custom_err_msg = " Error[ get_db_home_n_vers() ]: getting oracle_home for database: %s" % (local_db)
            custom_err_msg = custom_err_msg + "%s, %s, %s, %s" % (sys.exc_info()[0], sys.exc_info()[1], err_msg, sys.exc_info()[2])
            raise Exception (custom_err_msg)
    else:

        debugg("    get_db_home_n_vers() %s has no vhome" % (local_db))
        vhome = get_orahome_procid(local_db)
        debugg("    get_db_home_n_vers() called get_orahome_procid() and got back vhome = %s" % (vhome))
        if vhome and not 'cannot' in vhome:
            vversion = strip_version(vhome)
        else:

            crs_info = get_crsctl_info(local_db)

            return( crs_info )
            # exit_msg = "no process running for : %s" % (local_db)
            # sys.exit(exit_msg)

    return_info = { local_db: {'home':vhome.strip(), 'version': vversion}} # <<== added strip() here

    return(return_info)


def get_crsctl_info(db):
    """ Given a database that exists in srvctl but no info could be obtained
        Try crsctl. It may be in a funky state.

        /app/19.0.0/grid/bin/crsctl status resource "ora.tstdb.db"
        NAME=ora.tstdb.db
        TYPE=ora.database.type
        TARGET=ONLINE , OFFLINE
        STATE=OFFLINE, OFFLINE
    """
    debugg("get_crsctl_info()...starting with db = %s" % (db))
    global grid_home

    if not is_rac():
        return({ db: { 'home': 'unknown', 'version': 'unknown' } })

    #{ local_db: {'home':vhome.strip(), 'version': vversion}}
    return_info = { db: {  } }

    if not grid_home:
        grid_home = get_gihome()

    try:
        cmd_str = "%s/bin/crsctl status resource 'ora.%s.db' -f" % (grid_home, db)
        output = run_remote_cmd(cmd_str)
    except:
        return({ db: { 'home': 'unknown', 'version': 'unknown' } } )

    debugg("get_crsctl_info() output = %s " % (str(output)))

    if not output:
        return({ db: { 'home': 'unknown', 'version': 'unknown' } } )

    for line in output.split():
        debugg("get_crsctl_info() processing line = %s" % (line))
        if 'TARGET=' in line:
            tmp_tgt = line.split("=")[1].strip()
            return_info[db].update( { 'target': tmp_tgt }  )
        elif 'STATE=' in line:
            tmp_state = line.split("=")[1].strip()
            return_info[db].update( { 'state': tmp_state, 'state_details': tmp_state } )
        elif 'ORACLE_HOME=' in line:
            tmp_home = line.split("=")[1].strip()
            debugg("    get_crsctl_info() tmp_home = [ %s ] " % (tmp_home))
            vversion = strip_version(tmp_home)
            return_info[db].update( { 'home': tmp_home, 'version': vversion } )

    return(return_info)


def get_ora_homes():
   """Return the different Oracle and Grid homes versions installed on the host. Include opatch versions on the host and cluster name"""
   global ora_home
   global err_msg
   global v_rec_count
   global global_ora_home

   has_changed = False
   tempHomes = {}
   try:
      allhomes = str(commands.getstatusoutput("cat /etc/oratab | grep -o -P '(?<=:).*(?=:)' | sort | uniq | grep -e app")[1])
   except:
      err_msg = err_msg + ' ERROR: get_ora_homes(): (%s)' % (sys.exc_info()[0])

   for newhome in allhomes.split("\n"):
      if "grid" in newhome.lower():
         # use the path returned above 'newhome' and execute this command to get grid version:
         try:
           tmpver = str(commands.getstatusoutput(newhome + '/bin/crsctl query crs activeversion'))
         except:
           err_msg = err_msg + ' ERROR: get_ora_homes() - grid version: (%s)' % (sys.exc_info()[0])

         # get everything between '[' and ']' from the string returned.
         gver = tmpver[ tmpver.index('[') + 1 : tmpver.index(']') ]
         tempHomes.update({'grid': {'version': gver, 'home': newhome}})

         # cluster name
         try:
           clu_name = (os.popen(newhome + "/bin/olsnodes -c").read()).rstrip()
         except:
           err_msg = err_msg + ' ERROR: get_ora_homes() - cluster name: (%s)' % (sys.exc_info()[0])

         tempHomes.update({'cluster_name': clu_name})

         # node names in the cluster
         try:
           clu_names = get_nodes((os.popen(newhome + "/bin/olsnodes -n -i").read()).rstrip())
         except:
           err_msg = err_msg + ' ERROR: get_ora_homes() - node names in cluster: (%s)' % (sys.exc_info()[0])

         tempHomes.update({'nodes': clu_names})

         for (vkey, vvalue) in clu_names.items():
           tempHomes.update({vkey: vvalue})


      elif "home" in newhome.lower():
         homenum = str(re.search("\d.",newhome).group())

         # this command returns : Oracle Database 11g     11.2.0.4.0
         try:
           dbver = get_field(4, os.popen(newhome + "/OPatch/opatch lsinventory | grep 'Oracle Database'").read())
         except:
           err_msg = err_msg + ' ERROR: get_ora_homes() - db long version: (%s)' % (sys.exc_info()[0])

         # also see what version of opatch is running in each home: opatch version | grep Version
         try:
           opver = str(commands.getstatusoutput(newhome + "/OPatch/opatch version | grep Version"))
         except:
           err_msg = err_msg + ' ERROR: get_ora_homes() - OPatch version by ora_home: (%s)' % (sys.exc_info()[0])

         try:
           srvctl_ver = str(commands.getstatusoutput("export ORACLE_HOME=" + newhome +";" + newhome + "/bin/srvctl -V | awk '{ print $3 }'"))
         except:
           err_msg = err_msg + ' ERROR: get_ora_homes() - db long version: (%s)' % (sys.exc_info()[0])

         tempHomes.update({ homenum + "g": {'home': newhome, 'db_version': dbver, 'opatch_version': opver[opver.find(":")+1:-2], 'srvctl_version': srvctl_ver[5:-2]}})

   return (tempHomes)

def oracle_restart_homes():
   """Return the different Oracle and Grid homes versions installed on Oracle Restart hosts"""
   global ora_home
   global err_msg
   global v_rec_count
   global global_ora_home

   has_changed = False
   tempHomes = {}

   allhomes = run_command("cat /etc/oratab | grep -o -P '(?<=:).*(?=:)' | sort | uniq | grep -e app")

   for newhome in allhomes.split("\n"):
      if "grid" in newhome.lower():
         tmpver = run_command(newhome + '/bin/crsctl query has softwareversion')
         # get everything between '[' and ']' from the string returned.
         gver = tmpver[ tmpver.index('[') + 1 : tmpver.index(']') ]
         tempHomes.update({'grid': {'version': gver, 'home': newhome}})


      elif "home" in newhome.lower():
         homenum = str(re.search("\d.",newhome).group())
         if int(homenum) <= 12:
             homenum = homenum+"g"
         else:
             homenum = homenum+"c"

         opver = run_command('export ORACLE_HOME=' + newhome +';' + newhome + '/OPatch/opatch version | grep Version')[16:]
         srvctl_ver = run_command('export ORACLE_HOME=' + newhome +';' + newhome + '/bin/srvctl -V')[16:]

         tempHomes.update({ homenum: {'home': newhome, 'opatch_version': opver, 'srvctl_version': srvctl_ver }})

   return (tempHomes)


def get_db_status(local_vdb):
    """
    Return the status of the database on the node it runs on.
    The db name can be passed with, or without the instance number attached.
    The return value is only the status of the instance it runs on so the instance numbers is obtained and
    is used as an index on this list: ['ONLINE', 'ONLINE'] and that value is returned.
    """
    global grid_home
    global msg
    global debugme
    global spcl_case_dbs
    node_number = ""
    err_msg = ""
    node_status = []
    tmp_cmd = ""

    if not grid_home:
        grid_home = get_gihome()

    if not grid_home:
        err_msg = ' Error [1]: orafacts module get_db_status() error - retrieving local_grid_home: %s' % (grid_home)
        err_msg = err_msg + "%s, %s, %s %s" % (sys.exc_info()[0], sys.exc_info()[1], err_msg, sys.exc_info()[2])
        raise Exception (err_msg)

    node_number = int(get_node_num())

    if node_number is None:
        err_msg = ' Error [2]: orafacts module get_db_status() error - retrieving node_number: %s' % (node_number)
        err_msg = err_msg + "%s, %s, %s %s" % (sys.exc_info()[0], sys.exc_info()[1], err_msg, sys.exc_info()[2])
        raise Exception (err_msg)

    if "ASM" in local_vdb.upper():
        tmp_cmd = grid_home + "/bin/crsctl status resource ora.asm | grep STATE"
    elif "MGMTDB" in local_vdb.upper():
        tmp_cmd = grid_home + "/bin/crsctl status resource ora.mgmtdb | grep STATE"
    elif local_vdb[-1].isdigit() and local_vdb not in spcl_case_dbs:
        tmp_cmd = grid_home + "/bin/crsctl status resource ora." + local_vdb[:-1] + ".db | grep STATE"
    else:
        tmp_cmd = grid_home + "/bin/crsctl status resource ora." + local_vdb + ".db | grep STATE"

    try:
      process = subprocess.Popen([tmp_cmd], stdout=PIPE, stderr=PIPE, shell=True)
      output, code = process.communicate()
    except:
       err_msg = ' Error [3]: srvctl module get_db_status() error - retrieving oracle_home excpetion: %s' % (sys.exc_info()[0])
       err_msg = err_msg + "%s, %s, %s %s" % (sys.exc_info()[0], sys.exc_info()[1], err_msg, sys.exc_info()[2])
       raise Exception (err_msg)

    node_status=output.strip().split(",")                  #  ['STATE=OFFLINE', ' OFFLINE'] ['STATE=ONLINE on tlorad01', ' ONLINE on tlorad02']

    i = 0
    for item in node_status:
      if "STATE=" in item:
          node_status[i]=item.split("=")[1].strip()            # splits STATE and OFFLINE and returns status 'OFFLINE'
          if "ONLINE" in node_status[i]:
              node_status[i] = node_status[i].strip().split(" ")[0].strip().rstrip()
      elif "ONLINE" in item:
          node_status[i]=item.strip().split(" ")[0].strip().rstrip()
      elif "OFFLINE" in item:
          node_status[i]=item.strip().rstrip()
      i += 1

    if len(node_status) > 1:
        tmpindx = int(node_number) - 1
    elif len(node_status) == 1:
        tmpindx = 0

    if debugme:
        msg = msg + " debug info[101]: get_db_status(%s) called tmp_cmd: %s node_status: %s and status_this_node: %s" % (local_vdb, tmp_cmd, str(node_status), node_status[tmpindx])

    if node_number is not None:
        try:
            status_this_node = node_status[tmpindx]
        except:
            err_msg = ' Error[4]: orafacts module get_db_status() tmpindx %s items in the node_status list: %s contents: %s node_number: %s excpetion: %s grid_home: %s local_vdb: %s' % (tmpindx, len(node_status), str(node_status), node_number, sys.exc_info()[0], grid_home, local_vdb)
            err_msg = err_msg + "%s, %s, %s %s" % (sys.exc_info()[0], sys.exc_info()[1], err_msg, sys.exc_info()[2])
            raise Exception (err_msg)
    else:
       err_msg = ' Error[5]: orafacts module get_db_status() tmpindx %s items in the node_status list %s contents %s node_number %s excpetion: %s grid_home %s local_vdb %s' % (tmpindx, len(node_status), str(node_status), node_number, sys.exc_info()[0], grid_home, local_vdb)
       err_msg = err_msg + "exc_info(0) %s exc_info(1) %s err_msg %s exc_info(2) %s" % (sys.exc_info()[0], sys.exc_info()[1], err_msg, sys.exc_info()[2])
       raise Exception (err_msg)

    return(status_this_node)


def get_meta_data(local_db):
    """Return meta data for a database from crsctl status resource"""
    tokenstoget = ['TARGET', 'STATE', 'STATE_DETAILS']
    global grid_home
    global my_msg
    global msg
    global spcl_case_dbs

    local_ora_home = ""
    spcl_state = ""
    metadata = {}

    debugg("get_meta_data() called from rac_running_homes() with ....local_db:[ %s ]" % (local_db))

    if not grid_home:
        grid_home = get_gihome()

    debugg("get_meta_data() grid_home: %s" % (grid_home))

    # get host / node name
    tmp_cmd = "/bin/hostname | cut -d. -f1"
    debugg("get_meta_data() tmp_cmd #1: %s" % (tmp_cmd))

    try:
        process = subprocess.Popen([tmp_cmd], stdout=PIPE, stderr=PIPE, shell=True)
        output, code = process.communicate()
    except:
       my_msg = ' Error [1]: srvctl module get_orahome() error - retrieving oracle_home excpetion: %s' % (sys.exc_info()[0])
       my_msg = my_msg + "%s, %s, %s %s" % (sys.exc_info()[0], sys.exc_info()[1], my_msg, sys.exc_info()[2])
       raise Exception (my_msg)

    debugg("get_meta_data() output #1: %s" % (str(output)))

    node_name = output.strip()

    if 'mgmt' in local_db and node_name[-1:] == "2":
        return({})

    # the next command takes db name without instance number, so remove it if it exists
    if local_db[-1].isdigit() and local_db not in spcl_case_dbs:
        local_db = local_db[:-1]

    if 'mgmt' in local_db.lower():
        tmp_cmd = grid_home + "/bin/crsctl status resource ora.mgmtdb.db -v -n " + node_name
    else:
        tmp_cmd = grid_home + "/bin/crsctl status resource ora." + local_db + ".db -v -n " + node_name
    debugg("get_meta_data() tmp_cmd #2: %s" % (tmp_cmd))

    try:
        process = subprocess.Popen([tmp_cmd], stdout=PIPE, stderr=PIPE, shell=True)
        output, code = process.communicate()
    except:
       my_msg = ' Error [1]: srvctl module get_meta_data() output: %s' % (output)
       my_msg = my_msg + "%s, %s, %s %s" % (sys.exc_info()[0], sys.exc_info()[1], my_msg, sys.exc_info()[2])
       raise Exception (my_msg)

    debugg("get_meta_data() output #2: %s" % (str(output)))

    if not output and 'mgmt' not in local_db.lower():
        try:
            local_ora_home = get_orahome_procid(local_db)
            spcl_state = get_more_db_info(local_db, local_ora_home)
        except:
            err_msg = ' Error: get_meta_data(): call to get_more_db_info(): local_db: %s local_ora_home: %s spcl_state: %s' % (local_db, local_ora_home, spcl_state)
            err_msg = err_msg + "%s, %s, %s %s" % (sys.exc_info()[0], sys.exc_info()[1], err_msg, sys.exc_info()[2])
            debugg("post get_more_db_info()..called with local_db = %s  local_ora_home = %s  err_msg = %s" % (local_db, local_ora_home, err_msg))
            raise Exception (err_msg)

        metadata = {'STATE': spcl_state,'TARGET': 'unknown','STATE_DETAILS': 'unknown', 'status': 'unknown'}
        debugg("    metadata = %s" % (str(metadata)))

    else:

        vhomey = ""

        try:
            for item in output.split('\n'):
                debugg("get_meta_data() item: %s" % (str(item)))
                if item:
                    if "STATE_DETAILS" in item and ',' in item:
                        # item: STATE_DETAILS=Open,HOME=/app/oracle/12.1.0.2/dbhome_1
                        # STATE_DETAILS  Open,HOME  /app/oracle/12.1.0.2/dbhome_1
                        vkey, vvalue, vhomey = item.split("=")
                        vvalue = vvalue.split(",")[0].upper()
                        debugg("    STATE_DETAILS CASE: vkey = %s  vvalue =  %s  vhomey = %s\n" % (vkey, vvalue, vhomey))
                        metadata[vkey] = vvalue
                    else:
                        vkey, vvalue = item.split('=')
                        vkey = vkey.strip()
                        vvalue = vvalue.strip()
                        debugg("      vkey = %s  vvalue = %s" % ( vkey, vvalue))
                        if "STATE=" in vvalue:
                            vvalue=vvalue.split("=")[1].strip()
                            if "ONLINE" in vvalue:
                                vvalue = vvalue.strip().split(" ")[0].strip().rstrip()
                        elif "ONLINE" in vvalue:
                            vvalue=vvalue.strip().split(" ")[0].strip().rstrip()
                        elif "OFFLINE" in vvalue:
                            vvalue=vvalue.strip().rstrip()

                        if vkey in tokenstoget:
                            metadata[vkey] = vvalue
        except:
            my_msg = "ERROR: srvctl module get_meta_data(%s) error - loading metadata dict: %s" % (local_db, str(metadata))
            my_msg = my_msg + "%s, %s, %s %s" % (sys.exc_info()[0], sys.exc_info()[1], my_msg, sys.exc_info()[2])
            debugg(my_msg)
            raise Exception (my_msg)

    debugg(" get_meta_data() exiting for db: %s metadata dictionary contents : %s" % (local_db, str(metadata)))

    return(metadata)


def get_more_db_info(vtmpdb, vtmporahome):
    """When database isn't registerd with crsctl (instance in startup nomount for duplication etc.) get actual state of db"""
    global node_number
    global err_msg
    global os_path
    dbstate = ""

    if not node_number:
        node_number = get_node_num()

    tmpsid = vtmpdb + str(node_number)

    tmpsql = "select decode( status, 'STARTED', 'STARTED NOMOUNT', 'MOUNTED', 'STARTED MOUNT','OPEN','OPEN','OPEN MIGRATE', 'OPEN UPGRADE') from v$instance;"

    try:

        os.environ['ORACLE_HOME'] = vtmporahome
        os.environ['ORACLE_SID'] = tmpsid
        os.environ['NLS_DATE_FORMAT'] = 'Mon DD YYYY HH24:MI:SS'
        os.environ['PATH'] = os_path
        os.environ['USER'] = 'oracle'
        session = subprocess.Popen(['sqlplus', '-S', '/ as sysdba'],stdin=PIPE,stdout=PIPE,stderr=PIPE)
        session.stdin.write(tmpsql)
        (stdout,stderr) = session.communicate()

    except:
        err_msg = ' Error: get_more_db_info() opening session'
        err_msg = err_msg + "%s, %s, %s %s" % (sys.exc_info()[0], sys.exc_info()[1], err_msg, sys.exc_info()[2])
        raise Exception (err_msg)

    dbstate = stdout.split('\n')[3]

    return(dbstate)


def rac_running_homes():
    """Return running databases for RAC, their version, oracle_home, pid, status"""
    # This function will get all the running databases and the homes they're
    # running out of. The pgrep statement was taken from Tanel Poders website. http://blog.tanelpoder.com
    global err_msg
    global msg
    global v_rec_count
    global ora_home
    global grid_home
    global node_number
    global spcl_case_dbs
    tempstat = ""
    tempdb = ""
    local_cmd = ""
    dbs = {}
    meta_data = {}
    srvctl_dbs = []
    tmp_db_status = ""
    spcl_state = ""

    debugg("======= rac_running_homes()...starting...")

    if not node_number:
        node_number = get_node_num()
    debugg("rac_running_homes()...node_number = %s" % (node_number))

    # Get a list of running instances
    try:
        # cmd_str = "pgrep -lf _pmon_ | grep -v oracle | grep -v sh | /bin/sed 's/ora_pmon_/ /; s/asm_pmon_/ /' | /bin/grep -v sed"
        cmd_str = "ps -ef | grep _pmon_ | /bin/sed 's/ora_pmon_/ /; s/asm_pmon_/ /' | grep -v sed | grep -v grep | awk '{ print $2 \" \" $8}'"
        debugg("        cmd_str = %s\n" % (cmd_str))
        # vproc = str(commands.getstatusoutput(cmd_str)[1])
        vproc = run_remote_cmd(cmd_str)
    except:
        err_msg = ' Error: rac_running_homes() - pgrep lf pmon: (%s)' % (sys.exc_info()[0])
        debugg(err_msg)
        return

    debugg("rac_running_homes() cmd output \nSTART vproc: \n=========>\n%s \n<======= \n   END\n" % (str(vproc)))

    # vproc holds : pid db_name  ex. (6205  jfpwtest1\n ) in a stack if all running dbs
    for vdbproc in vproc.split("\n"):
        debugg("rac_running_homes() #1...vdbproc = [ %s ]" % (vdbproc))
        vprocid, vdbname = vdbproc.strip().split()
        debugg("rac_running_homes() #2...vprocid = %s    vdbname = %s" % (vprocid, vdbname))
        # get Oracle home the db process is running out of
        try:
          vhome = str(commands.getstatusoutput("sudo ls -l /proc/" + vprocid + "/exe | awk -F'>' '{ print $2 }' | sed 's/bin\/oracle$//' | sort | uniq"))
        except:
          err_msg = err_msg + ' Error: rac_running_homes() - vhome: (%s)' % (sys.exc_info()[0])

        debugg("    rac_running_homes()...vhome = %s" % (vhome))

        # Get the running database version from the Oracle home path that was returned:
        if "oracle" in vhome:
            vver = vhome[vhome.index("oracle")+7:vhome.index("dbhome")-1]
        elif "grid" in vhome:
            vver = vhome[vhome.index("app")+4:vhome.index("grid")-1]
        debugg("    rac_running_homes()...vver = %s" % (vver))

        ora_home = vhome[ vhome.find("/") : -3 ]
        debugg("    rac_running_homes()...ora_home = %s" % (ora_home))

        if "MGMTDB" in vdbname.upper():
            vdbname = "mgmtdb"

        tmpdbstatus = get_db_status(vdbname)    #<<<<<<<<<<<<<<<<<<<<<

        if not tmpdbstatus:
            tmpdbstatus = "unknown"

        debugg("    rac_running_homes()...tmpdbstatus = %s" % (tmpdbstatus))
        # tmpnodenum = int(node_number) - 1

        debugg("#1 ............CUTTING.....vdbname = %s" % (vdbname))
        if vdbname[-1].isdigit() and vdbname not in spcl_case_dbs:
            tmpdbname = vdbname[:-1]
        else:
            tmpdbname =  vdbname
        debugg("    rac_running_homes()...tmpdbname = %s ___________ after cutting........." % (tmpdbname))

        # get metadata (STATE=OFFLINE, STATE_DETAILS=Instance Shutdown, TARGET=OFFLINE) for each db
        if tmpdbname.lower() not in ["mgmtdb", "+asm"] and vdbname.lower() != "grid":
            try:
                debugg("line #738 tmpdbname = [ %s ] calling get_meta_data()" % (tmpdbname))
                metadata = {}
                metadata = get_meta_data(tmpdbname)
                if metadata:
                    dbs.update({vdbname: {'home': vhome[ vhome.find("/") - 1 : -3].strip(), 'version': vver, 'pid': vprocid, 'state': metadata['STATE'], 'target': metadata['TARGET'], 'state_details': metadata['STATE_DETAILS'], 'status': tmpdbstatus }} ) #[77]
            except:
                # err_msg = ' Error: loading dbs dict vdbname: %s home: %s version: %s pid: %s state: %s target: %s state_details: %s status: %s' % (vdbname, vhome[ vhome.find("/") - 1 : -3], vver, vprocid, metadata['STATE'], metadata['TARGET'],metadata['STATE_DETAILS'], tmpdbstatus )
                err_msg = 'Error: rac_running_homes() - get_meta_data() : %s ' % (vdbname)
                err_msg = err_msg + "%s, %s, %s %s" % (sys.exc_info()[0], sys.exc_info()[1], err_msg, sys.exc_info()[2])
                debugg("%s" % (err_msg))
                raise Exception (err_msg)
        else:
            dbs.update({vdbname: {'home': vhome[ vhome.find("/") - 1 : -3].strip(), 'version': vver, 'pid': vprocid, 'status': tmpdbstatus }} )
            debugg("    rac_running_homes()...dbs = %s" % (str(dbs)))


    # get a list of all databases registered with srvctl to find those offline
    local_cmd = ""
    tmporahome = get_installed_ora_homes2() # returns the highest ranking home
    local_cmd = "export ORACLE_HOME=" + tmporahome + "; " + tmporahome + "/bin/srvctl config"
    try:
      process = subprocess.Popen([local_cmd], stdout=PIPE, stderr=PIPE, shell=True)
      output, code = process.communicate()
    except:
       err_msg = err_msg + ' Error: srvctl module get_db_status() error - retrieving tmporahome: %s excpetion: %s' % (tmporahome, sys.exc_info()[0])
       err_msg = err_msg + "%s, %s, %s %s" % (sys.exc_info()[0], sys.exc_info()[1], err_msg, sys.exc_info()[2])
       raise Exception (err_msg)
    debugg("    rac_running_homes()...output = %s" % (str(output)))
    # put all the srvctl config databases in a list (srvctl_dbs)
    for i in output.strip().split("\n"):
        if i:
            srvctl_dbs.append(i)
    debugg("    srvctl_dbs=%s" % (str(srvctl_dbs)))

    # databases registered with srvctl but not already listed with running databases. (OFFLINE)
    local_cmd = ""
    vversion = ""
    vdatabase = ""
    vnextdb = ""
    tmpdbhome = {}
    tmpdbstatus = ""
    vmetadata={}

    for vdatabase in srvctl_dbs:
      debugg("for vdatabase=%s in srvctl_dbs")
      if is_rac:
          vnextdb = vdatabase + str(node_number)
      else:
          vnextdb = vdatabase

      if vnextdb not in dbs:

          msg = msg + "srvctl dbs %s" % (vnextdb)

          tmpdbhome = get_db_home_n_vers(vnextdb) # return_info = { local_db: {'home':vhome, 'version': vversion}}

          tempdbstatus = get_db_status(vnextdb)

          vmetadata = get_meta_data(vnextdb)

          if vnextdb[-1].isdigit() and vnextdb not in spcl_case_dbs:
              dbname = vnextdb[:-1]
          else:
              dbname = vnextdb

          debugg("[102] vnextdb: %s tmpdbhome[home]: %s tmpdbhome[version]: %s vmetadata %s" % (vnextdb, tmpdbhome[dbname]['home'], tmpdbhome[dbname]['version'], str(vmetadata) ))

          try:
              # dbs.update({vnextdb: {'home': tmpdbhome, 'version': vversion, 'status': tempdbstatus}})
              dbs.update({vnextdb: {'home': tmpdbhome[dbname]['home'].strip(), 'version': tmpdbhome[dbname]['version'], 'state': vmetadata['STATE'], 'target': vmetadata['TARGET'], 'state_details': vmetadata['STATE_DETAILS'], 'status': tempdbstatus }} ) #this should work with or without the error
          except:
               err_msg = ' Error: orafacts module rac_running_homes() error - adding srvctl homes not in dbs: %s %s' % (tmporahome, sys.exc_info()[0])
               err_msg = err_msg + msg
               err_msg = err_msg + "%s, %s, %s %s" % (sys.exc_info()[0], sys.exc_info()[1], err_msg, sys.exc_info()[2])
               raise Exception (err_msg)

    debugg("rac_running_homes() ....returning......dbs = %s" % (str(dbs)))

    return(dbs)


def si_running_homes():
    """Return running databases and the homes their running from for Single Instance Oracle installation"""
    global ora_home
    global v_rec_count
    dbs = {}

    # SI is different from RAC in that it doesn't use sudo for ls -l for finding vhome
    # This is more of an authentication problem we're having right now.
    # This function will get all the running databases and the homes they're
    # running out of. This was taken from Tanel Poders website. http://blog.tanelpoder.com

    # db_processes=os.system("pgrep -lf _pmon_ | /bin/sed 's/ora_pmon_/ /; s/asm_pmon_/ /' | grep -v sed")
    try:
        cmd_str = "pgrep -lf _pmon_ | grep -v oracle | /bin/sed 's/ora_pmon_/ /; s/asm_pmon_/ /' | grep -v sed"
        vproc = str(commands.getstatusoutput(cmd_str)[1])
    except:
        err_msg = ' Error: si_running_homes() - vproc: (%s)' % (sys.exc_info()[0])

    for vdbproc in vproc.split("\n"):
      vprocid,vdbname = vdbproc.split()

      try:
        vhome = str(commands.getstatusoutput("ls -l /proc/" + vprocid + "/exe | awk -F'>' '{ print $2 }' | sed 's/bin\/oracle$//' | sort | uniq")[1])
      except:
        err_msg = err_msg + ' Error: si_running_homes() - vhome: (%s)' % (sys.exc_info()[0])

      # Get the running database version from the Oracle home path:
      if "oracle" in vhome:
        vver = vhome[vhome.index("oracle")+7:vhome.index("dbhome")-1]
      elif "grid" in vhome:
        vver = vhome[vhome.index("app")+4:vhome.index("grid")-1]

    dbs.update({vdbname: {'home': vhome[1: -1], 'pid': vprocid, 'version': vver, 'status': 'running'}})
    ora_home = vhome[1: -1]

    return(dbs)


def is_rac():
    """Determine if a host is running RAC or Single Instance"""
    global err_msg
    global israc

    if israc is not None:
        return(israc)

    # Determine if a host is Oracle RAC ( return 1 ) or Single Instance ( return 0 )
    vproc = run_command("ps -ef | grep lck | grep -v grep | wc -l")

    if int(vproc) > 0:
      # if > 0 "lck" processes running, it's RAC
      israc = True
      return True
    else:
      israc = False
      return False


def is_oracle_restart():
    """Determine if a host is single instance Oracle Restart"""
    global err_msg

    if not is_rac():
        check_ocssd = run_command("ps -ef | grep ocss[d] | wc -l")
        if int(check_ocssd) > 0:
            return True
        else:
            return False

    return False

def is_ora_running():
    """Determine if Oracle database processses are running on a host"""
    try:
      vproc = str(commands.getstatusoutput("ps -ef | grep pmon | grep -v grep | wc -l")[1])
    except:
      err_msg = ' Error: is_ora_running() - proc: (%s)' % (sys.exc_info()[0])

    if int(vproc) == 0:
      # No databases are running
      return False
    elif int(vproc) > 0:
      return True


def is_ora_installed():
    """Quick determination if Oracle db software has been installed"""
    # Check if there's an /etc/oratab
    if os.path.isfile("/etc/oratab"):
      return True
    else:
      # no /etc/oratab installed, so Oracle may not be installed.
      return False


def tnsnames():
    """Locate tnsnames.ora file being used by this host"""
    # vtns1 = run_command("/bin/cat ~/.bash_profile | grep TNS_ADMIN | cut -d '=' -f 2")
    # vtns2 = run_command("/bin/cat ~/.bashrc | grep TNS_ADMIN | cut -d '=' -f 2")

    if is_rac() or is_oracle_restart():
        return(get_gihome()+'/network/admin')
    else:
        return(run_command(("/bin/cat ~/.bash_profile | grep TNS_ADMIN | cut -d '=' -f 2")))


def is_lsnr_up():
  """Determine if the local listener is up"""
  global err_msg
  global ora_home

  # determine if the listener is up and running - returns 1 if no listener running 0 if the listener is running
  try:
    vlsnr = str(commands.getstatusoutput("export ORACLE_HOME=" + ora_home + ";" + ora_home + "/bin/lsnrctl status | grep 'TNS-12560' | wc -l")[1])
  except:
    err_msg = err_msg + ' Error: is_lsnr_up() - vlsnr: (%s)' % (sys.exc_info()[0])

  # the command returns 1 if no listener, so return 0
  try:
    if int(vlsnr) == 0:
      return True
    else:
      return False
  except:
    err_msg = err_msg + 'Error: is_lsnr_up() - vlsnr: (%s)' % (sys.exc_info()[0])


def listener_info():
  """Return listner facts"""
  global ora_home
  global err_msg
  lsnrfax={}

  if is_lsnr_up():
    # Find lsnrctl parameter file
    try:
      temp = str(commands.getstatusoutput("export ORACLE_HOME=" + ora_home + "; " + ora_home + "/bin/lsnrctl status | grep Parameter | awk '{print $4}'")[1])
    except:
      err_msg = err_msg + ' Error: listener_info() - find parameter file: (%s)' % (sys.exc_info()[0])

    if temp:
      lsnrfax['parameter_file'] = temp
    else:
      lsnrfax['parameter_file'] = "No parameter file found."

    # Find lsnrctl alert log
    try:
      temp = str(commands.getstatusoutput("export ORACLE_HOME=" + ora_home + "; " + ora_home + "/bin/lsnrctl status | grep Log | awk '{print $4}'")[1])
    except:
      err_msg = err_msg + ' Error: listener_info() - find alert log : (%s)' % (sys.exc_info()[0])

    # add lsnrctl alert log to lsnrfax
    if temp:
      lsnrfax['log_file'] = temp[:-13] + "trace/listner.log"
    else:
      lsnrfax['log_file'] = "No listener.log found."

    # Find lsnrctl version
    try:
      temp = str(commands.getstatusoutput("export ORACLE_HOME=" + ora_home + "; " + ora_home + "/bin/lsnrctl status | grep Version | awk '{print $6}' | grep -v '-'")[1])
    except:
      err_msg = err_msg + ' Error: listener_info() - find lsnrctl version: (%s)' % (sys.exc_info()[0])

    # add lsnrctl version to lsnrfax
    if temp:
      lsnrfax['version'] = temp
    else:
      lsnrfax['version'] = "Listener version could not be determined."

     # add the oracle home this ran out of.
     if ora_home:
         lsnrfax['home'] = ora_home
     else:
         lsnrfax['home'] = "unknown"

    return(lsnrfax)

  else:

    return({"lsnrctl": "No listener running"})


def rac_dblist():
  """Return database information from srvctl"""
  global ora_home
  global err_msg
  dblist = []
  database_info = { 'database_details':{} }

  debugg("rac_dblist")

  global grid_home
  global msg

  if not grid_home:
      grid_home = get_gihome()

  srvctl_verbose = run_command("export ORACLE_HOME=" + grid_home + ";" + grid_home + "/bin/srvctl config database -verbose")

  if srvctl_verbose != "No databases are configured":
    for line in srvctl_verbose.split("\n"):
      split = line.split()
      item = {}

      srvctl_config = run_command("export ORACLE_HOME=" + split[1] + ";" + split[1] + "/bin/srvctl config database -d " + split[0])
      for x in srvctl_config.split("\n"):
          if x.startswith('Domain:'):
              item['domain'] = x[8:]
          elif x.startswith('Services:'):
              item['services'] = x[10:]

      dblist.append(split[0])
      database_info['database_details'].update({split[0]: {'oracle_home': split[1], 'version': split[2], 'domain': item['domain'], 'services': item['services']}})

  database_info['databases'] = dblist
  return(database_info)


def si_dblist():
  """Return database information from /etc/oratab"""
  global err_msg
  global msg
  dblist = []
  database_info = { 'database_details':{} }

  debugg("si_dblist")

  try:
    oratab = str(commands.getstatusoutput("cat /etc/oratab | grep -v '^#\|^\s*$' | cut -d: -f 1")[1])
  except:
    err_msg = err_msg + ' Error: si_dblist() - oratab: (%s)' % (sys.exc_info()[0])

  if oratab:
    for dbname in oratab.split("\n"):
      dblist.append(dbname)

      oracle_home = str(commands.getstatusoutput("grep " + dbname + " /etc/oratab |cut -d: -f2 -s")[1])
      version = str(commands.getstatusoutput("grep " + dbname + " /etc/oratab | grep -o '[0-9][0-9]\.[0-9].[0-9].[0-9]'")[1])
      database_info['database_details'].update({dbname: {'oracle_home': oracle_home, 'version': version }})

  database_info['databases'] = dblist
  return(database_info)


def get_version(local_db):
    """Return the general Oracle version for a given database"""
    global grid_home
    global msg
    global spcl_case_dbs

    if not grid_home:
        grid_home = get_gihome()

    if local_db[:-1].isdigit() and local_db not in spcl_case_dbs:
        tmp_cmd = "/bin/cat /etc/oratab | /bin/grep -m 1 " + local_db[:-1] + " | cut -d/ -f4"
    else:
        tmp_cmd = "/bin/cat /etc/oratab | /bin/grep -m 1 " + local_db + " | cut -d/ -f4"

    try:
      process = subprocess.Popen([tmp_cmd], stdout=PIPE, stderr=PIPE, shell=True)
      output, code = process.communicate()
    except:
        msg = msg + ' ERROR [5] get_version() retrieving version for database : %s' % (local_db)
        # module.fail_json(msg='ERROR: %s' % (err_msg), changed=False)

    oracle_version = output.strip()

    if "12" in oracle_version:
        return("12")
    elif "11" in oracle_version:
        return("11")
    else:
        return("unk")


def host_name():
    """Return the hostname"""
    global msg

    cmd_str = "/bin/hostname"

    try:
        process = subprocess.Popen([cmd_str], stdout=PIPE, stderr=PIPE, shell=True)
        output, code = process.communicate()
    except:
        msg = msg + ' ERROR [33] host_name() error obtaining hostname on linux : %s' % (local_db)
        module.fail_json(msg='ERROR: %s' % (err_msg), changed=False)

    tmphost = output.strip()

    return(tmphost)


def get_orahome_procid(vdb):
    """Get database Oracle Home from the running process."""
    global global_ora_home

    if 'mgmt' in vdb.lower():
        return
    # get the pmon process id for the running database.
    # 10189  tstdb1
    try:
      vproc = str(commands.getstatusoutput("pgrep -lf _pmon_" + vdb + " | /bin/sed 's/ora_pmon_/ /; s/asm_pmon_/ /' | /bin/grep -v sed")[1])
    except:
      err_cust_err_msg = 'Error: get_orahome_procid() - pgrep lf pmon: (%s)' % (sys.exc_info()[0])
      err_cust_err_msg = cust_err_msg + "%s, %s, %s %s" % (sys.exc_info()[0], sys.exc_info()[1], err_msg, sys.exc_info()[2])
      raise Exception (err_msg)

    # if the database isnt running (no process id)
    # try getting oracle_home from /etc/oratab
    if not vproc:
        tmp_home = get_dbhome(vdb)
        if tmp_home:
            return tmp_home
        else:
            exit_msg = "Error determining oracle_home for database: %s all attempts failed! (proc id, srvctl, /etc/oratab)"
            sys.exit(exit_msg)

    # ['10189', 'tstdb1']
    vprocid = vproc.split()[0]

    # get Oracle home the db process is running out of
    # (0, ' /app/oracle/12.1.0.2/dbhome_1/')
    try:
      vhome = str(commands.getstatusoutput("sudo ls -l /proc/" + vprocid + "/exe | awk -F'>' '{ print $2 }' | sed 's/\/bin\/oracle$//' ")[1])
    except:
      custom_err_msg = 'Error[ get_orahome_procid() ]:  (%s)' % (sys.exc_info()[0])
      err_cust_err_msg = cust_err_msg + "%s, %s, %s %s" % (sys.exc_info()[0], sys.exc_info()[1], err_msg, sys.exc_info()[2])
      raise Exception (err_msg)

    ora_home = vhome.strip()

    if not global_ora_home:
        global_ora_home = ora_home
    else:
        if ora_home > global_ora_home:
            global_ora_home = ora_home

    return(ora_home)


def get_scan(ora_home):
    """Get scan listener info"""

    if not ora_home:
        ora_home = "none specified"
    else:
        ora_home = ora_home.strip()

    scan_info = {}

    # Get the scan listner name first test-scan.ccci.org
    try:
       # command to create a manual AWS RDS snapshot
       tmp_cmd = "%s/bin/srvctl config scan | /bin/grep name | /bin/awk '{ print $3 }'" % (ora_home)
    except:
       err_msg = 'orafacts get_scan() Error trying to concatenate the following: tmp_cmd: [ %s ] in orafacts' % (vdb_inst_id,vdb_snap_id)
       err_msg = err_msg + "%s, %s, %s, %s" % (sys.exc_info()[0], sys.exc_info()[1], err_msg, sys.exc_info()[2])
       raise Exception (err_msg)

    try:
      os.environ['USER'] = 'oracle'
      os.environ['ORACLE_HOME'] = ora_home
      process = subprocess.Popen([tmp_cmd], stdout=PIPE, stderr=PIPE, shell=True)
      output, code = process.communicate()
    except:
      err_msg = ' Error [1]: orafacts module get_scan() output: %s' % (output)
      err_msg = err_msg + "%s, %s, %s, %s" % (sys.exc_info()[0], sys.exc_info()[1], err_msg, sys.exc_info()[2])
      raise Exception (err_msg)

    tmp_scan_listener = output.strip()[:-1]

    scan_info.update({'scan_listener': tmp_scan_listener })

    # get ip addresses next
    try:
       # command to create a manual AWS RDS snapshot
       tmp_cmd = "%s/bin/srvctl config scan -all | /bin/grep VIP | grep '[0-9]' | /bin/awk '{ print $5 }'" % (ora_home)
    except:
       err_msg = 'orafacts get_scan() Error trying to concatenate the following: tmp_cmd: [ %s ] in orafacts' % (vdb_inst_id,vdb_snap_id)
       err_msg = err_msg + "%s, %s, %s, %s" % (sys.exc_info()[0], sys.exc_info()[1], err_msg, sys.exc_info()[2])
       raise Exception (err_msg)

    try:
      os.environ['USER'] = 'oracle'
      os.environ['ORACLE_HOME'] = ora_home
      process = subprocess.Popen([tmp_cmd], stdout=PIPE, stderr=PIPE, shell=True)
      output, code = process.communicate()
    except:
      err_msg = ' Error [1]: orafacts module get_scan() output: %s' % (output)
      err_msg = err_msg + "%s, %s, %s, %s" % (sys.exc_info()[0], sys.exc_info()[1], err_msg, sys.exc_info()[2])
      raise Exception (err_msg)

    tmp_ips = output.strip()

    idx = 1
    for item in tmp_ips.splitlines():
        vip = "ip%s" % (idx)
        scan_info.update({vip: item})
        idx += 1

    if not scan_info:
        scan_info.update({'Error': 'Unable to get scan info. srvctl may be down' })

    return (scan_info)

def run_command(cmd):
    """Runs given shell command, returns stdout."""
    global err_msg

    try:
        p = subprocess.Popen([cmd], stdout=PIPE, stderr=PIPE, shell=True)
        output, code = p.communicate()
    except:
       err_msg = err_msg + ' Error run_cmd: %s' % (cmd)
       err_msg = err_msg + "%s, %s, %s" % (sys.exc_info()[0], sys.exc_info()[1], sys.exc_info()[2])
       raise Exception (err_msg)

    return output.strip()

# ================================== Main ======================================
def main(argv):
  global ora_home
  global err_msg
  global v_rec_count
  global msg
  global global_ora_home
  global spcl_case_dbs

  ansible_facts={ 'orafacts': {} }
  facts = {}

  module = AnsibleModule(
      argument_spec = dict(
      ),
      supports_check_mode = True,
  )

  if is_ora_installed():
    if is_ora_running():

      # get the hostname to passback:
      try:
         dest_host = 'ora_facts_' + str(commands.getstatusoutput("hostname | sed 's/\..*//'")[1])
      except:
         err_msg = err_msg + ' Error: retrieving hostname: (%s)' % (sys.exc_info()[0])

      # Run these functions for RAC:  <<< ============================== RAC
      if is_rac():
        msg = msg + "RAC Environment"

        # get GRID_HOME and VERSION, ORACLE_HOMES and VERSIONS and Opatch version
        all_homes = get_ora_homes()
        for (vkey, vvalue) in all_homes.items():
            ansible_facts['orafacts'][vkey] = vvalue

        # define dictionary to hold all databases registered with srvctl
        ansible_facts['orafacts']['all_dbs']={}
        # this returns running databases, their PID and the homes they're running out of
        run_homes = rac_running_homes()
        debugg("run_homes = %s" % (str(run_homes)))
        # Loop through all databases (running and offline) and make a list of dbs and status
        # helpful in tasks or playbooks to iterate through databases of certain version or status (offline/online)
        if run_homes:
            for vkey, vvalue in run_homes.items():
                debugg("\nmain() :: looping through individual databases: vkey=%s vvalue=%s" % (vkey, vvalue))
                ansible_facts['orafacts'][vkey] = vvalue
                if "+asm" not in vkey.lower() and "pmon" not in vkey.lower() and "mgmtdb" not in vkey.lower():
                    if vkey[-1:].isdigit() and vkey not in spcl_case_dbs:
                        tmpdb = vkey[:-1]
                    debugg("    processing vkey = %s   tmpdb= %s" % (vkey, tmpdb))
                    if not tmpdb[-1].isdigit() or tmpdb in spcl_case_dbs:
                        tmpdb = tmpdb + str(node_number)
                    tmpver = get_version(tmpdb)
                    ansible_facts['orafacts']['all_dbs'].update({tmpdb: {'status': vvalue['status'], 'version': tmpver, 'metadata': ansible_facts['orafacts'][tmpdb]['state_details'].upper()}})

            debugg("    ansible_facts => ansible_facts['orafacts']['all_dbs'] = %s" % (str(ansible_facts['orafacts']['all_dbs'])))

        # Get list of all databases configured in SRVCTL
        ansible_facts.update(rac_dblist())

        # Add scan info
        vorahome = ansible_facts['orafacts']['12g']['home']
        tmpscan = get_scan(vorahome)
        ansible_facts['orafacts']['scan'] = tmpscan

        # vhuge = hugepages()
        # ansible_facts_dict['contents']['hugepages'] = vhuge['hugepages']
      elif is_oracle_restart(): # Run these for Oracle Restart Instance <<< ========================= SI_ASM
        msg = msg + "Single Instance Oracle Restart Environment"

        homes = oracle_restart_homes()
        for (vkey, vvalue) in homes.items():
            ansible_facts['orafacts'][vkey] = vvalue

        #database_details
        # Get list of all databases configured in SRVCTL
        ansible_facts.update(rac_dblist())

        #databases


      else: # Run these for Single Instance <<< ========================= SI
        msg="Single Instance (SI) Environment"

        # get single instance running databases and their homes
        run_homes = si_running_homes()
        if run_homes:
          for (vkey, vvalue) in run_homes.items():
            ansible_facts['orafacts'][vkey] = vvalue
        else:
          msg = msg + ".\n It appears No Oracle database is running."

        # Get list of all databases in /etc/oratab
        ansible_facts.update(si_dblist())

      # Run the following functions for both RAC and SI
      # Get tnsnames info
      vtmp = tnsnames()
      ansible_facts['orafacts']['tnsnames'] = vtmp + "/tnsnames.ora"
      ansible_facts['orafacts']['tns_admin'] = vtmp

      # Get local listener info
      vtmp = listener_info()
      ansible_facts['orafacts']['lsnrctl'] = vtmp

      vtmp = host_name()
      ansible_facts['orafacts']['host_name'] = vtmp

      # Add any error messages caught before passing back
      if err_msg:
        msg = msg + err_msg

      module.exit_json( msg=msg , ansible_facts=ansible_facts , changed="False")

      sys.exit(0)

    else:
      msg="\nOracle does not appear to be running. (No pmon services running)"
  else:
    msg="\nOracle does not appear to be installed on this host. (No /etc/oratab file found)"

  msg = msg + err_msg

  module.fail_json( msg=msg )

  sys.exit(1)

# code to execute if this program is called directly
if __name__ == "__main__":
   main(sys.argv)
