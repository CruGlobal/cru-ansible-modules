#!/opt/rh/python27/root/usr/bin/python
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
#  [ ]  2) listeners being used
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
from ansible.module_utils.basic import *
from ansible.module_utils.facts import *
import commands
import json
import sys
import os
import os.path
import subprocess
from subprocess import PIPE,Popen
import re


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

'''
ora_home = ''
err_msg=''
v_rec_count=0

def get_field(fieldnum, vstring):
    """Simple fuction to return a field from a string of items"""
    x = 1
    for i in vstring.split():
      if fieldnum == x:
        return i
      else:
        x += 1


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


def get_ora_homes():
   """Return the different Oracle and Grid homes versions installed on the host. Include opatch versions on the host and cluster name"""
   global ora_home
   global err_msg
   global v_rec_count

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


def rac_running_homes():
    """Return running databases for RAC, their versions and the homes they are running out of for RAC installation"""
    # This function will get all the running databases and the homes they're
    # running out of. The pgrep statement was taken from Tanel Poders website. http://blog.tanelpoder.com
    global err_msg
    global v_rec_count
    global ora_home
    dbs = {}

    try:
      vproc = str(commands.getstatusoutput("pgrep -lf _pmon_ | /bin/sed 's/ora_pmon_/ /; s/asm_pmon_/ /' | grep -v sed")[1])
    except:
      err_msg = err_msg + ' Error: rac_running_homes() - vproc: (%s)' % (sys.exc_info()[0])

    for vdbproc in vproc.split("\n"):
        vprocid,vdbname = vdbproc.split()
        # get Oracle home the db process is running out of
        try:
          vhome = str(commands.getstatusoutput("sudo ls -l /proc/" + vprocid + "/exe | awk -F'>' '{ print $2 }' | sed 's/bin\/oracle$//' | sort | uniq"))
        except:
          err_msg = err_msg + ' Error: rac_running_homes() - vhome: (%s)' % (sys.exc_info()[0])

        # Get the running database version from the Oracle home path:
        if "oracle" in vhome:
            vver = vhome[vhome.index("oracle")+7:vhome.index("dbhome")-1]
        elif "grid" in vhome:
            vver = vhome[vhome.index("app")+4:vhome.index("grid")-1]

        ora_home = vhome[ vhome.find("/") : -3 ]
        dbs.update({vdbname: {'home': vhome[ vhome.find("/") - 1 : -2], 'version': vver, 'pid': vprocid, 'status': 'running'}}) #this should work with or without the error



    #dbs.update({'whoami': vwhoami}) #running as "oracle"
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
      vproc = str(commands.getstatusoutput("pgrep -lf _pmon_ | /bin/sed 's/ora_pmon_/ /; s/asm_pmon_/ /' | grep -v sed")[1])
    except:
      err_msg = err_msg + ' Error: si_running_homes() - vproc: (%s)' % (sys.exc_info()[0])

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

    # Determine if a host is Oracle RAC ( return 1 ) or Single Instance ( return 0 )
    try:
      vproc = str(commands.getstatusoutput("ps -ef | grep lck | grep -v grep | wc -l")[1])
    except:
      err_msg = err_msg + ' Error: is_rac() - vproc: (%s)' % (sys.exc_info()[0])

    if int(vproc) > 0:
      # if > 0 "lck" processes running, it's RAC
      return True
    else:
      return False


def is_ora_running():
    """Determine if Oracle database processses are running on a host"""
    try:
      vproc = str(commands.getstatusoutput("ps -ef | grep pmon | grep -v grep | wc -l")[1])
    except:
      err_msg = err_msg + ' Error: is_ora_running() - proc: (%s)' % (sys.exc_info()[0])

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
    try:
      vtns1 = str(commands.getstatusoutput("/bin/cat ~/.bash_profile | grep TNS_ADMIN | cut -d '=' -f 2")[1])
    except:
      err_msg = err_msg + ' Error: tnsnames() - vtns1: (%s)' % (sys.exc_info()[0])

    if vtns1:
        return(str(vtns1) + "/tnsnames.ora")
    else:
        return("Could not locate tnsnames.ora file.")


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
  if int(vlsnr) == 0:
    return True
  else:
    return False


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

    if temp:
      lsnrfax['log_file'] = temp[:-13] + "trace/listner.log"
    else:
      lsnrfax['log_file'] = "No listener.log found."

    # Find lsnrctl version
    try:
      temp = str(commands.getstatusoutput("export ORACLE_HOME=" + ora_home + "; " + ora_home + "/bin/lsnrctl status | grep Version | awk '{print $6}' | grep -v '-'")[1])
    except:
      err_msg = err_msg + ' Error: listener_info() - find lsnrctl version: (%s)' % (sys.exc_info()[0])

    if temp:
      lsnrfax['version'] = temp
    else:
      lsnrfax['version'] = "Listener version could not be determined."

    return(lsnrfax)

  else:
    return({"lsnrctl": "No listener running"})


# ================================== Main ======================================
def main(argv):
  global ora_home
  global err_msg
  global v_rec_count

  ansible_facts={ 'orafacts': {} }

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
        msg="RAC Environment"

        # get GRID_HOME and VERSION, ORACLE_HOMES and VERSIONS and Opatch version
        all_homes = get_ora_homes()
        for (vkey, vvalue) in all_homes.items():
          ansible_facts['orafacts'][vkey] = vvalue

        # this returns running databases, their PID and the homes they're running out of
        run_homes = rac_running_homes()
        for (vkey, vvalue) in run_homes.items():
          ansible_facts['orafacts'][vkey] = vvalue

        # vhuge = hugepages()
        # ansible_facts_dict['contents']['hugepages'] = vhuge['hugepages']

      else: # Run these for Single Instance <<< ========================= SI
        msg="Single Instance (SI) Environment"

        # get single instance running databases and their homes
        run_homes = si_running_homes()
        if run_homes:
          for (vkey, vvalue) in run_homes.items():
            ansible_facts['orafacts'][vkey] = vvalue
        else:
          msg = msg + ".\n It appears No Oracle database is running."


      # Run the following functions for both RAC and SI
      # Get tnsnames info
      vtmp = tnsnames()
      ansible_facts['orafacts']['tnsnames'] = vtmp

      # Get local listener info
      vtmp = listener_info()
      ansible_facts['orafacts']['lsnrctl'] = vtmp

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
