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
                    'version': '0.1'}

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

def get_field(fieldnum, vstring):
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


def get_rac_homes():
   """Return the different Oracle and Grid homes versions installed on the host. Include opatch versions on the host and cluster name"""
   global ora_home
   has_changed = False
   tempHomes = {}
   allhomes = str(commands.getstatusoutput("cat /etc/oratab | grep -o -P '(?<=:).*(?=:)' | sort | uniq | grep -e app")[1])
   for newhome in allhomes.split("\n"):
      if "grid" in newhome.lower():
         # tempHomes.update({'GRID': {'HOME': newhome})
         # use the path returned above 'newhome' and execute this command to get grid version:
         tmpver = str(commands.getstatusoutput(newhome + '/bin/crsctl query crs activeversion'))
         # get everything between '[' and ']' from the string returned.
         gver = tmpver[ tmpver.index('[') + 1 : tmpver.index(']') ]
         tempHomes.update({'grid': {'version': gver, 'home': newhome}})
         # cluster name
         clu_name = (os.popen(newhome + "/bin/olsnodes -c").read()).rstrip()
         tempHomes.update({'cluster_name': clu_name})
         # node names in the cluster
         clu_names = get_nodes((os.popen(newhome + "/bin/olsnodes -n -i").read()).rstrip())
         for (vkey, vvalue) in clu_names.items():
           tempHomes.update({vkey: vvalue})
      elif "home" in newhome.lower():
         homenum = str(re.search("\d.",newhome).group())
         ora_home = newhome
         # tempHomes.update({'ORACLE_' + homenum + '_HOME' : newhome})
         # this command returns : Oracle Database 11g     11.2.0.4.0
         dbver = get_field(4, os.popen(newhome + "/OPatch/opatch lsinventory | grep 'Oracle Database'").read())
         # also see what version of opatch is running in each home: opatch version | grep Version
         # opver = get_field(3, commands.getstatusoutput(newhome + "/OPatch/opatch version | grep Version"))
         opver = str(commands.getstatusoutput(newhome + "/OPatch/opatch version | grep Version"))
         srvctl_ver = str(commands.getstatusoutput("export ORACLE_HOME=" + newhome +";" + newhome + "/bin/srvctl -V | awk '{ print $3 }'"))
         tempHomes.update({ homenum + "g": {'home': newhome, 'db_version': dbver, 'opatch_version': opver[opver.find(":")+1:-2], 'srvctl_version': srvctl_ver[5:-2]}})
        #  tempHomes.update({ homenum + "g": {'HOME': newhome, 'VERSION': dbver}})

   return (tempHomes)


def rac_running_homes():
    """Return running databases, their versions and the homes they are running out of for RAC installation"""
    # This function will get all the running databases and the homes they're
    # running out of. The pgrep statement was taken from Tanel Poders website. http://blog.tanelpoder.com
    dbs = {}
    # vwhoami = str(commands.getstatusoutput("whoami")[1])  <<== result of this was "oracle"
    # db_processes=os.system("pgrep -lf _pmon_ | /bin/sed 's/ora_pmon_/ /; s/asm_pmon_/ /' | grep -v sed")
    vproc = str(commands.getstatusoutput("pgrep -lf _pmon_ | /bin/sed 's/ora_pmon_/ /; s/asm_pmon_/ /' | grep -v sed")[1])
    for vdbproc in vproc.split("\n"):
        vprocid,vdbname = vdbproc.split()
        # This commented out command provided by oracle uses grep against /etc/oratab...doesn't help during upgrade
        # vhome = str(commands.getstatusoutput("/app/oracle/12.1.0.2/dbhome_1/bin/dbhome " + vdbname))
        vhome = str(commands.getstatusoutput("sudo ls -l /proc/" + vprocid + "/exe | awk -F'>' '{ print $2 }' | sed 's/bin\/oracle$//' | sort | uniq"))
        # Get the running database version from the Oracle home path:
        if "oracle" in vhome:
            vver = vhome[vhome.index("oracle")+7:vhome.index("dbhome")-1]
        elif "grid" in vhome:
            vver = vhome[vhome.index("app")+4:vhome.index("grid")-1]
            #vver = vhome[vhome.index("app")+4:vhome.index("grid")-1]
        # This SHOULD work, but because we have a problem with LDAP the above command
        # gives this : "home": "sudo: ldap_sasl_bind_s(): Invalid credentials\\n /app/oracle/12.1.0.2/dbhome_1/"
        # so we need to edit out the error:
        # dbs.update({vdbname: {'home': vhome[5:-2], 'pid': vprocid}})
        # index 5 gets rid of (0, ' out of (0, '/app/12.1.0.2/grid/network/admin')
        # con = cx_Oracle.connect('system/'+ {{ database_passwords[db_name][item] }} + '@' + vdbname + '.ccci.org')
        # print con.version
        # con.close()
        # dbs.update({vdbname: {'home': vhome[ 5 + vhome.find("/") - 5 : -2], 'version': vver, 'status': 'running'}}) #this should work with or without the error
        dbs.update({vdbname: {'home': vhome[ vhome.find("/") - 1 : -2], 'version': vver, 'status': 'running'}}) #this should work with or without the error
    #dbs.update({'whoami': vwhoami}) #running as "oracle"
    return(dbs)


def si_running_homes():
    #"""Return running databases and the homes their running from for Single Instance Oracle installation"""
      # SI is different from RAC in that it doesn't use sudo for ls -l for finding vhome
      # This is more of an authentication problem we're having right now.
      # This function will get all the running databases and the homes they're
      # running out of. This was taken from Tanel Poders website. http://blog.tanelpoder.com
      dbs = {}
      # db_processes=os.system("pgrep -lf _pmon_ | /bin/sed 's/ora_pmon_/ /; s/asm_pmon_/ /' | grep -v sed")
      vproc = str(commands.getstatusoutput("pgrep -lf _pmon_ | /bin/sed 's/ora_pmon_/ /; s/asm_pmon_/ /' | grep -v sed")[1])
      for vdbproc in vproc.split("\n"):
          vprocid,vdbname = vdbproc.split()
          vhome = str(commands.getstatusoutput("ls -l /proc/" + vprocid + "/exe | awk -F'>' '{ print $2 }' | sed 's/bin\/oracle$//' | sort | uniq"))
          dbs.update({vdbname: {'home': vhome[ vhome.find("/") - 1 : -2], 'pid': vprocid}})

      return(dbs)


def is_rac():
    # Determine if a host is Oracle RAC ( return 1 ) or Single Instance ( return 0 )
    vproc = str(commands.getstatusoutput("ps -ef | grep lck | grep -v grep | wc -l")[1])

    if int(vproc) > 0:
      # if > 0 "lck" processes running, it's RAC
      return (1)
    else: # else it's Single Instance
      return (0)


def tnsnames():
    # global ora_home
    # Find which TNS_NAMES file is being used:
    # first check for TNS_ADMIN environmental variable

    # This command returns something like this : /app/12.1.0.2/grid/network/admin/tnsnames.ora
    # v_cmd = "/usr/bin/strace " + ora_home + "/bin/sqlplus -L scott/tiger@orcl 2>&1| /bin/grep -i 'open.*tnsnames.ora' | /bin/awk -F'\"' '{ print $2 }' | /usr/bin/head -n 1"
    # v_cmd = "/usr/bin/strace " + ora_home + "/bin/sqlplus -L scott/tiger@orcl 2>&1| /bin/grep -i 'open.*tnsnames.ora'"
    # vtns1 = str(commands.getstatusoutput(v_cmd))
    # vtns1 = str(commands.getstatusoutput("strace sqlplus -L scott\/tiger\@orcl 2>&1| grep -i 'open.*tnsnames.ora'"))
    # p = subprocess.Popen(v_cmd, stdout=subprocess.PIPE, shell=True )
    # (output, err) = p.communicate()
    # p_status = p.wait()
    # vtns1 = str(output)
    # vtns1 = str(commands.getstatusoutput("strace sqlplus -L scott/tiger@orcl 2>&1| grep -i 'open.*tnsnames.ora' | awk -F'\"' '{ print $2 }' | head -n 1")[1])
    # re.findall(r'"([^"]*)"', inputString)

    # subprocess.check_output(['strace', ora_home + '/bin/sqlplus', '-L', 'scott/tiger@orcl', '2>&1', '|', 'grep', '-i', 'open.*tnsnames.ora', 'awk', '-F\'\"', '{ print $2 }', '|', 'head', '-n', '1'])

    vtns1 = str(commands.getstatusoutput("/bin/cat ~/.bash_profile | grep TNS_ADMIN | cut -d '=' -f 2")[1])
    # scriptpath="/u01/oracle/bin/"
    # vtns1 = str(os.system(scriptpath+"tnsloc.sh"))
    # vtns1 = subprocess.check_output(['/u01/oracle/bin/tnsloc.sh'], shell=True)
    # vtns1 = commands.getstatusoutput('python --version', shell=True)
    # proc =  Popen(['sudo su - oracle | /u01/oracle/bin/tnsloc.sh'], stdout=PIPE)
    # vtns1 = proc.communicate()[0].split()
    # d = dict(os.environ)
    # d['LD_LIBRARY_PATH'] = '$ORACLE_HOME/lib:$LD_LIBRARY_PATH'
    # subprocess.Popen(['/u01/oracle/bin/tnsloc.sh'])
    # return(vtns1)
    if vtns1:
        return(str(vtns1) + "/tnsnames.ora")
    else:
        if os.path.exists(ora_home + "/network/admin/tnsnames.ora"):
          return(ora_home + "/network/admin/tnsnames.ora")
        else:
          return("not located or does not exist")


def listener_info():
  """Return listner facts"""
  global ora_home
  lsnrfax={}

  temp = str(commands.getstatusoutput("export ORACLE_HOME=" + ora_home + ";" + ora_home + "/bin/lsnrctl status | grep Parameter | awk '{print $4}'")[1])
  if temp:
    lsnrfax['parameter_file'] = temp
  else:
    lsnrfax['parameter_file'] = "not located or does not exist"

  temp = str(commands.getstatusoutput("export ORACLE_HOME=" + ora_home + ";" + ora_home + "/bin/lsnrctl status | grep Log | awk '{print $4}'")[1])
  if temp:
    lsnrfax['log_file'] = temp[:-13] + "trace/listner.log"
  else:
    lsnrfax['log_file'] = "not located or does not exist"

  temp = str(commands.getstatusoutput("export ORACLE_HOME=" + ora_home + ";" + ora_home + "/bin/lsnrctl status | grep Version | awk '{print $6}' | grep -v '-'")[1])
  if temp:
    lsnrfax['version'] = temp
  else:
    lsnrfax['version'] = "not determined"
  return(lsnrfax)


def get_si_homes():
   """Return the different Oracle and Grid homes versions installed on the host. Include opatch versions on the host and cluster name"""
   global ora_home
   has_changed = False
   tempHomes = {}
   allhomes = str(commands.getstatusoutput("cat /etc/oratab | grep -o -P '(?<=:).*(?=:)' | sort | uniq | grep -e app")[1])
   for newhome in allhomes.split("\n"):
      if "home" in newhome.lower():
         homenum = str(re.search("\d.",newhome).group())
         ora_home = newhome
         # tempHomes.update({'ORACLE_' + homenum + '_HOME' : newhome})
         # this command returns : Oracle Database 11g     11.2.0.4.0
         #  dbver = get_field(4, os.popen(newhome + "/OPatch/opatch lsinventory | grep 'Oracle Database'").read())
        #  dbver = str(get_field(4, commands.getstatusoutput("export ORACLE_HOME=" + newhome + "; " + newhome + "/OPatch/opatch lsinventory | grep 'Oracle Database'")[1]))
         dbver = str(commands.getstatusoutput("export ORACLE_HOME=" + newhome + "; " + newhome + "/OPatch/opatch lsinventory | grep 'Oracle Database' | awk '{ print $4 }'")[1])
         # also see what version of opatch is running in each home: opatch version | grep Version
         # opver = get_field(3, commands.getstatusoutput(newhome + "/OPatch/opatch version | grep Version"))
         opver = str(commands.getstatusoutput(newhome + "/OPatch/opatch version | grep Version"))
        #  srvctl_ver = str(commands.getstatusoutput("export ORACLE_HOME=" + newhome +";" + newhome + "/bin/srvctl -V | awk '{ print $3 }'"))
         tempHomes.update({ homenum + "g": {'home': newhome, 'db_version': dbver, 'opatch_version': opver[opver.find(":")+1:-2] }})
        #  tempHomes.update({ homenum + "g": {'HOME': newhome, 'VERSION': dbver}})

   return (tempHomes)

# ================================== Main ======================================
# def main(argv):
def main(argv):
  global ora_home
  tmpfacts = {}
  ansible_facts={ 'orafacts': {} }

  module = AnsibleModule(
      argument_spec = dict(
      ),
      supports_check_mode = True,
  )


  # check if Oracle install is Single Instance (SI) or Real Application Cluster (RAC)
  vrac = is_rac()
  if vrac == 1:
    msg="RAC Environment"
  elif vrac == 0:
    msg="Single Instance Environment"
  else:
    msg="Error determing RAC or SI"

  # get the hostname to passback:
  dest_host = 'ora_facts_' + str(commands.getstatusoutput("hostname | sed 's/\..*//'")[1])


  # Run these functions for RAC:  <<< ============================== RAC
  if is_rac():
    # get GRID_HOME and VERSION, ORACLE_HOMES and VERSIONS and Opatch version
    all_homes = get_rac_homes()
    for (vkey, vvalue) in all_homes.items():
      ansible_facts['orafacts'][vkey] = vvalue
      # ansible_facts['orafacts'].update({vkey: vvalue})
      # tmpfacts[vkey] = vvalue


    # this returns running databases, their PID and the homes they're running out of
    run_homes = rac_running_homes()
    for (vkey, vvalue) in run_homes.items():
      ansible_facts['orafacts'][vkey] = vvalue
      # tmpfacts[vkey] = vvalue

    # vhuge = hugepages()
    # ansible_facts_dict['contents']['hugepages'] = vhuge['hugepages']

  else: # Run these for Single Instance <<< ========================= SI

    all_homes = get_si_homes()
    for (vkey, vvalue) in all_homes.items():
      ansible_facts['orafacts'][vkey] = vvalue

    run_homes = si_running_homes()
    for (vkey, vvalue) in run_homes.items():
      ansible_facts['orafacts'][vkey] = vvalue
      # tmpfacts[vkey] = vvalue


  # ora_home = ansible_facts['orafacts']['11g']['home']

  # Run these functions on either RAC or SI

  # Get tnsnames info
  vtmp = tnsnames()
  ansible_facts['orafacts']['tnsnames'] = vtmp
  # tmpfacts['tnsnames'] = vtmp

  vtmp = listener_info()
  ansible_facts['orafacts']['lsnrctl'] = vtmp


  module.exit_json( msg=msg, ansible_facts=ansible_facts , changed="False")

# code to execute if this program is called directly
if __name__ == "__main__":
   main(sys.argv)
