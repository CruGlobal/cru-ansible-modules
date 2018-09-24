#!/opt/rh/python27/root/usr/bin/python

from ansible.module_utils.basic import *
from ansible.module_utils.facts import *
import commands
import subprocess
import sys
import os
import json
import re
import math
import os.path
import pdb
from subprocess import PIPE, Popen

# global variables
debugme = True
grid_home = ""
oracle_home = ""
node_number = ""
debug_msg = ""
debugme = True
vphys_mem = ""

ANSIBLE_METADATA = {'status': ['stableinterface'],
                    'supported_by': 'Cru DBA team',
                    'version': '0.1'}

DOCUMENTATION = '''
---
module: memck ( memory check )
short_description: This memory determines if there is enough memory on the destination
    host to add and start up another database. If not it attempts to adjust the
    sga and pga settings so that the database will start and fit in available memory.

notes: If the pfile location is provided this module will adjust the sga_target and
    pga_aggregate_target accordingly.
'''

EXAMPLES = '''

    # if cloning or restoring a database place this call just before the
    # startup new database with pfile.
    - name: Check memory on destination host
      memck:
        src_sga_tgt: "{{ sourcefacts['sga_target'] }}"
        src_pga_agg_tots: "{{ sourcefacts['pga_aggregate_target'] }}"
        dest_mem_info: "{{ orafacts['hugepages'] }}"
        pfile: "{{ oracle_stage }}/pfile.ora"

    notes:
        pfile: is optional. If given the module will attempt to update
        all occurrances of sga_target and __pga_aggregate_target

'''


def get_gihome():
    """Determine the Grid Home directory"""
    global grid_home

    try:
      process = subprocess.Popen(["/bin/ps -eo args | /bin/grep ocssd.bin | /bin/grep -v grep | /bin/awk '{print $1}'"], stdout=PIPE, stderr=PIPE, shell=True)
      output, code = process.communicate()
    except:
        err_msg = err_msg + ' get_gihome() retrieving GRID_HOME : (%s,%s)' % (sys.exc_info()[0],code)
        module.fail_json(msg='ERROR: %s' % (err_msg), changed=False)

    grid_home = (output.strip()).replace('/bin/ocssd.bin', '')

    if not grid_home:
         err_msg = err_msg + ' Error: srvctl module get_gihome() error - retrieving grid_home : %s output: %s' % (grid_home, output)
         err_msg = err_msg + "%s, %s, %s %s" % (sys.exc_info()[0], sys.exc_info()[1], err_msg, sys.exc_info()[2])
         raise Exception (err_msg)

    return(grid_home)


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


def hugepages():
    """Gather Hugepage information from the host including database parameters for all running databases."""

#     "hugepages": {
#     "meminfo": {
#         "AnonHugePages": "0 kB",
#         "HugePages_Free": "34433",
#         "HugePages_Rsvd": "67",
#         "HugePages_Surp": "0",
#         "HugePages_Total": "76800",
#         "Hugepagesize": "2048 kB",
#         "MemFree": "45329568 kB",
#         "MemTotal": "264476444 kB",
#         "SwapFree": "17825788 kB",
#         "SwapTotal": "17825788 kB"
#     },
#     "memlock": {
#         "hard": "238029005",
#         "soft": "238029005"
#     },
#     "os_nr_hugepages_conf": "76800",
#     "pga_aggregte_target_totals": 0,
#     "sga_target_totals": "0",
#     "transparent_hugepages": "never"
#     }
#     }
#     }

    global grid_home
    global node_number
    global debugme
    global debug_msg
    sga_target_running_tot = 0
    pga_agg_running_tot = 0
    parameters_to_get = ['sga_target','pga_aggregate_target','memory_target','use_large_pages']

    hg_info = { 'hugepages': {} }
    hg_info['hugepages'].update( {'meminfo': {} })

    # get system memory information
    try:
        cmd_str = "/bin/grep 'Huge\|Mem\|Swap' /proc/meminfo | grep -v Cached"
        process = subprocess.Popen([cmd_str], stdout=PIPE, stderr=PIPE, shell=True)
        output, code = process.communicate()
    except:
        custom_err_msg = 'Error[ checking if alias already exists ] cmd_str: %s' % (cmd_str)
        custom_err_msg = custom_err_msg + "%s, %s, %s" % (sys.exc_info()[0], sys.exc_info()[1], sys.exc_info()[2])
        raise Exception (custom_err_msg)

    for item in output.strip().split("\n"):
        tmp = item.split()
        if len(tmp) == 3:
            vtitle = tmp[0][:-1]
            vsize  = "%s %s" % (tmp[1],tmp[2])
            hg_info['hugepages']['meminfo'].update({ vtitle: vsize })
        elif len(tmp) == 2:
            vtitle = tmp[0][:-1]
            vsize  = "%s" % (tmp[1])
            hg_info['hugepages']['meminfo'].update({ vtitle: vsize })

    # server configuration setting for number of hugepages in Huge Pages pool
    try:
        cmd_str = "/bin/cat /etc/sysctl.conf | /bin/grep huge | cut -d '=' -f2"
        process = subprocess.Popen([cmd_str], stdout=PIPE, stderr=PIPE, shell=True)
        output, code = process.communicate()
    except:
        custom_err_msg = 'Error[ checking if alias already exists ] cmd_str: %s' % (cmd_str)
        custom_err_msg = custom_err_msg + "%s, %s, %s" % (sys.exc_info()[0], sys.exc_info()[1], sys.exc_info()[2])
        raise Exception (custom_err_msg)

    os_cnf_num_hugepages = output.strip()

    if not os_cnf_num_hugepages:
        # If the first method didn't work and os_num_hugepages is null try this method
        # server configuration for number of hugepages in Huge Pages pool
        try:
            cmd_str = "/bin/cat /proc/sys/vm/nr_hugepages"
            process = subprocess.Popen([cmd_str], stdout=PIPE, stderr=PIPE, shell=True)
            output, code = process.communicate()
        except:
            custom_err_msg = 'Error[ checking if alias already exists ] cmd_str: %s' % (cmd_str)
            custom_err_msg = custom_err_msg + "%s, %s, %s" % (sys.exc_info()[0], sys.exc_info()[1], sys.exc_info()[2])
            raise Exception (custom_err_msg)

        os_cnf_num_hugepages = output.strip()

    hg_info['hugepages'].update( {'os_nr_hugepages_conf' : os_cnf_num_hugepages } )

    # soft and hard memlock
    try:
        cmd_str = "/bin/cat /etc/security/limits.conf | grep memlock | grep -v '#'"
        process = subprocess.Popen([cmd_str], stdout=PIPE, stderr=PIPE, shell=True)
        output, code = process.communicate()
    except:
        custom_err_msg = 'Error[ getting free hugepages ] cmd_str: %s' % (cmd_str)
        custom_err_msg = custom_err_msg + "%s, %s, %s" % (sys.exc_info()[0], sys.exc_info()[1], sys.exc_info()[2])
        raise Exception (custom_err_msg)

    memlock1, memlock2 = output.strip().split("\n")

    memlock1 = memlock1.split()
    memlock2 = memlock2.split()

    title1 = "%s_memlock" % (memlock1[1])
    title2 = "%s_memlock" % (memlock2[1])

    hg_info['hugepages'].update( {'memlock': { memlock1[1] : memlock1[3], memlock2[1]: memlock2[3] } } )

    # Get transparent hugepages info
    try:
        cmd_str = "sudo /bin/cat /etc/grub.conf | /bin/grep transpar | /bin/grep -v '#' | /bin/grep `uname -r` | /bin/grep -Po 'transparent_hugepage=\K[^ ]+'"
        process = subprocess.Popen([cmd_str], stdout=PIPE, stderr=PIPE, shell=True)
        output, code = process.communicate()
    except:
        custom_err_msg = 'Error[ getting free hugepages ] cmd_str: %s' % (cmd_str)
        custom_err_msg = custom_err_msg + "%s, %s, %s" % (sys.exc_info()[0], sys.exc_info()[1], sys.exc_info()[2])
        raise Exception (custom_err_msg)

    xparent = output.strip()

    hg_info['hugepages'].update( { 'transparent_hugepages': xparent } )

    if not node_number:
        node_number = int(get_node_num())

    hg_info['hugepages'].update( {'sga_target_totals' : str(sga_target_running_tot), 'pga_aggregte_target_totals': pga_agg_running_tot  } )

    return(hg_info)


def main ():

    global debugme

    ansible_facts={}

    # Name to call facts dictionary being passed back to Ansible
    # This will be the name you reference in Ansible. i.e. source_facts['sga_target'] (source_fact)
    refname = 'oramemck'
    ansible_facts = { refname:{} }

    module = AnsibleModule(
      argument_spec = dict(
        src_sga_tgt       =dict(required=True),
        src_pga_agg_tots  =dict(required=True),
        dest_mem_info     =dict(required=True),
        pfile             =dict(required=False)
      ),
      supports_check_mode=True,
    )

    # Get arguements passed from Ansible playbook
    vsrc_sga_tgt        = module.params.get('src_sga_tgt')
    vsrc_pga_agg_tots   = module.params.get('src_pga_agg_tots')
    vdest_mem_info      = module.params.get('dest_mem_info')
    vpfile              = module.params.get('pfile')

    vchanged = False

    vmeminfo = hugepages()

    vphys_mem = vmeminfo['hugepages']['meminfo']['MemTotal']

    if debugme:
        msg = "vsrc_sga_tgt: %s vsrc_pga_agg_tots: %s vdest_mem_info: %s vpfile_dest: %s vphys_mem: %s " % (vsrc_sga_tgt,vsrc_pga_agg_tots,vdest_mem_info,vpfile,vphys_mem)

    # print json.dumps( ansible_facts_dict )
    module.exit_json( msg=msg, ansible_facts=ansible_facts , changed=vchanged)

# code to execute if this program is called directly
if __name__ == "__main__":
   main()
