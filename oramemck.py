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
# defaults:
default_sga_target_amt = 2000
default_sga_target_units = "mb"
default_pga_aggregate_target_amt = 1000
default_pga_aggregate_target_units = "mb"


ANSIBLE_METADATA = {'status': ['stableinterface'],
                    'supported_by': 'Cru DBA team',
                    'version': '0.1'}

DOCUMENTATION = '''
---
module: oramemck ( Oracle memory check - HugePages )

short_description:
    This module determines if there is enough memory on the destination server
    to start up another database. If not it attempts to adjust the sga and pga settings
    so that the database will start and fit in available memory.
        1. Will check source sga_target and pga_aggregate_target to see if it will fit.
        2. If source ( sga_target and pga_aggregate_target ) amounts are too big it will check predefined amounts
           against available size on the destination server and programmed defaults * (sga_target: 2000MB and pga_aggregate_target: 1000MB)
           * These programmed default amounts are what where required to stand up a restore of SBLCRMP.

notes:
    If the pfile location is provided this module will adjust the sga_target and
    pga_aggregate_target in the pfile.

'''

EXAMPLES = '''

    # If cloning or restoring a database place this call just before the
    # 'startup new database with pfile' task.
    # Argument units should be (KB) and/or add the size units to the argument.
    # i.e. '2040 MB' or '2048 mb' either way 1024 is assumed for calculations.
    - name: Check memory on destination host
      oramemck:
        src_sga_target: "{{ sourcefacts['sga_target'] }}"
        src_sga_max_size: "{{ sourcefacts['sga_max_size']}}"
        src_pga_aggregate_target: "{{ sourcefacts['pga_aggregate_target'] }}"
        src_memory_target: "{{ sourcefacts['memory_target']}}"
        src_memory_max_target: "{{ sourcefacts['memory_max_target']}}"
        src_use_large_pages: "{{ sourcefacts['use_large_pages']}}"
        dest_sga_target: "{{ database_parameters[dest_db_name].sga_target }}"
        dest_pga_aggregate_target: "{{ database_parameters[dest_db_name].pga_aggregate_target }}"
        pfile: "{{ oracle_stage }}/pfile.ora"

    notes:
        pfile: is optional. If given the module will attempt to update
        all occurrances of sga_target and pga_aggregate_target

'''


def split_amt_n_long_units(vinput):
    """Take a string input of format '2000M' or '2000MB' and convert to '2000' and 'MB' and return amount '2000' and units (k,m,g) in long format 'mb' """
    j = 0
    for vchar in reversed(vinput):
        if vchar.isdigit():
            print "vchar: %s is a digit. vcount: %s" % (vchar,j)
            tmp_units = vinput[-j:]
            idx = len(vinput) - j
            tmp_amt = vinput[:idx]
            break
        j += 1

    if len(tmp_units) == 1:
        if tmp_units.lower() = "k":
            tmp_units = "kb"
        elif tmp_units.lower() = "m":
            tmp_units = "mb"
        elif tmp_units.lower() = "g":
            tmp_units = "gb"

    return tmp_amt,tmp_units


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


def shared_mem_in_use():
    """Find and return Shared Hugepages in use"""

    # get system memory information
    try:
        cmd_str = "/usr/bin/ipcs -m|awk '{ print $5}'|awk '{a+=$0}END{print a}'"
        process = subprocess.Popen([cmd_str], stdout=PIPE, stderr=PIPE, shell=True)
        output, code = process.communicate()
    except:
        custom_err_msg = 'Error[shared_mem_in_use()] cmd_str: %s' % (cmd_str)
        custom_err_msg = custom_err_msg + "%s, %s, %s" % (sys.exc_info()[0], sys.exc_info()[1], sys.exc_info()[2])
        raise Exception (custom_err_msg)

    vshared_bytes = output.strip()

    return(vshared_bytes)


def hugepages():
    """Gather Hugepage information from the host including database parameters for all running databases."""
    # Example of the dictionary this function returns:  https://www.kernel.org/doc/Documentation/vm/hugetlbpage.txt
    # --------------------------------------
    #     "hugepages": {
    #     "meminfo": {
    #         "AnonHugePages": "0 kB",
    #         "HugePages_Free": "34433",    # is the number of huge pages in the pool that are not yet allocated.
    #         "HugePages_Rsvd": "67",       # a commitment to allocate from the pool has been made, but no allocation has yet been made. These come from HugePages_Free
    #         "HugePages_Surp": "0",        # surplus - the number of huge pages in the pool above the value in /proc/sys/vm/nr_hugepages.
    #         "HugePages_Total": "76800",   # Total number of HugePages - is the size of the pool of huge pages.
    #         "Hugepagesize": "2048 kB",    # default hugepage size (in Kb).
    #         "MemFree": "45329568 kB",
    #         "MemTotal": "264476444 kB",   # Total Physical memory
    #         "SwapFree": "17825788 kB",
    #         "SwapTotal": "17825788 kB"
    #     },
    #     "memlock": {                      # The memlock parameter specifies how much memory the oracle user can lock into its address space. Note that Huge Pages are locked in physical memory. The memlock setting is specified in KB and must match the memory size of the number of Huge Pages that Oracle should be able to allocate.
    #         "hard": "238029005", (kb by definition)
    #         "soft": "238029005"  (kb by definition)
    #     },
    #     "nr_hugepages": "76800",          # indicates the current number of "persistent" huge pages in the kernel's huge page pool.
    #     "transparent_hugepages": "never"
    #     }
    #     }
    #     }
    #
    # Hugetlb         ( Translation Lookaside Buffer - tlb ) is the total amount of memory (in kB), consumed by huge
    #                 pages of all sizes.
    #                 If huge pages of different sizes are in use, this number
    #                 will exceed HugePages_Total * Hugepagesize. To get more
    #                 detailed information, please, refer to
    #                 /sys/kernel/mm/hugepages (described below).
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
        cmd_str = "/bin/grep 'Huge\|Mem\|Swap' /proc/meminfo | /bin/grep -v Cached"
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
        cmd_str = "/bin/cat /etc/sysctl.conf | /bin/grep huge | /bin/cut -d '=' -f2"
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

    hg_info['hugepages'].update( {'nr_hugepages' : os_cnf_num_hugepages } )

    # soft and hard memlock
    try:
        cmd_str = "/bin/cat /etc/security/limits.conf | /bin/grep memlock | /bin/grep -v '#'"
        process = subprocess.Popen([cmd_str], stdout=PIPE, stderr=PIPE, shell=True)
        output, code = process.communicate()
    except:
        custom_err_msg = 'Error[ getting free hugepages ] cmd_str: %s' % (cmd_str)
        custom_err_msg = custom_err_msg + "%s, %s, %s" % (sys.exc_info()[0], sys.exc_info()[1], sys.exc_info()[2])
        raise Exception (custom_err_msg)

    memlock_soft, memlock_hard = output.strip().split("\n")

    memlock_soft = memlock1.split()
    memlock_hard = memlock2.split()

    title1 = "%s_memlock" % (memlock_soft[1])
    title2 = "%s_memlock" % (memlock_hard[1])

    hg_info['hugepages'].update( {'memlock': { memlock_soft[1] : memlock_soft[3], memlock_hard[1]: memlock_hard[3] } } )

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

    # if not node_number:
    #     node_number = int(get_node_num())

    hg_info['hugepages'].update( {'sga_target_totals' : str(sga_target_running_tot), 'pga_aggregte_target_totals': pga_agg_running_tot  } )

    return(hg_info)


def convert_to(vsize_amt,vsize_unit_now,vsize_unit_to):
    """Input arguments 1: amount, 2 current_unit, 3 convert to units. Pass back new amt and unit"""

    if int(vsize_amt) == 0:
        return vsize_amt,vsize_unit_to
    elif vsize_unit_now == vsize_unit_to:
        return int(vsize_amt),vsize_unit_to

    size_name = ("bytes", "kb", "mb", "gb", "tb", "pb") # , "E", "Z", "Y")

    # where you are index# (vnow) converting to index# (vthen)
    vidx_now  = int(size_name.index(vsize_unit_now))
    vidx_then = int(size_name.index(vsize_unit_to))

    # Figure out how many places to move up or down.
    vdiff = vidx_then - vidx_now

    # if moving up multiply
    if vdiff > 0:
        vfactor = math.pow(1024, vdiff)
        new_size = int(vsize_amt) * vfactor
    # if moving down divide
    else:
        abs(vdiff)
        vfactor = math.pow(1024, vdiff)
        new_size = round(int(vsize_amt)/vfactor, 2)

    return int(round(new_size)),size_name[vfactor]


def msgg(new_str):
    """This function adds info to the msg that will be returned to the playbook from the module"""
    global msg

    if not msg:
        msg = new_str
    else:
        msg = "%s %s" % (msg,new_str)


def debugg(new_str):
    """Adding debugging info to msg to be passed back if debugme=True"""
    global msg
    global debugme

    if debugme == True:
        if not msg:
            msg = new_str
        else:
            msg = "%s %s" % (msg,new_str)


def get_db_home(local_db):
    """Using /etc/oratab return the Oracle Home for the database"""
    global err_msg
    return_info = {}

    if local_db[-1].isdigit():
        local_db = local_db[:-1]

    try:
      cmd_str = "/bin/cat /etc/oratab | /bin/grep -m 1 %s | /bin/cut -d ':' -f 2" % (local_db)
      process = subprocess.Popen([cmd_str], stdout=PIPE, stderr=PIPE, shell=True)
      output, code = process.communicate()
    except:
      err_msg = err_msg + " Error: orafacts module get_db_home_n_vers() - retrieving oracle_home and version"
      err_msg = err_msg + "%s, %s, %s, %s" % (sys.exc_info()[0], sys.exc_info()[1], err_msg, sys.exc_info()[2])
      raise Exception (err_msg)

    vhome = output.strip()

    return(vhome)


def get_pga_sga_totals():
    """Get a list of running databases on the host and select sga_target and pga_aggregate_target from each db while keeping a running total.
       Returning python dictionary object : { 'sga_totals': sga_totals, 'sga_units': 'kb', 'pga_totals': pga_totals, 'pga_units': 'kb' }"""

    db_param = ["sga_target","pga_aggregate_target"]
    sga_totals = 0
    pga_totals = 0
    fx_info = {}

    # Get transparent hugepages info
    try:
        cmd_str = "/usr/bin/pgrep -lf _pmon_ | /bin/cut -d '_' -f 3 | /bin/grep -v 'MGMTDB\|ASM1\|cut'"
        process = subprocess.Popen([cmd_str], stdout=PIPE, stderr=PIPE, shell=True)
        output, code = process.communicate()
    except:
        custom_err_msg = 'Error[ getting free hugepages ] cmd_str: %s' % (cmd_str)
        custom_err_msg = custom_err_msg + "%s, %s, %s" % (sys.exc_info()[0], sys.exc_info()[1], sys.exc_info()[2])
        raise Exception (custom_err_msg)

    running_dbs = output.strip().split("\n")

    for oracle_sid in running_dbs:

        # get db home for this db: /bin/cat /etc/oratab | /bin/grep -m 1 jfpwdev1 | /bin/cut -d ':' -f 2
        oracle_home = get_db_home(db)

        # Get sga_target and pga_aggregate_target for each database converting to kb during select
        for item in db_param:

            try:
                cmd_str1 = 'export ORACLE_HOME=%s; export ORACLE_SID=%s; %s/bin/sqlplus / as sysdba' % (oracle_home,oracle_sid,oracle_home)
                cmd_str2 = "select value/1024 from v$parameter where name = '%s';" % (db_param) # kb
                process = subprocess.Popen(cmd_str1, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
                output, code = process.communicate(cmd_str2)
            except:
                custom_err_msg = 'Error[ retrieving hugepage parameters ] oracle_home: %s oracle_sid: %s parameter: %s cmd_str1: %s cmd_str2: %s debug_msg: %s' % (oracle_home,oracle_sid,db_param,cmd_str1,cmd_str2,debug_msg)
                custom_err_msg = custom_err_msg + "%s, %s, %s" % (sys.exc_info()[0], sys.exc_info()[1], sys.exc_info()[2])
                raise Exception (custom_err_msg)

            tmp_value = output.strip()

            if item == "sga_target":
                sga_totals = int(sga_totals) + int(tmp_value)
            elif item == "pga_aggregate_target":
                pga_totals = int(pga_totals) + int(tmp_value)

    # values are in kb as noted. parameters (bytes) were converted during select with /1024 = kb
    fx_info = { "sga_totals": sga_totals, "sga_units": "kb", "pga_totals": pga_totals, "pga_units": "kb" }

    return(fx_info)


def number_recommended_hugepages(hugepg_size,hugepg_size_unit):
    """(Doc ID 401749.1) Compute values for the recommended HugePages/HugeTLB configuration for the current shared memory segments on Oracle Linux.
       Input arg1 string HugePage size (amount) and arg2 string units (kb).
       If kernel version is correct (2.4, 2.6, 3.8, 3.10, 4.1) the function returns number of recommended HugePages."""

    # get kernel version (i.e. 2.6)
    try:
        cmd_str = "/bin/uname -r | /bin/grep -Po '^...'"
        process = subprocess.Popen(cmd_str, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
        output, code = process.communicate(cmd_str)
    except:
        custom_err_msg = 'Error[ retrieving hugepage parameters ] oracle_home: %s oracle_sid: %s parameter: %s cmd_str1: %s cmd_str2: %s debug_msg: %s' % (oracle_home,oracle_sid,db_param,cmd_str1,cmd_str2,debug_msg)
        custom_err_msg = custom_err_msg + "%s, %s, %s" % (sys.exc_info()[0], sys.exc_info()[1], sys.exc_info()[2])
        raise Exception (custom_err_msg)

    vkern = output.strip()

    # Get a sum total of shared memory segements currently running on the host
    # This is a moving target so don't take the value as hard.
    try:
        cmd_str = "ipcs -m|awk '{ print $5}'| /bin/awk '{a+=$0}END{print a}'"
        process = subprocess.Popen(cmd_str, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
        output, code = process.communicate(cmd_str)
    except:
        custom_err_msg = 'Error[ retrieving hugepage parameters ] oracle_home: %s oracle_sid: %s parameter: %s cmd_str1: %s cmd_str2: %s debug_msg: %s' % (oracle_home,oracle_sid,db_param,cmd_str1,cmd_str2,debug_msg)
        custom_err_msg = custom_err_msg + "%s, %s, %s" % (sys.exc_info()[0], sys.exc_info()[1], sys.exc_info()[2])
        raise Exception (custom_err_msg)

    # Shared memory being used (bytes)
    vrunning_shared_mem = output.strip()

    # Convert shared memory being used (bytes) to kb:                            from    to
    vrun_shared_mem_size,vrun_shared_mem_units = convert_to(vrunning_shared_mem,"bytes","kb")

    # make sure arguments passed in: hugepage_size is in kb: convert_to(vsize_amt,vsize_unit_now,vsize_unit_to):
    if hugepg_size_unit.lower() != "kb":
        hugepg_size,hugepg_unit = convert_to(hugepage_size,hugepg_unit,"kb")

    # number of pages required to handle the currently running shared memory segments, Oracle and otherwise.
    # total required number of pages = shared_memory (kb) / HugePage_size (kb)
    req_pgs = int(vrunning_shared_mem) / int(hugepg_size)

    # return recommendation for HugePage pool size based on kernel version
    # Translation Lookaside Buffer (hugetlb)
    # '2.4') HUGETLB_POOL=`echo "$NUM_PG*$HPG_SZ/1024" | bc -q`;
    if vkern in ("2.4","2.6","3.8","3.10","4.1"):
        return req_pgs
    else:
        msgg "Unsupported kernel version - unable to execute recommended_hugepages() function."
        return 1


def main ():
    """
    This module will check a destination host to see if there is enough memory or hugepages to startup
    another instance. If there's not enough space for the predefined sga_target and pga_aggregate_target
    it will attempt to suggest another size for sga_target and pga_aggregate_target to fit in the available space,
    and allow the database to startup and still fit within the available space. HugePages are cosidered 'pinned' in memory.
    Therefore, one must assure that memlock is set to account for the at least the total amount of huge pages that will be allocated.
    """
    global debugme
    vchanged = False
    ansible_facts={}

    # Name to call facts dictionary being passed back to Ansible
    # This will be the name you reference in Ansible. i.e. source_facts['sga_target'] (source_fact)
    refname = 'oramemck'
    ansible_facts = { refname:{} }

    module = AnsibleModule(
      argument_spec = dict(
        src_sga_target                     =dict(required=True),
        src_sga_max_size                   =dict(required=True),
        src_pga_aggregate_target           =dict(required=True),
        src_memory_target                  =dict(required=True),
        src_memory_max_target              =dict(required=True),
        src_use_large_pages                =dict(required=True),
        pre_defined_sga_target             =dict(required=True),
        pre_defined_pga_aggregate_target   =dict(required=True),
        pfile                              =dict(required=True)
      ),
      supports_check_mode=True,
    )

    # Get arguements passed from Ansible playbook
    vsrc_sga_target                    = module.params.get('src_sga_target')
    vsrc_sga_max_size                  = module.params.get('sga_max_size')
    vsrc_pga_aggregate_target          = module.params.get('src_pga_aggregate_target')
    vsrc_memory_target                 = module.params.get('src_memory_target')
    vsrc_memory_max_target             = module.params.get('src_memory_max_target')
    vsrc_use_large_pages               = module.params.get('src_use_large_pages')
    vpre_defined_sga_target            = module.params.get('pre_defined_sga_target')
    vpre_defined_pga_aggregate_target  = module.params.get('pre_defined_pga_aggregate_target')
    vpfile                             = module.params.get('pfile')

    # You cannot use AMM with hugepages.
    # Disable AMM by setting MEMORY_TARGET and MEMORY_MAX_TARGET to 0
    if vsrc_memory_target != 0 and vsrc_memory_max_target != 0:
        tmp_str = "Automatic Memory Management (AMM) CANNOT be used with hugepages. To disable AMM set MEMORY_TARGET and MEMORY_MAX_TARGET to 0."
        msgg(tmp_str)

    # Call hugepages() function and get server hugepage info
    tmp = hugepages()
    vmeminfo = tmp['hugepages']['meminfo']
    # available hugepages = Total -  Reserved
    vphys_mem       = vmeminfo['MemTotal']          # actual physical memory on the server
    vhuge_free      = vmeminfo['HugePages_Free']    # number of free HugePages - number of unused HugePages
    vhuge_rsvd      = vmeminfo['HugePages_Rsvd']    # number of reserved HugePages - not yet allocated ( not yet subtracted from 'free', but reserved for use.)
    vhuge_tot       = vmeminfo['HugePages_Total']   # number of total HugePages
    tmp_huge_size   = vmeminfo['Hugepagesize']      # HugePage mem size (kb)
    vmemlock_hard   = vmeminfo['memlock']['hard']   # Memlock_hard (kb)
    vmemlock_soft   = vmeminfo['memlock']['soft']   # Memlock_soft (kb)

    # split tmp_huge_size string into amount (size) and units (kb) e.x. tmp_huge_size : '2048 kB'
    vhuge_size_amt,vhuge_size_units = tmp_huge_size.split()
    # if hugepage_size is not kb convert to kb:
    if vhuge_size_units.lower() != "kb":
        vhuge_size_amt,vhuge_size_units = convert_to(vhuge_size_amt,vhuge_size_units,"kb")

    # Check that memlock.soft and memlock.hard are equal ( they should have originally been set that way )
    if vmemlock_hard != vmemlock_soft:
        msgg "The hard memlock %s and soft memlock %s are note equal, but should be. Change it here: %s" % (vmemlock_hard,vmemlock_soft,"/proc/meminfo")

    # Use Oracle provided script to get recommended hugepages size for currently running shared memory
    vrecomm_num_hgpgs = number_recommended_hugepages(vhuge_size_amt,vhuge_size_units)

    # Simple compare of recommended total to current HugePage total
    if int(vrecomm_num_hgpgs) > int(vhuge_tot):
        msgg "NOTICE: Recommended HugePages (currently recommended to accomodate running shared memory): [%s] exceeds currently configured total number of HugePages: [%s]" % (vrecom_hgpgs,vhuge_tot)
    else:
        msgg "Current HugePages: [%s] and recommended HugePages based on currently running processes: [%s]" % (vhuge_tot,vrecomm_num_hgpgs)

    # Get summation of sga_target and pga_aggregate_target totals ( summation of all databases on the host )
    # fx returns a dictionary of format: { "sga_totals": sga_totals, "sga_units": "kb", "pga_totals": pga_totals, "pga_units": "kb" }
    # This works in parallel (another size / space check) along with Oracle script that calculates recommended number of HugePages.
    sga_pga_totals = get_pga_sga_totals()

    # Free or unused unallocated (reserved) hugepages: number of Free HugePages - Reserved HugePages ( Reserved HugePages will come out of the Free HugePages )
    # so 'free' hugepages do not reflect true free HugePages unless reserved = 0
    vnum_free_unused_unallocated_hgpgs = int(vhuge_free) - int(vhuge_rsvd)

    # Number of HugePages required for the new database using source db sga
    src_req_hgpgs = int(vsrc_sga_target) / int(huge_size)

    # get total physical memory required by the source database (kb) = sga_target + pga_aggregate_target
    src_phys_mem_req = int(vsrc_sga_target) + int(vsrc_pga_aggregate_target)

    # Check if the source sizes are too big to fit on the new server
    # Check if number of HugePages required for the new database (src_db_req_hg_pgs) is less than truly free HugePages,
    # which is free HugePages minus reserved HugePages (vnum_free_unused_unallocated_hgpgs)
    # Check that sga_target + pga_aggregate_target ( src_phys_mem_req ) of new db is less than free physical memory
    # if so, we're good to go. No further action required.
    # So if either of the blow conditions is true, there's a problem.
    if ( int(src_db_req_hg_pgs) > int(vnum_free_unused_unallocated_hgpgs) ) or ( int(src_phys_mem_req) > int(vnum_free_unall_hgpgs) ):

        # If either of the above is true there's a space problem, see if the defined parameter amounts will fit.
        # Take units off the end of vdest_sga_target being passed in
        # and convert to kb
        if not vdest_sga_target[-1].isdigit():
            vdest_sga_target_amt,vdest_sga_target_unit = split_amt_n_long_units(vdest_sga_target)
            if vdest_sga_target_unit.lower() != "kb":
                vdest_sga_target_amt,vdest_sga_target_unit = convert_to(vdest_sga_target_amt,vdest_sga_target_unit,"kb")

        # Take units off the end of vdest_pga_aggregate_target
        # and convert to kb
        if not vdest_pga_aggregate_target[-1].isdigit():
            vdest_pga_aggregate_target_amt,vdest_pga_aggregate_target_unit = set_long_unit(vdest_pga_aggregate_target)
            if vdest_pga_aggregate_target_unit.lower() != "kb":
                vdest_pga_aggregate_target_amt,vdest_pga_aggregate_target_unit = convert_to(vdest_pga_aggregate_target_amt,vdest_pga_aggregate_target_unit,"kb")

        # check parameter passed sizes against module default sizes and server space.



    # Number of pages required for whats already running:
    running_req_ = l_sga_pga_tot['sga_totals']

    # Check summation of all sga_targets against memlock.
    sum_of_all_sga = int(l_sga_pga_tot['sga_totals']) + int(vsrc_sga_target)
    # meminfo.hard/soft should be equal to or greater than sum of all sga_targets
    if int(sum_of_all_sga) > int(vmemlock_soft):
        msgg "The sum off all sga_target values (%s) is greater than hard and soft memlock."


    # See if there are enough Free, unallocated HugePages to start the new database
    if int(src_db_req_hg_pgs) < int(vnum_free_unall_hgpgs):
        msgg "Current sga_target: %s and pga_aggregate_target: %s settings should work. No change is required."





    # # Free hugepage memory
    # vfree_mem = int(vnum_hugepgs_avail) * int(vhuge_size)

    # number of allocated hugepages ( (total - free = used ) + reserved = all allocated )
    vhuge_allocated = ( int(vhuge_tot) - int(vhuge_free) ) + int(vhuge_rsvd)
    # all HugePages - allocated HugePages =
    vhuge_remaining = int(vhuge_tot) - int(vhuge_allocated)

    # memory needed for allocated hugepages vhuge_alloc * hugepages_size (kb) = needed mem in kb
    vmem_needed_for_huge_kb = int(vhuge_alloc) * int(vhuge_size)

    #  parameter use_large_pages should be enabled
    if vsrc_use_large_pages.lower() == "false":
        tmp_str = "USE_LARGE_PAGES parameter should be enabled, set to TRUE or ONLY, but is: %s" % (vsrc_use_large_pages)
        msgg(tmp_str)

    # AMM disabled? MEMORY_TARGET and MEMORY_MAX_TARGET should be set to 0
    if int(vsrc_memory_target) != 0 and int(vsrc_memory_max_target) != 0:
        tmp_str = "Automatic Memory Management (AMM) must be disabled. AMM is not compatible with HugePages."
        msgg(tmp_str)

    # memlock (hard,soft) value should be slightly smaller than the amount of RAM installed on the database server.
    if int(vmem_lock_hrd) >= int(vphys_mem):
        tmp_str = "The memlock hard settings should be smaller than the amount of physical memory"

    # get shared memory in use:
    vshared_mem_in_use = shared_mem_in_use()
    vshared_mem_in_use = convert_mem(vshared_mem_in_use,'bytes','kb')

    # SGA_TARGET and PGA_AGGREGATE_TARGET together, should not be more than the available memory.
    vsga_pga_tot = int(vsrc_sga_tgt) + int(vsrc_pga_agg_tots)
    if int(vsga_pga_tot) > int(vphys_mem):
        sga_pga_flag = 1
        msgg("SGA_TARGET and PGA_AGGREGATE_TARGET exceed available memory. Attempting to adjust those values to fit in available memory.")
    else:
        sga_pga_flag = 0
        msgg("SGA_TARGET and PGA_AGGREGATE_TARGET are less than available memory. No adjustment needed.")

    # Calculate the number of hugepages needed vs what’s available on the system for the database
    # sga_target divided by hugepages_size = Number of Hugepages
    db_hugepages_needed = int(vsrc_sga_tgt) / int(vhuge_size)

    # Find remaining HugePages needed on server for all databases
    l_sga_pga_tot = get_pga_sga_totals()

    # Confirm that Automatic Shared Memory Management (ASMM) is being used instead of AMM
    if int(vsrc_sga_tgt) == 0 and sga_max_size == 0:


    if int(num_hugepages_needed) > int(vhuge_free):
        tmp_str = "The number of HugePages needed to replicate the source database settings: sga_target (%s) /  HugePage_size (%s) = %s (Number of HugePages needed) is greater than available free HugePages: %s " % (vsrc_sga_tgt,vhuge_size,num_hugepages_needed,vhuge_free)
        msgg(tmp_str)

    if debugme:
        tmp_str = "vsrc_sga_tgt: %s vsrc_pga_agg_tots: %s vdest_mem_info: %s vpfile_dest: %s vphys_mem: %s " % (vsrc_sga_tgt,vsrc_pga_agg_tots,vdest_mem_info,vpfile,vphys_mem)
        debugg(tmp_str)

    # print json.dumps( ansible_facts_dict )
    module.exit_json( msg=msg, ansible_facts=ansible_facts , changed=vchanged)

# code to execute if this program is called directly
if __name__ == "__main__":
   main()
