# This file contains:
# Cru Utils Python Global Variables:
import os
import subprocess
import sys
import os
# import json
# import re                           # regular expression
import yaml
# import fnmatch
# import time
# import datetime
# import inspect
# import glob
# import ast
# import threading
# import socket
from subprocess import PIPE
from pathlib import Path

try:
    import cx_Oracle
    cx_Oracle_found = True
except ImportError:
    cx_Oracle_found = False
    print("unable to import cx_Oracle")

# Vault file location
vault_file = ""

# to verify a value is True ([affirm]ative) it must be in this list:
affirm = ['True','TRUE', True, 'true', 'T', 't', 'Yes', 'YES', 'yes', 'y', 'Y']

# Debugging variables:
debugme = True
debug_filename = ".crutils_debug.log"
debug_log = os.path.expanduser("~/.mod_debug.log")

# dbas dictionary: this is used by cx.py to make database connections with
# minimal information: { db, host} it uses whoami to find the dba user and map
# to the proper db user without having to input it.
dbas = {
    "samk": "skohler"
}

# ignore errors and don't fail the play? If ignore_errors is not provided this
# is the default. This will cause the module to fail the play if the module fails.
default_ignore = False

# utils settings location
utils_settings_file = os.path.expanduser("~/.utils")

# Cru domains
cru_domain = ".ccci.org"
dr_domain = ".dr.cru.org"

# List of commands that should be flagged and kept from being used without approval
# and the exemption list.
nasty_list = [
            "truncate",
            "drop"
            "drop database"
            ]
exemption_list = [ "drop restore point" ]

p_dict = None

msg = ""


def get_utils_setting(get_param):
    """ Given a parameter from ~/.utils file return its value
        valid parameter settings are:
            ans_dir         ans_vault       pass_files
            ans_aws_dir     ssh_user        log_dir
            exe_dir         resend_vault    ora_client
            symlinknag      debug

        Note:
        returns only the value of the setting
    """
    global debug_log
    global utils_settings_file
    debugg("Global :: get_utils_setting()....starting....get_param={} sfk".format(get_param or "None passed!"))
    if not debug_log:
        set_debug_log()

    cmd_str = "cat {} | grep {}".format(utils_settings_file, get_param)
    debugg("cmd_str = {}".format(cmd_str))
    output = run_local(cmd_str)
    debugg("Global :: get_utils_setting() ... output={}".format(output))
    if output and get_param in output:
        tmp = output.split('=')[1]
        return(tmp)

    return(None)


def debugg(dbug_msg):
    """
    Append this dbug_msg string to the debug.log file.
    """
    global debug_log
    global debugme
    global affirm

    if debugme not in affirm:
        return()

    if not debug_log:
        set_debug_log()
        if not debug_log:
            add_to_msg("Error setting debug log. No debugging will be available.")
        return()

    try:
        with open(debug_log, 'a') as f:
            f.write(dbug_msg + "\n")
    except:
        pass

    return()


def israc(host_str=None):
    """
    Determine if a host is running RAC or Single Instance
    """
    global err_msg
    global cru_domain
    global dr_domain

    if host_str is None:
        return()

    if "org" in host_str:
        host_str = host_str.replace(cru_domain,"")
        host_str = host_str.replace(dr_domain, "")

    if "dr" in host_str or "dw" :
        return(False)

    # if the last digits is 1 or 2 ( something less than 10) and not 0 (60) return True
    if host_str[-1:].isdigit() and int(host_str[-1:]) < 10 and int(host_str[-1:]) != 0:
        return(True)
    else:
        return(False)


def add_to_msg(a_msg):
    """Add the arguement to the msg to be passed out"""
    global msg

    if msg:
        msg = msg + " " + a_msg
    else:
        msg = a_msg


def set_debug_log():
    """
    Set the debug_log value to write debugging messages
    Debugging will go to: cru-ansible-oracle/bin/.utils/debug.log
    """
    global utils_settings_file
    global debug_log
    global debug_filename
    global debugme

    if debug_log:
        # already set
        return()

    if not utils_settings_file:
        return()

    cmd_str = "cat $HOME/.utils | grep ans_dir"
    tmp = run_local(cmd_str)
    # output = tmp.decode("utf-8")

    try:
        tmp_path = output.strip().split("=")[1]
        debug_log = tmp_path + "/bin/.utils/{}".format(debug_filename)
    except:
        print("ans_dir not set in ~/.utils unable to write debug info to file")
        pass
    return()


class cx(object):
    """
    to instantiate this class pass a Python dictionary in the follow format:
    {
    "sid" : fscmp1
    "host" : plrac1.ccci.org
    "port" : 1521
    "user" : system
    "password" : supersecret
    "cx_mode" : cx_Oracle.SYSDBA * optional-if user=sys mode = sysdba
    }

    minimal required for a connection:
        sid, host, user, password
    note: since its a dictionary order doesn't matter.
    """

    def __init__(self, info={}):
        self.host = info.get('host',"")
        self.sid = info.get('db',"")
        self.port = info.get('port', 1521)
        self.user = info.get('user', "")
        self.password = info.get('password', "")
        self.service_name = info.get('service_name',"")
        self.cx_mode = info.get('cx_mode', "")
        self.__status = "None"
        self.__dsn = None
        self.__cur = None
        self.init()

    def recon(self, item):
        """
        reconnect to a new db
        exepecting input parameter as Python dictionary:
        minimally required:
        { "sid": fscmp1, "host": plrac1.ccci.org }
        * this will use whoami results to map to db user id using variable dbas
          set in config.py
        * or use format shown in class header
        """
        t_user = item.get('user',"")
        if not t_user:
            results = run_local("whoami")

            self.user = dbas.get(results)

    def show_db(self):
        """
        if we forget what db this object is connected to ask here
        """
        return(self.sid)

    def show_status(self):
        """
        return connection status
        """
        return(self.__status)

    def init(self):
        """
        If a dictionary containing all needed information was passed in
        create a cx_Oracle connection.
        """
        debugg("class cx :: init()...starting....")
        if self.user.lower() == "sys" and not self.cx_mode:
            self.cx_mode = "cx_Oracle.SYSDBA"

        # if all the values are needed to make a connection, make one.
        if self.host and self.sid and self.port and self.user and self.password:
            results = self.dsn_tns()
            debugg("class cx :: init()...dsn_tns returned success.")
            if results == "success":
                self.connect()

    def dsn_tns(self):
        """
        uses three values:
            p1 = self.host, fully qualified i.e. ploradr.dr.cru.org
            p2 = database self.sid fscmp1
            p3 = self.port, default 1521
        create a dsn_tns connect string for cx_Oracle and return it
        """
        debugg("class cx :: dsn_tns()...starting....")
        try:
            if self.host and self.port and self.sid:
                self.__dsn = cx_Oracle.makedsn(self.host, self.port, self.sid)
            elif self.service_name:
                self.__dsn = cx_Oracle.makedsn(host=self.host, port=self.port, service_name=self.service_name)
        except cx_Oracle.DatabaseError as exc:
            error_msg, = exc.args
            debugg("Error: creating dsn_tns {}".format(error_msg))
            self.__status = "dsn_tns failed"
            return("error")

        self.__con_status = "dsn_tns"
        return("success")

    def connect(self):
        """
        Uses five values:
            self.sid = database sid
            self.user = user id used to connect to the Oracle database
            self.password = password for the user
            dns = dsn_tns
            cx_mode = connection mode "normal" or "sysdba" * optional
        create a connection and cursor and pass back the cursor.
        ** if userid = sys then sysdba is assumed. Therefore con_mode is optional.
        mode examples and types:
            mode = {
            'sysdba': cx_Oracle.SYSDBA,
            'sysasm': cx_Oracle.SYSASM,
            'sysoper': cx_Oracle.SYSOPER,
            'sysbkp': cx_Oracle.SYSBKP,
            'sysdgd': cx_Oracle.SYSDGD,
            'syskmt': cx_Oracle.SYSKMT,
             }
        """
        debugg("crutils :: cx :: create_con()...starting....p1={} p2={} p3={}".format(self.user or "Empty!", self.password or "Empty!", self.__dsn or "Empty!"))
        con = None

        try:
            if self.user.lower() == "sys" or ( "cx_Oracle" in self.cx_mode):
                con = cx_Oracle.connect(dsn=self.dns, user=self.user, password=self.password, mode=cx_Oracle.SYSDBA)
            else:
                con = cx_Oracle.connect(self.user, self.password, self.__dsn)
        except cx_Oracle.DatabaseError as exc:
            error_msg, = exc.args
            debugg("class cx error creating connection error {}".format(error_msg))
            self.__status = "connection failed"
            return("fail")

        self.__cur = con.cursor()
        self.__status = "connected"
        return("success")

    def run(self, cmd_str, expect_results):
        """
        Once a connection is made,
        pass a cmd_str in here to execute it against the connection
        """
        global affirm
        debugg("class cx :: exe() cmd_str={} expect_results={}".format(cmd_str or "Empty!", expect_results or "Empty!"))

        if self.__status != "connected":
            return("Unable to execute command. Connection status is {}".format(self.__status or "Unknown"))

        self.__cur.execute(cmd_str)
        if expect_results in affirm:
            vtemp = self.__cur.fetchall()
            output = vtemp[0][0]
            return(output)
        else:
            return()

    def close(self):
        try:
            self.__cur.close()
        except:
            pass


def run_local(cmd_str):
    """
    Run a command on the local host using the subprocess module.
    """

    try:
        process = subprocess.Popen([cmd_str], stdout=PIPE, stderr=PIPE, shell=True)
        output, code = process.communicate()
    except subprocess.CalledProcessError as e:
        err_msg = "common::run_command() : [ERROR]: output = %s, error code = %s\n".format(e.output, e.returncode)
        if debug_filename:
            debugg("Error running global run_local(). cmd_str={} Error: {}".format(cmd_str,err_msg))

    results = output.decode('ascii').strip()

    return(results)


def run_remote(cmd_str, vhost):
    """ Run a command: cmd_str on the remote host
    """
    global cru_domain
    global dr_domain
    global affirm
    global abbr_host_domain_hash
    debug_local = True
    debugg("global run_remote_cmd() ::....starting....vhost={} cmd_str={}".format(vhost or "Empty!", cmd_str or "Empty!"),"run_remote_cmd")

    if sam():
        sshUser = whichsam(vhost)
    else:
        sshUser = whoami()
    debugg("global run_remote_cmd() :: sshUser={}".format(sshUser or "None!"))
    try:
        debugg("if cru_domain [{cd}] not in vhost [{vh}] or dr_domain [{dd}] not in vhost[{vh}]:".format(cd=cru_domain, vh=vhost, dd=dr_domain))
        if cru_domain not in vhost and dr_domain not in vhost:
            debugg("global run_remote_cmd() :: Checkpoint #1" )
            add_this_domain = abbr_host_domain_hash[vhost]
            tgtHost = vhost + add_this_domain
        else:
            debugg("global run_remote_cmd() :: Checkpoint #2")
            tgtHost = vhost

        debugg("global run_remote_cmd() :: cmd_str={} tgtHost={} ssh_user={}".format(cmd_str or "Empty!", tgtHost or "Empty!", sshUser or "Empty!"))
        if debug_local in affirm: debugg("global run_remote_cmd() :: sshUser = {} tgtHost = {} cmd_str = {}".format(sshUser, tgtHost, cmd_str))
        output = subprocess.run(["ssh", sshUser + "@" + tgtHost, cmd_str], shell=False, stdout=subprocess.PIPE, stderr=subprocess.PIPE, encoding='utf8')
    except:
        err_msg = 'global run_remote_cmd() :: Error: {} cmd_str={}'.format(str(sys.exc_info()[0]), cmd_str)
        err_msg = err_msg + " Meta:: {}, {}, {} {}".format(str(sys.exc_info()[0]), str(sys.exc_info()[1]), err_msg, str(sys.exc_info()[2]))
        debugg("Error[israc] {}".format(err_msg))
        if debug_local in affirm: debugg("{}".format(err_msg))
        return

    debugg("global run_remote_cmd() :: output={}".format(str(output)))
    if debug_local in affirm: debugg("run_remote_cmd()...exiting...output = {}".format(str(output)))
    return(output.stdout) # .decode('ascii').strip())


def isipaddr(host):
    """Determine if a host is actually an IP address"""
    debugg("isipaddr()...starting....")
    try:
        socket.inet_aton(host)
        return(True)
    except socket.error:
        return(False)


def write_to_file(info, f):
    """
    write info to file ( f )
        returns(0) if successful
        returns(1) if failure
    """
    try:
        with open(f, 'w') as log:
            log.write(str(info) + "\n")
    except:
        return(1)

    return(0)


def app_to_file(info, a_file):
    """
    append info to file ( f )
        returns(0) if successful
        returns(1) if failure
    """
    try:
        with open(a_file, 'a') as f:
            f.write(str(info) + "\n")
    except:
        return(1)

    return(0)


def pkg_pass(db, user, pri_filter=None, sec_filter=None):
    """
        ====================================
        Given a vault name ( v_name ):
            aws      or  aws_vault.yml
            tower    or  tower_vault.yml
            mysql    or  mysql_vault.yml
            postgres or  postgres_vault.yml
        and a primary filter ( i.e. f1 database name )
        and a secondary filter ( i.e. f2 user name )
        example:
            v_name: aws_vault.yml or just aws
            primary filter f1: [ database_passwords, asm_passwords]
                secondary filter f2: [ dbname ]
                    teritary_filter f3: [ user name ]
        or
        just a primary filter:
        example:
            vault: postgres_vault.yml
            f1: [ ploemomr01_pdb_admin_pass, scp_user, temppass, osb_password, datadog_oracle_password, datadog_api_key/datadog_app_key ]
        return the password from an AWS ansible-vault

    Attempting new method to retrieve ansible vault passwords when this code is packaged.

    """
    debug_passwords = False
    global affirm
    global p_dict
    new_yml_str = ""
    if not pri_filter:
        pri_filter = "database_passwords"

    if user:
        user = user.lower()

    if "dr" in db:
        db = db.replace("dr","")

    debugg("\nGlobal pkg_pass()....starting...\nparameters:\n\tdb={}\n\tuser={}\n\tpri_filter={}\n\tsec_filter={}".format(db or "Empty!", user or "Empty!", pri_filter or "Empty!", sec_filter or "Empty!"))

    filter_count = count_filters(db, user, pri_filter, sec_filter)

    if not p_dict:
        unlock_lpass()

        v_loc = get_vault_location()

        debugg("\nGlobal pkg_pass():: \n\tv_loc = {}".format(v_loc))

        # /Users/samk/.pyenv/shims/ansible-vault if needed
        cmd_str = "ansible-vault view {}".format(v_loc)

        if debug_passwords in affirm: debugg("\nGlobal pkg_pass() :: CALLING SUBPROCESS...\n\tcmd_str = {}\n\toutput={}".format(cmd_str, output))
        output = run_local(cmd_str)

        if debug_passwords in affirm: debugg("\nGlobal pkg_pass():: ...after communicate() ... \n\touput = {} ".format(output or "Empty!") ) #), code or "Empty!"))

        for item in output.split("\n"):
            if debug_passwords in affirm: debugg("for loop() === line={}".format(item))
            if item[:1] == "#" in ['#','---']:
                continue
            else:
                new_yml_str = new_yml_str + item + "\n"

        if debug_passwords in affirm: debugg("\nGlobal :: pkg_pass() :: \n\tcleaned up dictionary => new_dict={}".format(str(new_yml_str)) )

        # good_dict = find_this_item(output)
        pwd_dict = yaml.safe_load( new_yml_str )
        if debug_passwords in affirm: debugg("\nGlobal :: pkg_pass() :: \n\tfilter_count = {} after yaml.safe_load(pwd_dict) => {}".format(
            filter_count or "Empty!", str(pwd_dict)))
        p_dict = pwd_dict
    else:
        if debug_passwords in affirm: debugg("\nGlobal :: pkg_pass() :: p_dict already populated {}".format(str(p_dict)))
        pwd_dict = p_dict

    if debug_passwords in affirm: debugg("\nGlobal :: pkg_pass() :: filter_count = {} attempting password retrieval....from {}".format(filter_count or "Empty!", str(pwd_dict)))
    try:
        if filter_count == 1:
            the_passwd = pwd_dict.get(db, None)
        elif filter_count == 2:
            the_passwd = pwd_dict[pri_filter][db].get(user, None)
        elif filter_count == 3:
            the_passwd = pwd_dict[pri_filter][db].get(user, None)
        elif filter_count == 4:
            the_passwd = pwd_dict[pri_filter][sec_filter][db].get(user, None)
        else:
            return(None)
    except:
        # It could have been a top level password:
        try:
            the_passwd = pwd_dict[db].get(user, None)
        except:
            debugg("\nGlobal :: pkg_pass() :: SECOND ATTEMPT: pwd_dict[{}].get({})\n".format(
                db or "Empty!",
                user or "Empty!"))
            return (None)
        if not the_passwd:
            add_to_msg("Password for {}  {}@{} not found in ansible vault.".format(pri_filter, user or "Empty!", db or "Empty!"))
            debugg("\nGlobal :: pkg_pass() :: password for user={}@db={} not found in ansible vault.\n".format(user or "Empty!",                                                                                              db or "Empty!"))
            return (None)

    debugg("\nGLOBAL :: pkg_pass() :: exiting\n\treturning password = {} for {}@{}\n".format(the_passwd,user,db))
    return(the_passwd)


def get_vault_location():
    """
       Read the ~/.utils settings file and
       retrieve the Anisble vault file location
       another piece of the puzzle needed to unlock the vault to get passwords
    """
    global vault_file
    debugg("\nGlobal :: utils :: get_vault_location() ...starting...\nvault_file={}".format(vault_file or "Empty!"))

    if vault_file:
        return(vault_file)

    utils_settings_file = os.path.expanduser("~/.utils")
    debugg("\nGlobal :: utils :: get_vault_location()\n\tutils_settings_file={}".format(utils_settings_file))
    try:
        cmd_str = "cat {} | grep ans_vault".format(utils_settings_file)
        output = run_local(cmd_str)
    except:
        # print("Error: reading ~/.utils to determine vault file location cmd_str = {}".format(cmd_str))
        debugg("Global :: utils :: Error: reading ~/.utils to determine vault file location cmd_str = {}".format(cmd_str))
        return

    debugg("\nGlobal :: utils :: get_vault_location()\n\toutput={}".format(output))

    vault_file = output.split("=")[1].strip()  # output.decode('utf-8').split("=")[1]
    debugg("\nGlobal :: utils :: get_vault_location()...exiting...\n\treturn={}".format(vault_file))
    if not os.path.isfile(vault_file):
        debugg("\nVault location defined\nHowever, file does not exist!\n{}".format(vault_file or "Empty!"))
        return(None)
    else:
        debugg("\nVault location defined\nreturning vault_file={}\n".format(vault_file))
        return(vault_file)


def count_filters(f1=None, f2=None, f3=None, f4=None):
    """ Called by "get_cloud_passwd() to count the number
        of filters passed
    """
    debugg("\nGlobal :: count_filters() :: ...starting....\nwith paramters:\n\tf1={}\n\tf2={}\n\tf3={}\n\tf4={}".format(f1 or "None", f2 or "None", f3 or "None", f4 or "None"))
    args = locals()
    count = 0
    for k, v in args.items():
        if v is not None:
            count += 1

    return(count)


def unlock_lpass():
    '''
    Passwords have moved to lpass, check status and unlock before trying to retrieve passwords.
    '''

    debugg("unlock_lpass().....starting......")
    cmd_str="lpass status"
    results = run_local(cmd_str)
    debugg("unlock_lpass()  results={}".format(results))
    # if not logged in result should be: "Not logged in." else "Logged in as sam.kohler@cru.org."
    if "Not logged in." == results:
        debugg("Not logged in! Asking for user password.")
        debugg("LastPass is locked.\n Go to iterm and unlock your account using:\n lpass login bob.user@cru.org \nExit this app and try again.")
        return("LOCKED")
    else:
        return("UNLOCKED")


def prep_sid(dbName, host):
    """
    Given a database name and a host name create and return an Oracle SID
    """
    global cru_domain
    global debug_log

    if not debug_log:
        set_debug_log()

    debugg("crumods :: prep_sid()....starting.....dbName={} host={}".format(dbName, host))


    if not dbName or not host:
        msg = "prep_sid() :: Error one or both required parameters missing: database: {} and host: {}".format(dbName or "No database passed!", host or "No host passed!")
        print(msg)
        debugg(msg)

    if cru_domain in host or dr_domain in host:
        host = host.replace(cru_domain, "").replace(dr_domain, "")

    if cru_domain in dbName or dr_domain in dbName:
        dbName = dbName.replace(cru_domain, "").replace(dr_domain, "")

    # If there's already a digit at the end of the db name assume its a SID else
    if dbName[-1:].isdigit():
        return(dbName)
    else:
        # our RAC's are 1 - 10. DW is 60 so last digit = 0
        if host[-1:].isdigit() and int(host[-1:]) != 0:
            sid = dbName + host[-1:]
        else:
            sid = dbName

    debugg(debug_log, "Global :: prep_sid() :: returning...sid : {}".format(sid))
    return(sid)
