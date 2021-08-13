# These are utility fx's
import subprocess
import time
import datetime
import sys
import os
import re
import inspect
import glob
import ast
import threading
import socket
import platform

def run_local(cmd_str):
    """
    Run a command on the local host using the subprocess module.
    """
    global global_debugFile
    df = ""

    if not debugFile:
        if global_debugFile:
            df = global_debugFile
    else:
        df = debugFile

    debugg(df, "run_sub() ...starting... with cmd_str={} from calling fx={}".format(cmd_str, calling_fx or "None"))

    try:
        process = subprocess.Popen([cmd_str], stdout=PIPE, stderr=PIPE, shell=True)
        output, code = process.communicate()
    except subprocess.CalledProcessError as e:
        err_msg = "common::run_command() : [ERROR]: output = %s, error code = %s\n".format(e.output, e.returncode)
        if global_debugFile:
            debugg(df, "Error running global run_sub. cmd_str={} Error: {}".format(cmd_str,err_msg))
        else:
            tellUser("Error running cmd: {} Error info: {}".format(cmd_str,err_msg))

    results = output.decode('ascii').strip()
    debugg(df,"run_sub()...exiting....output={} code={}".format(results, code))

    return(results)


def write_to_file(info, f):
    """ write info to file ( f ) """
    with open(f, 'w') as log:
        log.write(str(info) + "\n")


def run_remote(cmd_str, vhost):
    """ Run a command: cmd_str on the remote host
    """
    global cru_domain
    global dr_domain
    global affirm
    global abbr_host_domain_hash
    debug_local = True
    debugg(None, "global run_remote_cmd() ::....starting....vhost={} cmd_str={}".format(vhost or "Empty!", cmd_str or "Empty!"),"run_remote_cmd")

    if sam():
        sshUser = whichsam(vhost)
    else:
        sshUser = whoami()
    debugg(None, "global run_remote_cmd() :: sshUser={}".format(sshUser or "None!"))
    try:
        debugg(None,"if cru_domain [{cd}] not in vhost [{vh}] or dr_domain [{dd}] not in vhost[{vh}]:".format(cd=cru_domain, vh=vhost, dd=dr_domain))
        if cru_domain not in vhost and dr_domain not in vhost:
            debugg(None,"global run_remote_cmd() :: Checkpoint #1" )
            add_this_domain = abbr_host_domain_hash[vhost]
            tgtHost = vhost + add_this_domain
        else:
            debugg(None, "global run_remote_cmd() :: Checkpoint #2")
            tgtHost = vhost

        debugg(None, "global run_remote_cmd() :: cmd_str={} tgtHost={} ssh_user={}".format(cmd_str or "Empty!", tgtHost or "Empty!", sshUser or "Empty!"))
        if debug_local in affirm: debugg(None, "global run_remote_cmd() :: sshUser = {} tgtHost = {} cmd_str = {}".format(sshUser, tgtHost, cmd_str))
        output = subprocess.run(["ssh", sshUser + "@" + tgtHost, cmd_str], shell=False, stdout=subprocess.PIPE, stderr=subprocess.PIPE, encoding='utf8')
    except:
        err_msg = 'global run_remote_cmd() :: Error: {} cmd_str={}'.format(str(sys.exc_info()[0]), cmd_str)
        err_msg = err_msg + " Meta:: {}, {}, {} {}".format(str(sys.exc_info()[0]), str(sys.exc_info()[1]), err_msg, str(sys.exc_info()[2]))
        debugg(None, "Error[israc] {}".format(err_msg))
        if debug_local in affirm: debugg(None, "{}".format(err_msg))
        return

    debugg(None, "global run_remote_cmd() :: output={}".format(str(output)))
    if debug_local in affirm: debugg(None, "run_remote_cmd()...exiting...output = {}".format(str(output)))
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
    """ write info to file ( f ) """
    with open(f, 'w') as log:
        log.write(str(info) + "\n")
