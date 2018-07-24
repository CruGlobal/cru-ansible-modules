def get_field(vstring, fieldnum):
    """
    Simple fuction to return a specific field from a string of items.
    Pass in a string and a field number.
    The function returns that item.
    """
    x = 1
    for i in vstring.split():
      if fieldnum == x:
        return i
      else:
        x += 1


def get_etc_orahome(local_vdb):
    """Return database home as recorded in /etc/oratab"""
    global my_msg

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


def get_node_num():
    """Return current Oracle node number of a host"""
    grid_home
    err_msg
    node_number
    node_name
    msg
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
  """Return the nodes in an Oracle RAC cluster and their names"""
  x = 1 # This counter counts node/line numbers
  tmp = {}
  for vline in vstring.splitlines():
    for token in vline.split():
        tmp.update({'node'+str(x) : token})
        break
    x += 1
  return tmp


def get_gihome():
    """Determine the Grid Home directory using Oracle running processes."""
    grid_home

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
        err_msg = err_msg + "%s, %s, %s %s" % (sys.exc_info()[0], sys.exc_info()[1], err_msg, sys.exc_info()[2])
        raise Exception (err_msg)

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


def tnsnames_loc():
    """Locate tnsnames.ora file being used by this host"""
    try:
       vtns1 = str(commands.getstatusoutput("/bin/cat ~/.bash_profile | grep TNS_ADMIN | cut -d '=' -f 2")[1])
    except:
       err_msg = err_msg + ' Error: tnsnames() - vtns1: (%s)' % (sys.exc_info()[0])
       err_msg = err_msg + "%s, %s, %s %s" % (sys.exc_info()[0], sys.exc_info()[1], err_msg, sys.exc_info()[2])
       raise Exception (err_msg)

    if vtns1:
        # return(str(vtns1) + "/tnsnames.ora")
        return(str(vtns1))
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

    return(ora_home)
