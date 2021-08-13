# Cru Security functions: password, vaults etc.
from crupac import config
from crupac import debugg

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

    debugg("\nGlobal pkg_pass()....starting...\nparameters:\n\tdb={}\n\tuser={}\n\tpri_filter={}\n\tsec_filter={}".format(db or "Empty!", user or "Empty!", pri_filter or "Empty!", sec_filter or "Empty!"))

    filter_count = count_filters(db, user, pri_filter, sec_filter)

    if not p_dict:
        unlock_lpass()

        v_loc = get_vault_location()

        debugg("\nGlobal pkg_pass():: \n\tv_loc = {}".format(v_loc))

        # /Users/samk/.pyenv/shims/ansible-vault if needed
        cmd_str = "ansible-vault view {}".format(v_loc)

        if debug_passwords in affirm: debugg("\nGlobal pkg_pass() :: CALLING SUBPROCESS...\n\tcmd_str = {}\n\toutput={}".format(cmd_str, output))
        output = run_cmd(cmd_str)

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
        output = run_cmd(cmd_str)
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
    results = run_cmd(cmd_str)
    debugg("unlock_lpass()  results={}".format(results))
    # if not logged in result should be: "Not logged in." else "Logged in as sam.kohler@cru.org."
    if "Not logged in." == results:
        debugg("Not logged in! Asking for user password.")
        debugg("LastPass is locked.\n Go to iterm and unlock your account using:\n lpass login bob.user@cru.org \nExit this app and try again.")
        return("LOCKED")
    else:
        return("UNLOCKED")
