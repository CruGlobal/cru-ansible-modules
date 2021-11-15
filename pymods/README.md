# CruMods

### ( [Cru] Python [Mod]ule[s] )
This is a python file containing some of the most common Python functions used throughout Cru's custom Ansible modules, Utils, a PyQt5 GUI wrapper for Cru's Ansible playbooks
and other funcationality.

|global variables|Type|Purpose|
|:-------------|:------|:----------|
|affirm|list|defines True for comparison in Python since Python doesn't have booleans.|
|debugme|string|Sets debugging mode flag for modules (not fully implemented).|
|debug_filename|string|Name of the debug log file.|
|debug_log|string|path to deebug log file including log file name.|
|dbas   |dictionary|Used by cx class to create Python database connections with minimal information. Maps local iMac user 'samk' to db user 'skohler'
|default_ignore | string/bool | Default value for 'ignore errors' when a module fails if no value passed.<br> ( default is False )   |
|utils_settings_file   |string   |Location of `~/.utils` settings file on local host   |
|cru_domain   | string  |Defines Cru's current domain to use: `.ccci.org`   |
|dr_domain   |string   |Defines Cru's disaster recovery domain: `.dr.cru.org`   |
|nasty_list   |list  |List of commands that should be flagged and kept from being used without approval. i.e. `truncate, drop` <br>Used in conjunction with exemption_list  |
|exemption_list   |list   |Defines exemptions to nasty_list i.e. `drop restore point` is an exemption to `drop` ban.   |
|p_dict   | dictionary  |This is used to load the password file into dictionary variable during lookups.   |


<br />

<style>
        .tab {
            display: inline-block;
            margin-left: 40px;
        }
</style>
Function Name | Purpose
:------------|:-----------------------------------------------------------------
get_utils_setting     | Pass this function a setting to retrieve from `~/.utils` <br/> Valid settings are:<br/><span class="tab"></span>ans_dir &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; ans_vault &nbsp; &nbsp; &nbsp; &nbsp;pass_files<br><span class="tab"></span>ans_aws_dir &nbsp;  ssh_user &nbsp; &nbsp; &nbsp; &nbsp; log_dir<br> <span class="tab"></span>exe_dir &nbsp; &nbsp; &nbsp; &nbsp;  &nbsp; resend_vault &nbsp; ora_client<br><span class="tab"></span>symlinknag &nbsp; &nbsp; debug|
debugg  |Pass a debugging string to this function to write it to the debug log file
israc|Pass a host to this function to determine if the host is part of a RAC cluster
add_to_msg|Pass this function strings to add them to the module output message
set_debug_log  |Function that defines the variables that control where the debug log is written
run_local  |  Run a command on the local host
write_to_file  | Pass a string and an absolute file path and name and it will write the string to that file.
run_remote  | Pass this function a command string and a host and it will run the command on the remote host.
isipaddr  |  determines if the host is a proper IP address or not. Returns `True` or `False`
pkg_pass  | Pass this fx a db name and user name and it will return the Anisble vault password for that user. It assumes the format:<br><span class="tab"></span> database_passwords:<br><span class="tab"></span><span class="tab"></span>db:<br><span class="tab"></span><span class="tab"></span><span class="tab"></span>user<br>If no results are found using that it tries db/user at the root level. Other filtering options can be done passing optional parameters:<br><span class="tab"></span>pri_filter and sec_filter<br>Where pri_filter might be `asm_passwords` vs `database_passwords`
get_vault_location  | Reads `~/.utils` and retrieves vault location.
count_filters  |  Used by get_cloud_passwd() to determine the number of filters passed.
unlock_lpass  |  Checks the status of lpass to ensure its unlocked before running plays etc. Prompts the user to unlock it if it is determined to be locked.
prep_sid  | Pass this fx a db name and host. It will attempt to determine and pass back the db SID

<br>

Class | Purpose
:-----|:-------
cx  |  When instantiated creates a cx-Oracle connection to a database. <br>instantiated by passing a Python dictionary with the following parameters:<br><span class="tab"></span>{<br><span class="tab"></span> "sid" : "fscmp1",<br><span class="tab"></span>"host": "plrac1.ccci.org",<br><span class="tab"></span>"port" : 1521,<br><span class="tab"></span>"user" : "system",<br><span class="tab"></span>"password" : "supersecret",<br><span class="tab"></span> "cx_mode": cx_Oracle.SYSDBA<br><span class="tab"></span>}<br><br> * cx_mode is optional-if using user=sys mode=sysdba is required<br><br>cx functions include:<br>--------------<br><span class="tab"></span>recon - used to reconnect the cx object to a new database using {sid:dbsid, host:hostname.ccci.org}<br><span class="tab"></span><span class="tab"></span>If no user and password are passed in it maps whoami results to an Oracle user using the global<br><span class="tab"></span>variable `dbas` and retrieves your password for the database from lpass<br><span class="tab"></span>show_db - Shows the database you are currently connected to.<br><span class="tab"></span>show_status - Returns connection status.<br><span class="tab"></span>run - pass this fx a command string and whether to expect results from that command: True/False.<br><span class="tab"></span>close -  Close the current cursor.

### To use the functions contained in the module file:

You can add this to the header of a python script. Notice that this example shows the relative path, so the script would have to be located in the cru-ansible-oracle root level directory for this exact example to work.  Otherwise, if the script you're writing is not located in the cru-ansible-oracle root directory, the absolute path to the module would have to be given in this example.
```
sys.path.append(r'./library/pymods/')
from crumods import *
```

It's also possible to add the modules directory to the `PYTHONPATH`
<br>
