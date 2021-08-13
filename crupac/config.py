# This file contains:
# Cru Utils Python Global Variables:
import os
# Vault file location
vault_file = ""

# to verify a value is True ([affirm]ative) it must be in this list:
affirm = ['True','TRUE', True, 'true', 'T', 't', 'Yes', 'YES', 'yes', 'y', 'Y']

# Debugging variables:
debugme = False
debug_filename = os.path.expanduser("~/.crutils_debug.log")
debug_log = None

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
