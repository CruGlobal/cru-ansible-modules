from crupac.config import *

def debugg(dbug_msg):
    """
    Append this dbug_msg string to the debug.log file.
    """
    global debug_log
    global debugme
    global affirm

    if debugme not in affirm:
        return()

    add_to_msg(db_msg)

    if not debug_log:
        set_debug_log()
        if not debug_log:
            add_to_msg("Error setting debug log. No debugging will be available.")
        return()

    try:
        with open(debug_log, 'a') as f:
            f.write(db_msg + "\n")
    except:
        pass

    return()


def set_debug_log():
    """
    Set the debug_log value to write debugging messages
    Debugging will go to: cru-ansible-oracle/bin/.utils/debug.log
    """
    global utils_settings_file
    global debug_log
    global debug_filename
    global debugme

    if not utils_settings_file:
        return()

    cmd_str = "cat $HOME/.utils | grep ans_dir"
    tmp = run_cmd(cmd_str)
    output = tmp.decode("utf-8")

    try:
        tmp_path = output.strip().split("=")[1]
        debug_log = tmp_path + "/bin/.utils/{}".format(debug_filename)
    except:
        print("ans_dir not set in ~/.utils unable to write debug info to file")
        pass
    return()
