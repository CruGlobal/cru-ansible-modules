# to use: import crutils.cx as cx
# to instantiate the cxcon class:
#       a = b = cx.cxcon({"db": "crmd1", "host":"tlrac1.ccci.org", "user": "myuser", "password":"supersecret" })

from crupac.config import *
from crupac.dbug import *
from crupac.utils import *

import os
import sys

try:
    import cx_Oracle
    cx_Oracle_found = True
except ImportError:
    cx_Oracle_found = False
    print("unable to import cx_Oracle")

class odbo(object):
    """
    to instantiate this class: pass a Python dictionary in the follow format:
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
        Given three parameters:
            p1 = host, fully qualified i.e. ploradr.dr.cru.org
            p2 = database sid fscmp1
            p3 = port, default 1521
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

        Given four values:
            db = database user name
            password = password for the user
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
