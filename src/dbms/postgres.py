from dbms.dbms_template import DBMSTemplate
import psycopg2
import os, subprocess, socket
import time
import json
from utils.logger import MyLogger
from dotenv import load_dotenv


class PgDBMS(DBMSTemplate):
    """ Instantiate DBMSTemplate to support PostgreSQL DBMS """
    def __init__(self, db, user, password, port, host, restart_cmd, recover_script, knob_info_path=None, log_path='src/dbms/logs'):
        super().__init__(db, user, password, port, host, restart_cmd, recover_script, knob_info_path)
        self.name = "postgres"
        self.log = MyLogger("PgDBMS", log_path, 'INFO').logger
    
    def _connect(self, db=None):
        """ Establish connection to database, return success flag """
        self.failed_times = 0
        if db==None:
            db=self.db
        print(f'Trying to connect to {db} with user {self.user}')
        while True:
            try:            
                self.connection = psycopg2.connect(
                    database = db, user = self.user, 
                    password = self.password, host = "localhost"
                )
                print(f"Success to connect to {db} with user {self.user}")
                return True
            except Exception as e:
                self.failed_times += 1
                print(f'Exception while trying to connect: {e}')
                if self.failed_times >= 4:
                    self.recover_dbms()
                    return False
                print("Reconnet again")
                time.sleep(3)
            
    def _disconnect(self):
        """ Disconnect from database. """
        if self.connection:
            print('Disconnecting ...')
            self.connection.close()
            print('Disconnecting done ...')
            self.connection = None

    def copy_db(self, target_db, source_db):
        # for tpcc, recover the data for the target db(benchbase)
        self.update_dbms(f'drop database if exists {target_db}')
        print('Dropped old database')
        self.update_dbms(f'create database {target_db} with template {source_db}')
        print('Initialized new database')
    
    def reset_log_config(self, log_path):
        self.log = MyLogger("PgDBMS", log_path, 'INFO').logger

    def check_template(self, template):
        sql = f"""
        SELECT EXISTS (
            SELECT 1
            FROM pg_database
            WHERE datname = '{template}'
        );
        """

        try:
            self._connect("postgres")
            cursor = self.connection.cursor()
            cursor.execute(sql)
            exists = cursor.fetchone()[0]
            cursor.close()
            self._disconnect()
            self._connect()
            if exists:
                print(f"Template `{template}` exists")
            else:
                print(f"Template `{template}` not exists")
            return exists

        except Exception as e:
            print("Error:", e)
            return False

    def reset_config(self):
        """ Reset all parameters to default values. """
        print("call reset_config()")
        if self.connection is None:
            self._connect()
        self.update_dbms('alter system reset all;')
        
    def reconfigure(self):
        """
        Restart to make parameter settings take effect.
        Handles PG startup by waiting until ready before reconnecting.
        """
        print("call reconfigure()")
        self._disconnect()
        os.system(self.restart_cmd)
        time.sleep(3)
        success = self._connect()
        return success
    
    def get_sql_result(self, sql):
        """ Execute sql query on dbms and return the result and its description """
        # self.connection.autocommit = True
        cursor = self.connection.cursor()
        cursor.execute(sql)
        result = cursor.fetchall()
        description = cursor.description
        self.connection.commit()
        cursor.close()
        return result, description
    
    def extract_knob_info(self, dest_path):
        """ execute "pg_settings" sql on dbms for knob information and store the query result in json format """
        knob_info = {}
        knobs_sql = "SELECT name FROM pg_settings;"
        knobs, _ = self.get_sql_result(knobs_sql)
        for knob in knobs:
            knob = knob[0]  # Extract the knob name from the result tuple
            knob_details_sql = f"SELECT * FROM pg_settings WHERE name = '{knob}';"
            knob_detail, description = self.get_sql_result(knob_details_sql)
            if knob_detail:
                column_names = [desc[0] for desc in description]
                knob_detail = knob_detail[0]
                knob_attributes = {}
                for i, column_name in enumerate(column_names):
                    knob_attributes[column_name] = knob_detail[i]
                knob_info[knob] = knob_attributes
        with open(dest_path, "w") as json_file:
            json.dump(knob_info, json_file, indent=4, sort_keys=True)
        print(f"The knob info is written to {dest_path}")

    def update_dbms(self, sql):
        """ Execute sql query on dbms to update knob value and return success flag """
        self.connection.autocommit = True
        cursor = self.connection.cursor()
        try:
            cursor.execute(sql)
            return True
        except Exception as e:
            self.log.error(f"Failed to execute \"{sql}\" to update dbms for error: {e}")
            return False 
        finally:
            cursor.close()
            

    def set_knob(self, knob, knob_value):
        """
        set value to knob
        if set successfully, return the `knob_value`,
        When set failed, if run `show {knob}` failed, meaning the knob is hallucinated, return 'HALLU' else return the acutal value
        """
        query_one = f'alter system set {knob} to \'{knob_value}\';'
        success =  self.update_dbms(query_one)
        if success:
            return knob_value
        else:
            # get the actual number the config uses
            cursor = self.connection.cursor()
            try:
                cursor.execute(f"SHOW {knob};")
            except Exception as e:
                self.log.error(f"Failed to execute \"SHOW {knob}\" to update dbms for error: {e}")
                cursor.close()
                return "HALLU"
            actual_value = cursor.fetchone()[0]
            self.log.info(f"Knob {knob} is set to {actual_value}")
            cursor.close()
            return actual_value
    
    def get_knob_value(self, knob):
        """ Get the current value for a knob """
        result, _ = self.get_sql_result(f"show {knob}")
        return result[0][0]
        
    def check_knob_exists(self, knob):
        cursor = self.connection.cursor()
        cursor.execute("SELECT * FROM pg_settings WHERE name = %s", (knob,))
        row = cursor.fetchone()
        cursor.close()
        return row is not None

    def exec_quries(self, sql):
        """ Executes all SQL queries in given file and returns success flag. """
        try:
            self.connection.autocommit = True
            cursor = self.connection.cursor()
            sql_statements = sql.split(';')
            for statement in sql_statements:
                if statement.strip():
                    cursor.execute(statement)
            # cursor.execute(sql)
            cursor.close()
            return True
        except Exception as e:
            print(f'Exception execution {sql}: {e}')
        return False
    
    def all_params(self):
        """ Return names of all tuning parameters. """
        cursor = self.connection.cursor()
        cursor.execute("select name from pg_settings " \
                       "where vartype in ('bool', 'integer', 'real')")
        var_vals = cursor.fetchall()
        cursor.close()
        return [v[0] for v in var_vals]
    
    