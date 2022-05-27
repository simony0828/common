import yaml
import sys
import time
import datetime
import os
from jinja2 import Template
from yamlinclude import YamlIncludeConstructor
from .executor_presto import ExecutorForPresto
from .watcher import Watcher
from .detector import Detector

class Executor:
    def __init__(self, yaml_file, run_setup=False, steps=None, is_dry_run=False, is_unit_test=False, variables=[]):
        # This unique key is to identify all the ETL steps for each run
        self.etl_run_key = int(time.time())

        if is_dry_run and is_unit_test:
            raise Exception("Cannot set both DRY_RUN and UNIT_TEST")

        # Run setup/create tables only
        self.run_setup = run_setup
        # Run specific steps from user
        if steps is not None:
            self.steps = steps.split(',')
        else:
            self.steps = []
        # Print SQLs only if enabled
        self.is_dry_run = is_dry_run
        # Do not insert to meta table if enabled
        self.is_unit_test = is_unit_test

        self.variables = variables
        self.database_type = None
        self.database_catalog = None
        self.target_table = None
        self.target_columns = []
        self.primary_keys = None
        self.partition_keys = None
        self.source_tables = []
        self.staging_tables = {}
        self.tmp_tables = {}
        self.etl = {}
        self.config_data = {}

        # Absolute directory path to the yaml config file
        # (any DQ/watcher file associate this yaml file would be in the same location) 
        self.yaml_path = os.path.dirname(os.path.abspath(yaml_file))
        self.dq_file = None
        self.watcher_file = None
        self.etl_file = os.path.abspath(yaml_file)

        self.__setup_config(yaml_file)

    def __read_config(self, config_file):
        ''' For reading and converting YAML file '''
        file_path = os.path.dirname(os.path.abspath(config_file))
        YamlIncludeConstructor.add_to_loader_class(loader_class=yaml.FullLoader, base_dir=file_path)
        with open(config_file) as f:
            return yaml.load(f, Loader=yaml.FullLoader)

    def __setup_config(self, config_file):
        ''' Parse the YAML file to global variables '''
        self.config_data = self.__read_config(config_file)
        #print(self.config_data)

        if 'database' not in self.config_data:
            raise Exception("Missing database!")

        if 'type' not in self.config_data['database']:
            raise Exception("Missing database type!")

        self.database_type = self.config_data['database']['type'].lower()

        if 'catalog' in self.config_data['database']:
            self.database_catalog = self.config_data['database']['catalog']

        if 'target_table' not in self.config_data:
            raise Exception("Missing target_table")
        else:
            self.target_table = self.config_data['target_table']['name']
            self.target_columns = self.config_data['target_table']['columns']

        if 'primary_key' in self.config_data['target_table']:
            self.primary_keys = ','.join(self.config_data['target_table']['primary_key'])

        if 'partition_key' in self.config_data['target_table']:
            self.partition_keys = ','.join(self.config_data['target_table']['partition_key'])

        if 'source_tables' in self.config_data:
            self.source_tables = self.config_data['source_tables']['name']

        if 'staging_tables' in self.config_data:
            for i, t in enumerate(self.config_data['staging_tables']):
                table = t['name']
                table_name = table.split('.')[-1]
                columns = t['columns']
                self.staging_tables[table_name] = {}
                self.staging_tables[table_name]['columns'] = columns
                self.staging_tables[table_name]['table'] = table

        if 'wait_for' in self.config_data:
            watcher_filename = self.config_data['wait_for']['file']
            self.watcher_file = self.yaml_path + "/" + watcher_filename

        if 'etl' not in self.config_data:
            raise Exception("Missing etl")
        else:
            self.etl = self.config_data['etl']
            for e in self.etl:
                # Control what steps to run if user specific
                if len(self.steps) > 0:
                    self.etl[e]['enabled'] = (e in self.steps)
                else:
                # Otherwise default to config or True if not set
                    self.etl[e]['enabled'] = self.etl[e].get('enabled', True)

        if 'dq' in self.config_data:
            dq_filename = self.config_data['dq']['file']
            self.dq_file = self.yaml_path + "/" + dq_filename

    def __replace_variables(self, str):
        ''' To replace any string with the variables list '''
        # Replace str for staging table names
        #for staging_table in self.staging_tables:
        #    staging = self.staging_tables[staging_table]['table']
        #    str = str.replace("<{t}>".format(t=staging_table), staging)

        # Replace str for tmp table names
        #for tmp_table in self.tmp_tables:
        #    tmp = self.tmp_tables[tmp_table]['table']
        #    str = str.replace("<{t}>".format(t=tmp_table), tmp)

        # Use jinga2 Template to replace table reference names
        tmp_dict = {}
        for staging_table in self.staging_tables:
            tmp_dict[staging_table] = self.staging_tables[staging_table]['table']
        for tmp_table in self.tmp_tables:
            tmp_dict[tmp_table] = self.tmp_tables[tmp_table]['table']
        tm = Template(str)
        str = tm.render(tmp_dict)

        # Replace str for from command line variables list
        for variable in self.variables:
            var_name = variable.split('=')[0]
            var_value = variable.split('=')[1]
            str = str.replace(":{vn}".format(vn=var_name), var_value)

        return str

    def __execute_all_etl(self, exe):
        ''' Internal function to run all ETL steps for different databases '''
        if self.run_setup:
            # Generate target create table statement
            target_create_sql = exe.gen_create_table(self.target_table, self.target_columns, self.partition_keys)
            target_create_sql = self.__replace_variables(target_create_sql)

            # Generat staging tables create statement
            staging_create_sqls = []
            for staging_table in self.staging_tables:
                staging = self.staging_tables[staging_table]['table']
                staging_create_sql = exe.gen_create_table(staging, self.staging_tables[staging_table]['columns'])
                staging_create_sqls.append(self.__replace_variables(staging_create_sql))

            # Execute all statements
            exe.execute_sql(target_create_sql)
            exe.execute_sqls(staging_create_sqls)
            sys.exit(0)
        else:
            for etl_name in self.etl:
                if self.etl[etl_name]['enabled']:
                    sql = None
                    print(">= {n} =<".format(n=etl_name))
                    if etl_name.startswith("tmp"):
                        etl_sql = self.etl[etl_name]['sql']
                        self.tmp_tables[etl_name] = {}
                        self.tmp_tables[etl_name]['table'] = etl_name
                        # For Presto, we need to define temporary schema
                        if self.database_type == 'presto':
                            unique_key = datetime.datetime.now().strftime('%Y%m%d%H%M%S')
                            self.tmp_tables[etl_name]['table'] = "{c}.dps_presto_etl_poc_dm_csi_pii.{t}__{k}".format(
                                c=self.database_catalog, t=etl_name, k=unique_key)
                        sql = exe.gen_etl_tmp(self.tmp_tables[etl_name]['table'], etl_sql)
                    elif etl_name.startswith("stg"):
                        etl_sql = self.etl[etl_name]['sql']
                        etl_table = self.staging_tables[etl_name]['table']
                        delete_clause = None
                        if 'delete' in self.etl[etl_name]:
                            delete_clause = self.etl[etl_name]['delete']
                        sql = exe.gen_etl_stg(etl_table, etl_sql, delete_clause)
                    elif etl_name == 'target_table':
                        # Create a target column list for target upsert/append
                        target_column_list = ""
                        for column in self.target_columns:
                            for key, value in column.items():
                                target_column_list += key + ','
                        target_column_list = target_column_list.rstrip(',')
                        delete_clause = None
                        if 'delete' in self.etl[etl_name]:
                            delete_clause = self.etl[etl_name]['delete']
                        if 'from' not in self.etl[etl_name]:
                            raise Exception("Target Table requires FROM")
                        from_table = str(self.etl[etl_name]['from'])
                        if 'mode' not in self.etl[etl_name]:
                            raise Exception("Target Table mode is not set")
                        else:
                            mode = self.etl[etl_name]['mode'].lower()
                            if mode == 'overwrite':
                                print("*** OVERWRITE ***")
                                sql = exe.gen_etl_tgt_overwrite(self.target_table, from_table, target_column_list)
                            elif mode == 'update':
                                print("*** UPDATE ***")
                                if self.primary_keys is None:
                                    raise Exception("UPDATE mode requires PRIMARY_KEY to be set")
                                sql = exe.gen_etl_tgt_update(self.target_table, from_table, target_column_list, self.primary_keys)
                            elif mode == 'append':
                                print("*** APPEND ***")
                                if 'delete_clause' in self.etl[etl_name]:
                                    delete_clause = self.etl[etl_name]['delete']
                                sql = exe.gen_etl_tgt_append(self.target_table, from_table, target_column_list, delete_clause)
                            else:
                                raise Exception("Unknown MODE for {e}".format(e=etl_name))
                        # For Presto, we need to drop temporary tables
                        if self.database_type == 'presto':
                            sql += exe.gen_drop_tmp_tables(self.tmp_tables)
                    else:
                        raise Exception("Unknown ETL name")
                    sql = self.__replace_variables(sql)
                    print('[' + datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S") + '] Running')
                    exe.execute_sql(sql)
                    print('[' + datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S") + '] Done')
                else:
                    print("*** {e} IS SKIPPED ***".format(e=etl_name))

    def run_etl(self):
        ''' Main function to generate and execute the ETL '''
        # If watcher file is set, run Watcher
        if self.watcher_file is not None and len(self.steps) == 0:
            print('>= Execute Watcher file: {f} =<'.format(f=self.dq_file))
            w = Watcher(
                yaml_file=self.watcher_file,
                is_dry_run=self.is_dry_run,
                is_unit_test=self.is_unit_test,
                variables=self.variables,
                )
            w.run_watcher()

        # ETL execution step
        print('>= Execute ETL file: {f} =<'.format(f=self.etl_file))
        if self.database_type == 'presto':
            if self.database_catalog is None:
                raise Exception("Database catalog is required for Presto!")
            # Update target_table with catalog
            self.target_table = "{c}.{t}".format(c=self.database_catalog, t=self.target_table)
            # Upate staging_tables with catalog
            for staging_table in self.staging_tables:
                table = self.staging_tables[staging_table]['table']
                table = "{c}.{t}".format(c=self.database_catalog, t=table)
                self.staging_tables[staging_table]['table'] = table
            # Update tmp_tables with catalog
            # => have to be done during ETL execution in __execute_all_etl() <=
            exe = ExecutorForPresto(self.database_catalog, self.is_dry_run)
        else:
            raise Exception("Unknown database type!")
        self.__execute_all_etl(exe)
        print('')

        # If DQ file is set, run DQ detector
        if self.dq_file is not None and len(self.steps) == 0:
            print('>= Execute DQ file: {f} =<'.format(f=self.dq_file))
            d = Detector(
                yaml_file=self.dq_file,
                is_dry_run=self.is_dry_run,
                is_unit_test=self.is_unit_test,
                variables=self.variables,
                )
            d.run_dq()
