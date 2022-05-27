import snowflake.connector
import os
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.hazmat.primitives.asymmetric import dsa
from cryptography.hazmat.primitives import serialization

class Snowflake:
    def __init__(self, SNOWSQL_SSO='SNOWSQL_SSO', SNOWSQL_ACCOUNT='SNOWSQL_ACCOUNT', SNOWSQL_USER='SNOWSQL_USER',
        SNOWSQL_PRIVATE_KEY_PASSPHRASE='SNOWSQL_PRIVATE_KEY_PASSPHRASE', SNOWSQL_PRIVATE_KEY_PATH='SNOWSQL_PRIVATE_KEY_PATH',
        SNOWSQL_PRIVATE_KEY_P8='SNOWSQL_PRIVATE_KEY_P8', SNOWSQL_PASSWORD='SNOWSQL_PASSWORD',
        SNOWSQL_DATABASE='SNOWSQL_DATABASE', SNOWSQL_WAREHOUSE='SNOWSQL_WAREHOUSE', SNOWSQL_ROLE='SNOWSQL_ROLE'):
        if SNOWSQL_ACCOUNT not in os.environ:
            raise Exception("Missing {v} as environment variable".format(v=SNOWSQL_ACCOUNT))
        if SNOWSQL_USER not in os.environ:
            raise Exception("Missing {v} as environment variable".format(v=SNOWSQL_USER))

        auth_method = 'keypair'
        if SNOWSQL_SSO in os.environ:
            if os.environ[SNOWSQL_SSO].lower() in ('true', '1', 't'):
                auth_method = 'sso'
        elif SNOWSQL_PASSWORD in os.environ:
            auth_method = 'password'
            password = os.environ[SNOWSQL_PASSWORD]
        else:
            if SNOWSQL_PRIVATE_KEY_PASSPHRASE not in os.environ:
                raise Exception("Missing {v} as environment variable".format(v=SNOWSQL_PRIVATE_KEY_PASSPHRASE))
            if SNOWSQL_PRIVATE_KEY_PATH not in os.environ and SNOWSQL_PRIVATE_KEY_P8 not in os.environ:
                raise Exception("Missing {v1} or {v2} as environment variable".format(
                    v1=SNOWSQL_PRIVATE_KEY_PATH,v2=SNOWSQL_PRIVATE_KEY_P8))

        account = os.environ[SNOWSQL_ACCOUNT]
        host = "{a}.snowflakecomputing.com".format(a=account)
        user = os.environ[SNOWSQL_USER]

        if auth_method == 'keypair':
            passphrase = os.environ[SNOWSQL_PRIVATE_KEY_PASSPHRASE].encode()

            if SNOWSQL_PRIVATE_KEY_P8 in os.environ:
                p_key = serialization.load_pem_private_key(
                    os.environ[SNOWSQL_PRIVATE_KEY_P8].encode(),
                    password=passphrase,
                    backend=default_backend()
                )
            else:
                key_path = os.environ[SNOWSQL_PRIVATE_KEY_PATH]
                with open(key_path, 'rb') as key:
                    p_key = serialization.load_pem_private_key(
                        key.read(),
                        password=passphrase,
                        backend=default_backend()
                    )

            pkb = p_key.private_bytes(
                encoding=serialization.Encoding.DER,
                format=serialization.PrivateFormat.PKCS8,
                encryption_algorithm=serialization.NoEncryption()
            )

        # Set default
        database = 'SHASTA_SDC_PUBLISHED'
        if user.upper().startswith('INT_'):
            database = 'INT_SHASTA_SDC_PUBLISHED'
        if user.upper().startswith('SBX_'):
            database = 'SBX_SHASTA_SDC_PUBLISHED'
        warehouse = None
        role = None

        if SNOWSQL_DATABASE in os.environ:
            database = os.environ[SNOWSQL_DATABASE]
        if SNOWSQL_WAREHOUSE in os.environ:
            warehouse  = os.environ[SNOWSQL_WAREHOUSE]
        if SNOWSQL_ROLE in os.environ:
            role = os.environ[SNOWSQL_ROLE]

        try:
            if auth_method == 'sso':
                self.connection = snowflake.connector.connect(
                    user=user,
                    account=account,
                    authenticator='externalbrowser',
                    warehouse=warehouse,
                    database=database,
                    role=role,
                    session_parameters={"QUERY_TAG": "From Python"},
                )
            elif auth_method == 'password':
                self.connection = snowflake.connector.connect(
                    user=user,
                    account=account,
                    password=password,
                    warehouse=warehouse,
                    database=database,
                    role=role,
                    session_parameters={"QUERY_TAG": "From Python"},
                )
            else:
                self.connection = snowflake.connector.connect(
                    user=user,
                    account=account,
                    private_key=pkb,
                    warehouse=warehouse,
                    database=database,
                    role=role,
                    session_parameters={"QUERY_TAG": "From Python"},
                )
        except(Exception) as error:
            print("ERROR ==> {e}".format(e=error))
            raise Exception(error)

        self.cursor = self.connection.cursor()
        self.db_host = host
        self.db_user = user
        self.account = account

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.commit()
        self.connection.close()

    def commit(self):
        self.connection.commit()

    @property
    def env(self) -> str:
        env = None
        if self.db_user.lower().startswith('int_'):
            env = 'NON-PROD'
        elif self.db_user.lower().startswith('sbx_'):
            env = 'NON-PROD'
        elif self.account.lower().startswith('prd_'):
            env = 'PROD'
        else:
            env = 'NON-PROD'
        return env

    @property
    def shared_schema(self) -> str:
        return ".".join(["COMMON", "SHARED"])

    @property
    def watcher_databases(self) -> str:
        return "'DB1', 'DB2', 'DB3', 'DB4'"

    @property
    def user_database(self) -> str:
        return "SF_USER"

    @property
    def dq_table(self) -> str:
        return f"{self.shared_schema}.dq_data_result"

    def execute(self, sql):
        try:
            self.cursor.execute(sql)
        except(Exception) as error:
            print("ERROR ==> {e}".format(e=error))
            raise Exception(error)
        self.commit()

    def query(self, sql, header=False):
        self.execute(sql)
        if self.cursor.description is None:
            return None
        try:
            if header:
                head_row = [desc[0] for desc in self.cursor.description]
                return (self.cursor.fetchall() , head_row)
            else:
                return self.cursor.fetchall()
        except(Exception) as error:
            print("ERROR ==> {e}".format(e=error))
            raise Exception(error)

    def rows(self):
        return self.cursor.rowcount

    def get_host(self):
        return self.db_host

    def get_user(self):
        return self.db_user

    def use_cached_result(self, use_cache=True):
        sql = "alter session set use_cached_result={use_cache};".format(**locals())
        print("*** USE_CACHE={use_cache} ***".format(**locals()))
        self.execute(sql)
