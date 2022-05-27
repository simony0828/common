import prestodb
import os
from lib.run_with_retry import run_with_retry

class Presto:
    SSL_CERT_PATH = os.environ.get('SSL_CERT', '/etc/ssl/certs/ca-certificates.crt')

    def __init__(self, PRESTO_HOST='PRESTO_HOST', PRESTO_PORT='PRESTO_PORT', PRESTO_USER='PRESTO_USER',
                 PRESTO_PASSWORD='PRESTO_PASSWORD', skip_cert_validation=False, retries_on_query_failure=0):
        """
        :param skip_cert_validation: Forces skipping validation of the SSL certificate (useful in development)
        :param ratries_on_query_failure: Number of retries when received PresotQueryError
        """

        if PRESTO_HOST not in os.environ:
            raise Exception("Missing {v} as environment variable".format(v=PRESTO_HOST))
        if PRESTO_PORT not in os.environ:
            raise Exception("Missing {v} as environment variable".format(v=PRESTO_PORT))
        if PRESTO_USER not in os.environ:
            raise Exception("Missing {v} as environment variable".format(v=PRESTO_USER))
        if PRESTO_PASSWORD not in os.environ:
            raise Exception("Missing {v} as environment variable".format(v=PRESTO_PASSWORD))

        host = os.environ[PRESTO_HOST]
        port = os.environ[PRESTO_PORT]
        user = os.environ[PRESTO_USER]
        password = os.environ[PRESTO_PASSWORD]

        try:
            self.connection = prestodb.dbapi.connect(
                host=host,
                port=port,
                user=user,
                http_scheme='https',
                auth=prestodb.auth.BasicAuthentication(user, password),
            )
            if skip_cert_validation is True:
                self.connection._http_session.verify = False
            else:
                self.connection._http_session.verify = Presto.SSL_CERT_PATH
        except(Exception) as error:
            print("ERROR ==> {e}".format(e=error))
            raise Exception(error)

        self.cursor = self.connection.cursor()
        self.db_host = host
        self.db_user = user
        self.cursor_result = None
        self.retries_on_query_failure = retries_on_query_failure

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.commit()
        self.connection.close()

    def commit(self):
        self.connection.commit()

    def execute(self, sql):
        try:
            for s in sql.split(';'):
                if len(s) > 0:
                    self.cursor.execute(s.rstrip(';'))
                    self.cursor_result = self.cursor.fetchall()
        except(Exception) as error:
            print("ERROR ==> {e}".format(e=error))
            raise error
        #self.commit()

    def __check_if_internal_error(self, ex: Exception) -> bool:
        # Returns true if error_type for exception is 'INTERNAL_ERROR'
        return (
            hasattr(ex, 'error_type') and
            ex.error_type == 'INTERNAL_ERROR'
        )

    def query(self, sql):
        run_with_retry(lambda: self.execute(sql),
                       exceptions=prestodb.exceptions.PrestoQueryError,
                       retry_number=self.retries_on_query_failure,
                       validate=self.__check_if_internal_error)

        return self.cursor_result
        #if self.cursor.description is None:
        #    return None
        #try:
        #    return self.cursor.fetchall()
        #except(Exception) as error:
        #    print("ERROR ==> {e}".format(e=error))
        #    raise Exception(error)

    def rows(self):
        return self.cursor.rowcount

    def get_host(self):
        return self.db_host

    def get_user(self):
        return self.db_user
