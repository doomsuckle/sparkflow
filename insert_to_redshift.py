"""Script to connect to redshift, copy from s3, and make the table swap"""

import psycopg2
import os
from argparse import ArgumentParser


class RedshiftConnectionError(Exception):
    """Error for when you can't connect to Redshift"""
    pass

class ArgHandler(ArgumentParser):
    """Generic wrapper for handling arguments"""

    def __init__(self, **kwargs):

        ArgumentParser.__init__(self, **kwargs)

        self.add_argument('--bucket_prefix', '-dirp',
                          help='s3 prefix for loading data',
                          required=True)

        self.add_argument('--tablename', '-tn',
                          help='name of the table to load data into',
                          required=True)

        self.add_argument('--s3_bucket', '-s3b',
                          help='table name for prefix in s3',
                          default='name of the s3 bucket to load into')

        self.add_argument('--pg_password', '-pgp',
                          help='redshift password',
                          required=True)

        self.add_argument('--pg_host', '-pgh',
                          help='redshift host name',
                          required=True)

        self.add_argument('--pg_user', '-pgu',
                          help='',
                          default='redshift username')

        self.add_argument('--pg_database', '-pgd',
                          help='',
                          default='redshift database name')

        self.args = self.parse_args()

        self.__checks()

    def __checks(self):
        """Internal checks for consistency"""

        # check postgres connection
        try:
            self.pg_conn = psycopg2.connect(database=self.args.pg_database,
                                            user=self.args.pg_user,
                                            host=self.args.pg_host,
                                            password=self.args.pg_password,
                                            port=5439)

        except RedshiftConnectionError:
            self.error('Failed to connect to redshift')

def make_query(tablename, aws_access_key_id, aws_secret_access_key, bucketname, bucket_prefix):
    """Generate query to run copy command and swap tables"""

    return '''
    copy {tablename}
    from 's3://{bucketname}{bucket_prefix}'
    credentials 'aws_access_key_id={aws_access_key_id};aws_secret_access_key={aws_secret_access_key}'
    maxerror 100000
    format as json 'auto';

    '''.format(tablename=tablename,
               bucketname=bucketname,
               bucket_prefix=bucket_prefix,
               aws_access_key_id=aws_access_key_id,
               aws_secret_access_key=aws_secret_access_key)


def main():
    """main program. create database connection, run query from make_query"""

    argparser = ArgHandler()

    print 'load data into:', argparser.args.tablename
    dbconn = argparser.pg_conn

    with dbconn.cursor() as cur:
        query = make_query(argparser.args.tablename,
                           os.getenv('AWS_ACCESS_KEY_ID'),
                           os.getenv('AWS_SECRET_ACCESS_KEY'),
                           argparser.args.s3_bucket,
                           argparser.args.bucket_prefix)

        cur.execute(query)

    dbconn.commit()
    dbconn.close()

if __name__ == '__main__':
    main()
