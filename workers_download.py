import json
import MySQLdb
import pandas as pd

def database_cursor():

    with open('./secrets.json') as data_file:
        CLIENT_SECRETS = json.load(data_file)

    USERNAME = CLIENT_SECRETS['mysql2']['username']
    PASSWORD = CLIENT_SECRETS['mysql2']['password']
    DB_NAME = CLIENT_SECRETS['mysql2']['db_name']

    db = MySQLdb.connect(host="fba-bank-db-backup.cgnvxq71e3jm.ap-south-1.rds.amazonaws.com",  # your host, usually localhost
                         user=USERNAME,  # your username
                         passwd=PASSWORD,  # your password
                         db=DB_NAME,  # name of the data base
                         charset='utf8',
                         port=3306)
    return [db, db.cursor()]


def get_workers(msr_id):

    db_cur = database_cursor()
    db = db_cur[0]
    df = pd.read_sql("""SELECT * FROM workers WHERE %s>0 LIMIT 1""" % msr_id, con=db)
    db.close()

    return df


def chunker(lst, n):
    return (lst[pos:pos + n] for pos in xrange(0, len(lst), n))


if __name__ == '__main__':

    df = pd.read_csv("/Users/parthchawla/musters_new.csv")
    ids = df['msr_id'].tolist()
    chunked = []

    for chunk in chunker(ids, 10):
        chunked.append(chunk)

    for chunk in chunked[:2]:
        

    print chunked
    exit()
    workers = get_workers(1)
    print workers