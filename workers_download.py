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

def get_workers(msr_ids):

    tup = tuple(msr_ids)
    db_cur = database_cursor()
    db = db_cur[0]
    df = pd.read_sql("""SELECT * FROM workers WHERE id='%s' or id='%s' or id='%s' or id='%s' or id='%s' or id='%s' or id='%s' or id='%s' or id='%s' or id='%s'""" % tup, con=db)
    db.close()

    return df

def chunker(lst, n):
    return (lst[pos:pos + n] for pos in xrange(0, len(lst), n))

if __name__ == '__main__':

    df = pd.read_csv("/Users/parthchawla/musters_new.csv")
    df = df.loc[df['done'] == 1]
    ids = df['msr_id'].tolist()
    print 'No. of musters with workers pulled:',len(ids)

    chunked = []
    for chunk in chunker(ids, 10):
        chunked.append(chunk)
    print 'No. of chunks:',len(chunked)

    workers = pd.DataFrame(columns = ['id','ac_credited_date','account_no','address','age_at_reg','average_daily_wage','bank_po_name','bpl_status','current_account_no','current_bank_po','gender','hoh_name','job_card_number','msr_id','msr_no','no_days_work_for_muster','panchayat_code','pending_payment_for_muster','person_id','po_address_branch_code','po_code_branch_name','reg_date','status','tool_payments','total_cash_payments','total_to_be_paid_for_muster','travel_food_expenses','village_name','wagelist_no','worker_code','worker_name'])
    for i,chunk in enumerate(chunked):
        print 'Chunk',i+1
        worker_chunk = get_workers(chunk)
        workers = workers.append(worker_chunk)

    workers.to_csv("/Users/parthchawla/workers.csv")
