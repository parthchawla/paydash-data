from multiprocessing.pool import ThreadPool
import os
import json
import logging
import requests
import argparse
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.dialects.mysql import BIGINT
from sqlalchemy.types import Integer, String, Float

output = []
valid_response_ids = []
empty_response_ids = []

def run(muster):
   
    url = 'https://nregarep2.nic.in/netnrega/nregapost/API_API3.asmx/API_API3_Workers'

    try:
        post_data = {
                'panchayat_code': str(muster['panchayat_code']).zfill(10),
                'mustrolid': str(muster['msr_no']),
                'Workid':'' 
            }
            
        r = requests.post(url, data=post_data, timeout=120)
        r_dict = r.json()
        r_data = json.loads(r_dict['reponse_data']) # load response data from response
            
        r_data = [{'panchayat_code':muster['panchayat_code'],
                    'msr_no':muster['msr_no'],
                    'msr_id': muster['msr_id'],
                    'address':worker['address'],
                    'age_at_reg':worker['age_at_reg'],
                    'bpl_status':worker['bpl_status'],
                    'current_account_no':worker['current_account_no'],
                    'current_bank_po':worker['current_bank_po'],
                    'gender':worker['gender'],
                    'hoh_name':worker['hoh_name'].strip(),
                    'job_card_number':worker['job_card_number'],
                    'person_id':worker['person_id'],
                    'reg_date':worker['reg_date'],
                    'village_name':worker['Village_name'],
                    'worker_code':worker['worker_code'],
                    'worker_name':worker['worker_name'].strip(),
                    'ac_credited_date':worker['ac_credited_date'],
                    'account_no':worker['account_no'],
                    'average_daily_wage':worker['average_daily_wage'],
                    'bank_po_name':worker['bank_po_name'],
                    'no_days_work_for_muster':worker['no_days_work_for_muster'],
                    'total_to_be_paid_for_muster':worker['total_to_be_paid_for_muster'],
                    'pending_payment_for_muster':worker['pending_payment_for_muster'],
                    'po_address_branch_code':worker['po_address_branch_code'],
                    'po_code_branch_name':worker['po_code_branch_name'],
                    'status':worker['status'],
                    'tool_payments':worker['tool_payments'],
                    'total_cash_payments':worker['total_cash_payments'],
                    'travel_food_expenses':worker['travel_food_expenses'],
                    'wagelist_no':worker['wagelist_no']} for worker in r_data]

        if (len(r_data) == 0):
            empty_response_ids.append(muster['msr_id'])
            return

        valid_response_ids.append(muster['msr_id'])
        output.append(pd.DataFrame(r_data))

    except Exception as e:
        logging.error("Exception occurred", exc_info=True)


def database_engine(db_name = 'musters'):

    with open('/Users/parthchawla/Desktop/secrets.json') as data_file:
        CLIENT_SECRETS = json.load(data_file)

    USERNAME = CLIENT_SECRETS['mysql']['username']
    PASSWORD = CLIENT_SECRETS['mysql']['password']
    DB_NAME = CLIENT_SECRETS['mysql']['db_name_{}'.format(db_name)]
    HOST = CLIENT_SECRETS['mysql']['host']

    engine = create_engine('mysql+mysqldb://' + USERNAME + ':' + PASSWORD + '@' + HOST + ':3306/' + DB_NAME + '?charset=utf8mb4', echo=False)

    return engine


def get_data_types():

    col_names = ['ac_credited_date', 'account_no', 'address', 'age_at_reg', 'average_daily_wage',
                 'bank_po_name', 'bpl_status', 'current_account_no', 'current_bank_po', 'gender',
                 'hoh_name', 'job_card_number', 'msr_no', 'msr_id', 'no_days_work_for_muster', 
                 'panchayat_code', 'pending_payment_for_muster', 'person_id', 
                 'po_address_branch_code', 'po_code_branch_name', 'reg_date', 
                 'status', 'tool_payments', 'total_cash_payments', 
                 'total_to_be_paid_for_muster', 'travel_food_expenses', 
                 'village_name', 'wagelist_no', 'worker_code', 
                 'worker_name']
    
    big_int_col_names = ['panchayat_code']
    int_col_names = ['age_at_reg', 'msr_no', 'msr_id']
    float_col_names = ['average_daily_wage', 'pending_payment_for_muster', 'tool_payments', 
                       'total_cash_payments', 'total_to_be_paid_for_muster', 'travel_food_expenses']
   
    data_types = {col_name: String(100) for col_name in col_names}
    data_types_float = {col_name: Float() for col_name in float_col_names}
    data_types_integer = {col_name: Integer() for col_name in int_col_names}
    data_types_big_integer = {col_name: BIGINT(20) for col_name in big_int_col_names}
    
    data_types.update(data_types_float)
    data_types.update(data_types_integer)
    data_types.update(data_types_big_integer)
        
    return(data_types)


def write_output(output, data_types):

    print("output:")
    print(output)
    # # Dump output to json
    # engine = database_engine('workers')
    # conn = engine.connect()
    # trans = conn.begin()
    
    # col_names = output.columns.values.tolist()

    # try:
    #     output.to_sql('workers', con = conn, chunksize = 1000, 
    #                   if_exists = 'append', dtype = data_types, index = False)
    #     trans.commit()
    #     conn.close()
    # except Exception as e:
    #     print(e)
    #     logging.error("Exception occurred while writing output", exc_info=True)
    #     trans.rollback()
    #     conn.close()

# def update_response(muster_ids, done):
    
#     engine = database_engine()
#     conn = engine.connect()
#     trans = conn.begin()
    
#     try:
#         conn.execute('UPDATE musters a SET a.done = '+ str(done) + ' WHERE a.msr_id IN (' + ','.join(str(i) for i in muster_ids) + ');')
#         trans.commit()
#         conn.close()
#     except Exception as e:
#         print(e)
#         logging.error("Exception occurred while updating queue", exc_info=True)
#         trans.rollback()
#         conn.close()

n = 1200

def pull_workers(musters):

    global output
    global valid_response_ids
    global empty_response_ids

    output = []
    valid_response_ids = []
    empty_response_ids = []

    p = ThreadPool(n)
    p.map(run, musters)
    p.terminate()
    p.join()

    if (len(output) > 0):
        output_db = pd.concat(output)
        data_types = get_data_types()
        write_output(output_db, data_types)

    # if (len(empty_response_ids) > 0):
    #     update_response(empty_response_ids, 2)

    # if (len(valid_response_ids) > 0):
    #     update_response(valid_response_ids, 1)

    print("workers pull finished for chunk")


def get_musters(state_code, test, fin_year):
    print("Getting musters list")

    db = database_engine()
    df_panchayats = pd.read_sql('''SELECT panchayat_code, msr_no, msr_id FROM musters WHERE done != 1;''', con=db)
                                   
    db.dispose()
    print df_panchayats.head()
    if test == 1: df_panchayats = df_panchayats.sample(n=n ,random_state=seed)
    panchayat_list = df_panchayats.to_dict('records')
    
    return(panchayat_list)

panchayat_list = get_musters(34, 0, 2017)

muster_chunks = [panchayat_list[i:i + n] for i in range(0, len(panchayat_list), n)]
print("Total chunks: " + str(len(muster_chunks)))

for index, musters in enumerate(muster_chunks):
    print("pulling workers for: " + str(index) + "/" + str(len(muster_chunks)))
    pull_workers(musters)
    print("pulled workers for: " + str(index))
