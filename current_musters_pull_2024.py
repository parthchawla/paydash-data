import argparse
import datetime
import multiprocessing
import pandas as pd
import requests
import shared.common as shared
import sys
import json
import os
import time

def start_process(chunk,parse_error_list,api_error_list):

    db_cur = shared.database_cursor()
    db = db_cur[0]
    cur = db_cur[1]

    # delete_table = "DROP TABLE IF EXISTS current_musters;"
    # cur.execute(delete_table)

    # new_table = "create table current_musters (msr_no VARCHAR(10) NOT NULL, work_name VARCHAR(1000) NOT NULL, work_code VARCHAR(100) NOT NULL, start_date date NOT NULL,
    # end_date date NOT NULL, block_code VARCHAR(7) NOT NULL, block_name VARCHAR(100) NOT NULL, panchayat_code VARCHAR(10) NOT NULL, panchayat_name VARCHAR(100) NOT NULL,
    # input_date date NOT NULL, created_at DATETIME NOT NULL, CONSTRAINT pk_RecordId PRIMARY KEY (block_code,msr_no)) CHARACTER SET utf8 COLLATE utf8_unicode_ci;";
    # '''CREATE TABLE current_muster_missed_blocks (block_code VARCHAR(7) NOT NULL) CHARACTER SET utf8 COLLATE utf8_unicode_ci;'''

    for param_set in chunk:
        d = param_set['date']
        state_code = param_set['state_code']
        block_code = param_set['block_code']
        block_name = param_set['block_name']
        fin_year = param_set['fin_year']
        state_block_code = param_set['state_block_code']
        url_current = 'https://nregarep2.nic.in/Netnrega/nrega-reportdashboard/api/dashboard_delay.aspx?fin_year=' + fin_year + '&r_date=' + d + '&Block_code=' + block_code + '&state_block_code=' + state_block_code

        # current muster api
        for i in range(0,5):
            api_success = 0
            try:
                response = requests.get(url_current, timeout=120, verify = True)
            except Exception, e:
                # if on the fifth try, insert into db and go on to next param_set
                if i == 4:
                    api_error_list.append("API error for {}: {}".format(param_set, str(e)))

                    if RETRY_MISSES == 'True':
                        cur.execute('''INSERT INTO current_muster_missed_blocks (block_code)
                          VALUES (%s);''', [block_code])
                        db.commit()

                    break
                else:
                    continue
            
            # if successful try to parse the response
            else:
                api_success = 1
                break

        # if the api is successful, try to parse the data
        if api_success == 1:
            try:
                response_json = shared.byteify(response.json())
                upload_data = []
                musters = []
                for item in response_json:
                    msr_no = item['muster_no']
                    work_name = item['work_name'].strip()
                    work_code = item['work_code'].strip()
                    start_date_string = item['Start_date'].strip()
                    start_date = datetime.datetime.strptime(start_date_string, "%m-%d-%y").date().strftime('%Y-%m-%d')
                    end_date_string = item['end_date'].strip()
                    end_date = datetime.datetime.strptime(end_date_string, "%m-%d-%y").date().strftime('%Y-%m-%d')
                    panchayat_code = item['panchayat_code']
                    panchayat_name = item['panchayat_name']
                    input_date = d
                    created_at = datetime.datetime.now()
                    if msr_no not in musters:  # ignore duplicates
                        upload_data.append((msr_no, work_name, work_code, start_date, end_date, block_code, block_name, panchayat_code, panchayat_name, input_date, created_at))
                        musters.append(msr_no)
                cur.executemany('''INSERT INTO current_musters (msr_no, work_name, work_code, start_date, end_date, block_code, block_name, panchayat_code, panchayat_name, input_date, created_at)
                      VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);''', upload_data)
                db.commit()

            except Exception, e:
                parse_error_list.append("Parse error for block {} (code {}) on {}: {}".format(param_set['block_name'], param_set['block_code'], d, e))

    db.commit()
    db.close()
    return

if __name__ == '__main__':

    parser = argparse.ArgumentParser(description='Parses input to specify which subset of block data to pull from the musters APIs')
    parser.add_argument('-s', '--current_date',         required=True)
    parser.add_argument('-m', '--maxprocesses',         required=True)
    parser.add_argument('-x', '--excludestates',        required=False, nargs="*", default=[])
    parser.add_argument('-n', '--includestates',        required=False, nargs="*", default=[])
    parser.add_argument('-p', '--pilotdistricts',       required=False)
    parser.add_argument('-t', '--treatmentdistricts',   required=False)
    parser.add_argument('-c', '--controldistricts',     required=False)
    parser.add_argument('-z', '--savetos3',             required=False)
    parser.add_argument('-r', '--retrymisses',          required=False)
    parser.add_argument('-a', '--retrycount',           required=False)
    parser.add_argument('-e', '--checktiming',          required=False)
    args = parser.parse_args()

    EXCLUDE_STATES = args.excludestates
    INCLUDE_STATES = args.includestates
    MAX_PROCESSES = int(args.maxprocesses)
    PILOT_DISTRICTS = args.pilotdistricts
    TREATMENT_DISTRICTS = args.treatmentdistricts
    CONTROL_DISTRICTS = args.controldistricts
    SAVE_TO_S3 = args.savetos3
    RETRY_MISSES = args.retrymisses
    RETRY_COUNT = int(args.retrycount)
    CHECK_TIMING = args.checktiming

    if len(INCLUDE_STATES) > 0 and len(EXCLUDE_STATES) > 0:
        print sys.exit("Error: Include and exclude args are mutually exclusive")

    if TREATMENT_DISTRICTS == 'True' and CONTROL_DISTRICTS == 'True':
        print sys.exit("Error: Treatment and control args are mutually exclusive")

    CURRENT_DATE = datetime.date(int(args.current_date[0:4]), int(args.current_date[5:7]), int(args.current_date[8:]))

    short_names = {'01': 'AN', '02': 'AP', '03': 'AR', '04': 'AS', '05': 'BH', '07': 'DN', '08': 'DD', '10': 'GO', '11': 'GJ', '12': 'HR', '13': 'HP', '14': 'JK', '15': 'KN', '16': 'KL', '17': 'MP',
                   '18': 'MH', '19': 'LK', '20': 'MN', '21': 'MG', '22': 'MZ', '23': 'NL', '24': 'OR', '25': 'PC', '26': 'PB', '27': 'RJ', '28': 'SK', '29': 'TN', '30': 'TR', '31': 'UP', '32': 'WB',
                   '33': 'CH', '34': 'JH', '35': 'UT'}

    api_inputs = []
    fin_year = shared.get_fy(CURRENT_DATE)
    current_date_str = CURRENT_DATE.strftime('%Y-%m-%d')
    region_list = shared.get_block_list(fin_year, INCLUDE_STATES, EXCLUDE_STATES)

    # Saving the db table data to .csv in case we need to analyze it later
    states_to_pull = [x.encode('utf-8') for x in pd.DataFrame(region_list,columns=['state_code']).drop_duplicates().state_code.tolist()]
    db_cur = shared.database_cursor()
    db = db_cur[0]
    cur = db_cur[1]

    # filter region list for selected state(s) to just the pilot districts
    if PILOT_DISTRICTS == 'True':
        districts = ['1729','3404','3315']
        region_list = [region for region in region_list if region['district_code'] in districts]
    
    # filter region list for selected state(s) to just the treatment districts
    if TREATMENT_DISTRICTS == 'True':
        districts = pd.read_sql('''SELECT district_code FROM treatment WHERE treatment<>0;''',con=db)['district_code'].tolist()
        region_list = [region for region in region_list if region['district_code'] in districts]

    # filter region list for selected state(s) to just the control districts
    if CONTROL_DISTRICTS == 'True':
        districts = pd.read_sql('''SELECT district_code FROM treatment WHERE treatment=0;''',con=db)['district_code'].tolist()
        region_list = [region for region in region_list if region['district_code'] in districts]

    if SAVE_TO_S3 == 'True':
        df_current = pd.read_sql('''SELECT * FROM current_musters;''', con=db)
        file_name_current = './musters_api/archived_musters/musters_current_'+CURRENT_DATE.strftime('%Y%m%d')+'.csv'
        target_current = 'musters/musters_current_'+CURRENT_DATE.strftime('%Y%m%d')+'.csv'
        bucket_name = 'paydata'
        
        if not os.path.isfile(file_name_current):
            df_current.to_csv(file_name_current,index=False, encoding='utf-8')
            #shared.s3_upload(file_name_current,target_current,bucket_name)
            os.remove(file_name_current)

        cur.execute('''DELETE FROM current_musters;''')

    if RETRY_MISSES == 'True':
        cur.execute('''DELETE FROM current_muster_missed_blocks;''')

    db.commit()
    db.close()

    for region in region_list:
        state_code = region['state_code']
        state_block_code = short_names[state_code] + region['block_code'][2:4]
        block_code = region['block_code']
        block_name = region['block_name']
        fin_year_url = '20'+fin_year[0:2]+'-20'+fin_year[2:4]
        api_inputs.append({'date': current_date_str, 'state_code': state_code, 'block_code': block_code, 'block_name': block_name, 'fin_year': fin_year_url, 'state_block_code': state_block_code})


    chunks = [api_inputs[i::MAX_PROCESSES] for i in xrange(MAX_PROCESSES) if i < len(api_inputs)]

    mgr = multiprocessing.Manager()
    parse_error_list = mgr.list()
    api_error_list = mgr.list()
    jobs=[]

    for chunk in chunks:
        p = multiprocessing.Process(target=start_process, args=(chunk,parse_error_list,api_error_list))
        jobs.append(p)
        p.start()

    for process in jobs:
        process.join()

    with open('./shared/recipients.json') as data_file:
        error_recipients = json.load(data_file)[0]['errors']

    if len(parse_error_list)>0 and RETRY_MISSES!='True':
        msg = '\n\n'.join(parse_error_list)
        subject = 'PayDash: Encountered %d Current Muster Parse Error(s)' % len(parse_error_list)
        shared.send_email(msg,subject,error_recipients)

    if len(api_error_list)>0 and RETRY_MISSES!='True':
        msg = '\n\n'.join(api_error_list)
        subject = 'PayDash: Encountered %d Current Muster API Error(s)' % len(api_error_list)
        shared.send_email(msg,subject,error_recipients)

    if RETRY_MISSES == 'True':
        retry_count = RETRY_COUNT
        sleep_time = 15 * 60

        # check how many blocks we missed. if it's not 0, wait 15 minutes then try again until it's 0 or we've tried 12 times
        for i in range(0,retry_count):

            db_cur = shared.database_cursor()
            db = db_cur[0]
            cur = db_cur[1]
            
            missed_blocks = pd.read_sql('''SELECT * FROM current_muster_missed_blocks;''', con=db).block_code.tolist()
            
            if len(missed_blocks) == 0:
                db.close()
                break
            
            print 'Retrying:',str(i+1)
            print 'Missed blocks to retry:',str(len(missed_blocks))
            print '\n'.join(missed_blocks)

            cur.execute('''DELETE FROM current_muster_missed_blocks;''')
            db.commit()
            db.close()
        
            region_list = [region for region in region_list if region['block_code'] in missed_blocks]
            
            api_inputs = []

            for region in region_list:
                state_code = region['state_code']
                state_block_code = short_names[state_code] + region['block_code'][2:4]
                block_code = region['block_code']
                block_name = region['block_name']
                fin_year_url = '20'+fin_year[0:2]+'-20'+fin_year[2:4]
                api_inputs.append({'date': current_date_str, 'state_code': state_code, 'block_code': block_code, 'block_name': block_name, 'fin_year': fin_year_url, 'state_block_code': state_block_code})

            chunks = [api_inputs[i::MAX_PROCESSES] for i in xrange(MAX_PROCESSES) if i < len(api_inputs)]

            mgr = multiprocessing.Manager()
            parse_error_list = mgr.list()
            api_error_list = mgr.list()
            jobs=[]

            print 'Going to sleep before trying the server again'
            time.sleep(sleep_time)

            print 'Running the retry pull'
            for chunk in chunks:
                p = multiprocessing.Process(target=start_process, args=(chunk,parse_error_list,api_error_list))
                jobs.append(p)
                p.start()

            for process in jobs:
                process.join()

            print i, (retry_count - 1)
            if i == (retry_count - 1):
                db_cur = shared.database_cursor()
                db = db_cur[0]
                cur = db_cur[1]
            
                missed_blocks = pd.read_sql('''SELECT * FROM current_muster_missed_blocks;''', con=db).block_code.tolist()
                db.close()

                if len(parse_error_list)>0:
                    msg = '\n\n'.join(parse_error_list)
                    subject = 'PayDash: Encountered %d Current Muster Parse Error(s)' % len(parse_error_list)
                    shared.send_email(msg,subject,error_recipients)

                if len(api_error_list)>0:
                    msg = '\n\n'.join(api_error_list)
                    subject = 'PayDash: Encountered %d Current Muster API Error(s)' % len(api_error_list)
                    shared.send_email(msg,subject,error_recipients)

                print 'Exiting because ran out of retries. Missed block count:',str(len(missed_blocks)) 
                print '\n'.join(missed_blocks)
                break


    # check if we got all the data in time
    if CHECK_TIMING == 'True':
        now_time = datetime.datetime.now().time()
        if now_time > datetime.time(3,0):
            if PILOT_DISTRICTS == 'True':
                msg = 'WARNING: Did not pull all the pilot current muster data before 8:30am IST'
                subject = 'PayDash: Current muster pilot pull warning'
            elif TREATMENT_DISTRICTS == 'True':
                msg = 'WARNING: Did not pull all the treatment current muster data before 8:30am IST'
                subject = 'PayDash: Current muster treatment pull warning'
            elif CONTROL_DISTRICTS == 'True':
                msg = 'WARNING: Did not pull all the control current muster data before 8:30am IST'
                subject = 'PayDash: Current muster control pull warning'
            else:
                msg = 'WARNING: Did not pull all the current muster data before 8:30am IST'
                subject = 'PayDash: Current muster pull warning'
            
            shared.send_email(msg,subject,error_recipients)


    
