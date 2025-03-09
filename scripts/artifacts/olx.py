__artifacts_v2__ = {
    "OLX": {
        "name": "OLX",
        "description": "Extracts data from OLX database files",
        "author": "Vitalii Kolesnyk",
        "version": "0.1",
        "date": "2025-03-02",
        "requirements": "none",
        "category": "OLX Selling",
        "notes": "",
        "paths": ('*/ua.slando/databases/ChatSellingDb.db*',),
        "function": "get_olx_selling"
    }
}

import sqlite3

from scripts.artifact_report import ArtifactHtmlReport
from scripts.ilapfuncs import logfunc, tsv, timeline, open_sqlite_db_readonly, does_table_exist


def get_olx_selling(files_found, report_folder, seeker, wrap_text, time_offset):
    # data_list1 = []
    logfunc("Processing data for OLX Selling")
    data_list = []
    for file_found in files_found:
        file_found = str(file_found)
        if file_found.endswith('db'):
            logfunc(f"Files found: {files_found}")
            logfunc(f"Using database file: {file_found}")
            db = open_sqlite_db_readonly(file_found)
            cursor = db.cursor()
            logfunc("Executing SQL query...")
            cursor.execute('''
            SELECT 
    datetime(ChatListItem.timestamp,'unixepoch'),
    id,
    json_extract(ad, '$.id') as ad_id,  
    json_extract(ad, '$.title') as ad_title,  
    json_extract(ad, '$.category.type') as ad_category,  
    json_extract(ad, '$.active') as ad_active,  
    json_extract(respondent, '$.id') AS respondent_id,  
    json_extract(respondent, '$.name') AS respondent_name,  
    json_extract(respondent, '$.type') AS respondent_type,  
    json_extract(respondent, '$.blocked') AS respondent_blocked,  
    json_extract(messages, '$[0].id') AS message_id,  
    json_extract(messages, '$[0].user_id') AS message_user_id,  
    json_extract(messages, '$[0].created_at') AS message_created_at,  
    json_extract(messages, '$[0].text') AS message_text
            FROM ChatListItem
            ''')
            logfunc("SQL query executed successfully")
            all_rows = cursor.fetchall()
            logfunc(f"Found Rows: {len(all_rows)}")
            # cursor.execute("PRAGMA table_info(ChatListItem);")
            # columns = cursor.fetchall()
            # logfunc(f"Columns in ChatListItem: {columns}")
            usageentries = len(all_rows)
            if usageentries > 0:
                logfunc(f"Found OLX {usageentries} Entries")
                report = ArtifactHtmlReport('OLX Selling')

            for row in all_rows:
                data_list.append((row[0], row[1], row[2], row[3], row[4], row[5], row[6], row[7], row[8], row[9],
                                  row[10], row[11], row[12], row[13]))
                db.close()
    if data_list:
        report = ArtifactHtmlReport('OLX Selling')
        report.start_artifact_report(report_folder, 'Selling Events')
        report.add_script()
        data_headers = ('Timestamp (UTC)', 'ID', 'Ad ID', 'Ad Title', 'Ad Category', 'Ad Active', 'Respondent ID', 'Respondent Name',
            'Respondent Type', 'Blocked', 'Message ID', 'Message UID', 'Message Created At', 'Message Text')

        report.write_artifact_data_table(data_headers, data_list, file_found)
        report.end_artifact_report()

        tsvname = f'OLX Selling'
        tsv(report_folder, data_headers, data_list, tsvname)

        tlactivity = f'OLX Selling'
        timeline(report_folder, tlactivity, data_list, data_headers)
    else:
        logfunc('No OLX Selling available')


__artifacts__ = {
    "OLX": (
        "OLX",
        ('ChatSellingDb.*'),
        get_olx_selling)
}
