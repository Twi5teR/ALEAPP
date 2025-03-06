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
        "paths": ('*/ua.slando/databases/ChatSellingDb.db',),
        "function": "get_olx_selling"
    }
}

from os.path import dirname, join
import sqlite3

from scripts.artifact_report import ArtifactHtmlReport
from scripts.ilapfuncs import logfunc, tsv, timeline, open_sqlite_db_readonly, does_table_exist


def get_olx_selling(files_found, report_folder, seeker, wrap_text, time_offset):
    data_list = []
    # data_list1 = []

    for file_found in files_found:
        file_found = str(file_found)

        if file_found.endswith(('-wal', '-shm', '-journal')):
            continue
        if file_found.endswith('ChatSellingDb.db'):
            db = open_sqlite_db_readonly(file_found)
    #   if file_found.endswith('ChatSellingDb.db-wal'):
    #      attachdb = file_found
    cursor = db.cursor()
    # cursor.execute(f"ATTACH DATABASE '{attachdb}' as ChatSellingDb-wal;")
    cursor.execute('''
        select
    datetime(timestamp, 'unixepoch') AS created_time,
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
        from ChatListItem
        where id = respondent_id and json_valid(content) = 1 order by created_time
        ''')

    all_rows = cursor.fetchall()

    if len(all_rows) > 0:
        for row in all_rows:
            data_list.append((row[0], row[1], row[2], row[3], row[4], row[5], row[6], row[7], row[8], row[9], row[10],
                              row[11], row[12], row[13]))

        report = ArtifactHtmlReport('OLX Selling')
        report.start_artifact_report(report_folder, 'OLX - Selling')
        report.add_script()
        data_headers = (
            'Timestamp', 'ID', 'Ad ID', 'Ad Title', 'Ad Category', 'Ad Active', 'Respondent ID', 'Respondent Name',
            'Respondent Type', 'Blocked', 'Message ID', 'Message UID', 'Message Created At', 'Message Text')
        report.write_artifact_data_table(data_headers, data_list, file_found)
        report.end_artifact_report()

        tsvname = 'OLX Selling'
        tsv(report_folder, data_headers, data_list, tsvname)

        tlactivity = 'OLX Selling'
        timeline(report_folder, tlactivity, data_list, data_headers)
    else:
        logfunc('No OLX Selling available')

    db.close()


__artifacts__ = {
    "OLX": (
        "OLX",
        ('*_im.db*', '*db_im_xx*'),
        get_olx_selling)
}
