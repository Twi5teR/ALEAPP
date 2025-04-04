__artifacts_v2__ = {
    "OLX": {
        "name": "OLX",
        "description": "Extracts data from OLX database files",
        "author": "Vitalii Kolesnyk",
        "version": "0.1",
        "date": "2025-03-15",
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
    logfunc("Processing data for OLX Selling")
    data_list = []
    data_list2 = []
    for file_found in files_found:
        file_found = str(file_found)
        if file_found.endswith('ChatSellingDb.db'):
            logfunc(f"Files found: {files_found}")
            logfunc(f"Using database file: {file_found}")
            db = open_sqlite_db_readonly(file_found)
            cursor = db.cursor()
            logfunc("Executing SQL query...")
            cursor.execute('''
                    SELECT 
            datetime(ChatListItem.timestamp,'unixepoch'),
            ChatListItem.id,
            json_extract(ChatListItem.ad, '$.id'),  
            json_extract(ChatListItem.ad, '$.title'),  
            json_extract(ChatListItem.ad, '$.category.type'),  
            case json_extract(ChatListItem.ad, '$.active') 
            when 0 then 'No'
            when 1 then 'Yes'
            end as "Ad Active",
            json_extract(ChatListItem.respondent, '$.id'),  
            json_extract(ChatListItem.respondent, '$.name'),  
            json_extract(ChatListItem.respondent, '$.type'),  
            json_extract(ChatListItem.respondent, '$.blocked'),  
            json_extract(messages.value, '$.id'),  
            json_extract(messages.value, '$.user_id'),  
            json_extract(messages.value, '$.created_at'),  
            json_extract(messages.value, '$.text')
                    FROM ChatListItem,
            json_each(ChatListItem.messages) AS messages;
                    ''')
            logfunc("SQL query executed successfully")
            all_rows = cursor.fetchall()
            logfunc(f"Found Rows: {len(all_rows)}")
            usageentries = len(all_rows)
            if usageentries > 0:
                logfunc(f"Found OLX {usageentries} Entries")
                report = ArtifactHtmlReport('OLX Selling')

            for row in all_rows:
                data_list.append((row[0], row[1], row[2], row[3], row[4], row[5], row[6], row[7], row[8], row[9],
                                  row[10], row[11], row[12], row[13]))
                db.close()

        if file_found.endswith('db3'):
            logfunc(f"Extraction from WAL found: {files_found}")
            logfunc(f"Using database file: {file_found}")
            db = open_sqlite_db_readonly(file_found)
            cursor = db.cursor()
            logfunc("Executing SQL query...")
            cursor.execute('''
            SELECT 
            datetime(ChatListItem.timestamp,'unixepoch'),
            ChatListItem.id,
            json_extract(ChatListItem.ad, '$.id'),  
            json_extract(ChatListItem.ad, '$.title'),  
            json_extract(ChatListItem.ad, '$.category.type'),  
            case json_extract(ChatListItem.ad, '$.active') 
            when 0 then 'No'
            when 1 then 'Yes'
            end as "Ad Active",
            json_extract(ChatListItem.respondent, '$.id'),  
            json_extract(ChatListItem.respondent, '$.name'),  
            json_extract(ChatListItem.respondent, '$.type'),  
            json_extract(ChatListItem.respondent, '$.blocked'),  
            json_extract(messages.value, '$.id'),  
            json_extract(messages.value, '$.user_id'),  
            json_extract(messages.value, '$.created_at'),  
            json_extract(messages.value, '$.text')
                    FROM ChatListItem,
            json_each(ChatListItem.messages) AS messages;
                    ''')
            logfunc("SQL query executed successfully")
            all_rows = cursor.fetchall()
            logfunc(f"Found Rows: {len(all_rows)}")
            usageentries = len(all_rows)
            if usageentries > 0:
                logfunc(f"Found OLX {usageentries} Entries")
                report = ArtifactHtmlReport('OLX Selling')

            for row in all_rows:
                data_list2.append((row[0], row[1], row[2], row[3], row[4], row[5], row[6], row[7], row[8], row[9],
                              row[10], row[11], row[12], row[13]))
                db.close()

        if data_list:
            report = ArtifactHtmlReport('OLX Selling')
            report.start_artifact_report(report_folder, 'Selling Events')
            report.add_script()
            data_headers = (
                'Timestamp (UTC)', 'ID', 'Ad ID', 'Ad Title', 'Ad Category', 'Ad Active', 'Respondent ID',
                'Respondent Name',
                'Respondent Type', 'Blocked', 'Message ID', 'Message UID', 'Message Created At', 'Message Text')

            report.write_artifact_data_table(data_headers, data_list, file_found)
            report.end_artifact_report()

            tsvname = f'OLX Selling'
            tsv(report_folder, data_headers, data_list, tsvname)

            tlactivity = f'OLX Selling'
            timeline(report_folder, tlactivity, data_list, data_headers)

        if data_list2:
            report = ArtifactHtmlReport('OLX Selling - WAL')
            report.start_artifact_report(report_folder, 'Selling Events - WAL')
            report.add_script()
            data_headers = (
                'Timestamp (UTC)', 'ID', 'Ad ID', 'Ad Title', 'Ad Category', 'Ad Active', 'Respondent ID',
                'Respondent Name',
                'Respondent Type', 'Blocked', 'Message ID', 'Message UID', 'Message Created At', 'Message Text')

            report.write_artifact_data_table(data_headers, data_list2, file_found)
            report.end_artifact_report()

            tsvname = f'OLX Selling'
            tsv(report_folder, data_headers, data_list2, tsvname)

            tlactivity = f'OLX Selling'
            timeline(report_folder, tlactivity, data_list2, data_headers)

        else:
            logfunc('No OLX Selling available')

__artifacts__ = {
    "OLX": (
        "OLX",
        ('ChatSellingDb.*'),
        get_olx_selling)
}
