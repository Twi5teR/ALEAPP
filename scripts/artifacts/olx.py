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
            ChatListItem.ID,
            ChatListItem.AD,
            ChatListItem.RESPONDENT,
            ChatListItem.MESSAGES
            FROM ChatListItem
            ORDER BY ID ASC''')
            logfunc("SQL query executed successfully")
            all_rows = cursor.fetchall()
            logfunc(f"Found Rows: {len(all_rows)}")
            #cursor.execute("PRAGMA table_info(ChatListItem);")
            #columns = cursor.fetchall()
            #logfunc(f"Columns in ChatListItem: {columns}")
            usageentries = len(all_rows)
            if usageentries > 0:
                logfunc(f"Found OLX {usageentries} Entries")
                report = ArtifactHtmlReport('OLX Selling')

            for row in all_rows:
                data_list.append((row[0], row[1], row[2], row[3]))
                db.close()
    if data_list:
                    report = ArtifactHtmlReport('OLX Selling')
                    report.start_artifact_report(report_folder, 'Selling Events')
                    report.add_script()
                    data_headers = ('ID', 'Ad ID', 'Respondent Name', 'Message ID')

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
