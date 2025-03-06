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
    # data_list1 = []
    logfunc("Processing data for OLX Selling")
    files_found = [x for x in files_found if not x.endswith('wal') and not x.endswith('shm')]
    file_found = str(files_found[0])
    db = open_sqlite_db_readonly(file_found)
    cursor = db.cursor()
    cursor.execute('''
    SELECT
    datetime(`timestamp`, 'unixepoch') AS created_time,
    `id`,
    `ad`,
    `respondent`,
    `messages`
        from ChatListItem
      /* where id = respondent_id and json_valid(respondent) = 1 order by created_time /*
        ''')

    all_rows = cursor.fetchall()
    usageentries = len(all_rows)
    data_list = []

    if usageentries > 0:
        logfunc(f"Found OLX {usageentries} Entries")
        for row in all_rows:
            data_list.append((row[0], row[1], row[2], row[3], row[4], row[5], row[6], row[7], row[8], row[9], row[10], row[11], row[12], row[13]))

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
        ('ChatSellingDb.db'),
        get_olx_selling)
}
