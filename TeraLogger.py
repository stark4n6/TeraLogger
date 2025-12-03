import argparse
import csv
import glob
import os
import sqlite3
import sys
import time
import urllib.parse

# --- MAPPING DICTIONARIES ---

STATE_MAP = {0: 'Added', 1: 'OK', 2: 'Verified', 3: 'Error', 4: 'Skipped', 5: 'Deleted', 6: 'Moved'}
IS_FOLDER_MAP = {0: 'No', 1: 'Yes'}
MARKED_MAP = {0: '', 1: 'Yes'}
HIDDEN_MAP = {0: '', 1: 'Yes'}
OPERATION_MAP = {1: 'Copy', 2: 'Move', 3: 'Test', 6: 'Delete'}

# --- SQL QUERIES (Simplified/Optimized) ---

sql_query_history = '''
SELECT
    Source,
    State,
    Size,
    IsFolder,
    datetime(julianday(Creation)),
    datetime(julianday(Access)),
    datetime(julianday(Write)),
    SourceCRC,
    TargetCRC,
    Message,
    Marked,
    Hidden
FROM Files
'''

sql_query_history_log = 'SELECT Timestamp, Message FROM Log'

sql_query_main = '''
SELECT
    Name AS "name",
    datetime(julianday(Started)) AS "job_start",
    datetime(julianday(Finished)) AS "job_end",
    Operation AS "operation",
    Source AS "src_path",
    Target AS "target_path"
FROM list
'''

# --- ASCII ART ---
ascii_art = r''' 
  _______             _                                
 |__   __|           | |                              
    | | ___ _ __ __ _| |     ___   __ _  __ _  ___ _ __ 
    | |/ _ \ '__/ _` | |    / _ \ / _` |/ _` |/ _ \ '__|
    | |  __/ | | (_| | |___| (_) | (_| | (_| |  __/ |    
    |_|\___|_|  \__,_|______\___/ \__, |\__, |\___|_|    
                                   __/ | __/ |            
                                  |___/ |___/            
  
TeraLogger v1.0
https://github.com/stark4n6/TeraLogger
'''

def is_platform_windows():
    return os.name == 'nt'

def _ensure_windows_long_path(raw_path: str) -> str:
    path = os.path.abspath(raw_path).replace('/', '\\')
    if path.startswith('\\\\?\\'):
        return path
    if path.startswith('\\\\'):
        return '\\\\?\\UNC' + path[1:]
    return '\\\\?\\' + path

def open_sqlite_db_readonly(path):
    if is_platform_windows():
        normalized = _ensure_windows_long_path(path)
    else:
        normalized = os.path.abspath(path)

    uri_path = urllib.parse.quote(normalized)
    return sqlite3.connect(f"file:{uri_path}?mode=ro", uri=True)

def does_table_exist(connection, table_name):
    try:
        cursor = connection.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?;", (table_name,))
        return cursor.fetchone() is not None
    except sqlite3.DatabaseError:
        return False

def iterate_folder_sqlite3_files(history_folder_path):
    pattern = os.path.join(history_folder_path, '*.db')
    for sqlite3_file_path in glob.glob(pattern):
        results = []
        results_log = []
        log_table_exists = False

        try:
            with open_sqlite_db_readonly(sqlite3_file_path) as connection:
                cursor = connection.cursor()
                cursor.execute("PRAGMA integrity_check")
                row = cursor.fetchone()
                if not row or row[0] != "ok":
                    print(f"Skipping {sqlite3_file_path} (failed integrity check: {row[0] if row else 'unknown'})")
                    continue

                # Files table
                try:
                    cursor.execute(sql_query_history)
                    results = cursor.fetchall()
                except sqlite3.DatabaseError as ex:
                    print(f"Error querying Files in {sqlite3_file_path}: {ex}")

                # Log table
                if does_table_exist(connection, 'Log'):
                    log_table_exists = True
                    try:
                        cursor.execute(sql_query_history_log)
                        results_log = cursor.fetchall()
                    except sqlite3.DatabaseError as ex:
                        print(f"Error querying Log in {sqlite3_file_path}: {ex}")

        except Exception as ex:
            print(f"Error opening {sqlite3_file_path}: {ex}")

        yield sqlite3_file_path, results, results_log, log_table_exists

def log_to_file(log_path, message):
    ts = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()) + f".{int((time.time()%1)*1000):03d}"
    with open(log_path, "a", encoding="utf-8") as lf:
        lf.write(f"[{ts}] {message}\n")

def main():
    start_time = time.time()
    print(ascii_art)

    parser = argparse.ArgumentParser()
    parser.add_argument('-i', '--input_path', required=True, type=str, help='Input file/folder path')
    parser.add_argument('-o', '--output_path', required=True, type=str, help='Output folder path')
    args = parser.parse_args()

    input_path = os.path.abspath(args.input_path)
    output_path = os.path.abspath(args.output_path)

    if not os.path.exists(input_path):
        parser.error("Input folder does not exist.")

    # Apply long path handling
    if is_platform_windows():
        input_path = _ensure_windows_long_path(input_path)
        if not input_path.endswith('\\'): input_path += '\\'
        output_path = _ensure_windows_long_path(output_path)
        if not output_path.endswith('\\'): output_path += '\\'
        splitter = '\\'
    else:
        if not input_path.endswith('/'): input_path += '/'
        if not output_path.endswith('/'): output_path += '/'
        splitter = '/'

    # Strip long path prefix for display purposes
    display_input = input_path.replace('\\\\?\\','')
    display_output = output_path.replace('\\\\?\\','')
    print(f"Source: {display_input}")
    print(f"Destination: {display_output}\n")

    data_headers = (
        'Job Start','Job End','Job Type','Source File Path','Source Folder','Destination Folder',
        'Status','File Size','Is Folder','File Creation Date','File Access Date','File Write Date',
        'Source CRC','Target CRC','Message','Marked','Hidden','Job File Path'
    )
    data_headers_log = ('Timestamp','Message','Source')

    base = 'TeraLogger_Out_'
    data_list = []
    data_list_log = []
    main_dict = {}
    count = 0
    parsed_history = 0
    parsed_files_total = 0
    parsed_logs_total = 0

    # Output folder
    output_ts = time.strftime("%Y%m%d-%H%M%S")
    out_folder = os.path.join(output_path, base + output_ts) + splitter
    os.makedirs(out_folder, exist_ok=True)
    out_folder_str = out_folder.replace('\\\\?\\', '')

    # Log file
    log_path = os.path.join(out_folder, f"TeraLogger_RunLog_{output_ts}.txt")
    log_to_file(log_path, "TeraLogger run started.")
    log_path_str = log_path.replace('\\\\?\\', '')

    # Parse main.db
    raw_input_glob = input_path.replace('\\\\?\\','') if is_platform_windows() else input_path
    for main_file_path in glob.glob(os.path.join(raw_input_glob, "main.db")):
        try:
            with open_sqlite_db_readonly(main_file_path) as connection:
                cursor = connection.cursor()
                cursor.execute("PRAGMA integrity_check")
                row = cursor.fetchone()
                if row and row[0] == "ok":
                    cursor.execute(sql_query_main)
                    main_results = cursor.fetchall()
                    if main_results:
                        cols = [c[0] for c in cursor.description]
                        for r in main_results:
                            job_name = r[0]
                            main_dict[job_name] = {cols[i]: r[i] for i in range(len(cols))}
                        print("Main.db processed.\n")
                        log_to_file(log_path, "Main.db processed successfully.")
                    else:
                        print("Main.db found but contains no jobs.\n")
                        log_to_file(log_path, "Main.db contained no jobs.")
        except Exception as ex:
            print(f"Error processing main.db: {ex}")
            log_to_file(log_path, f"Error processing main.db: {ex}")

    # Process History DB files
    history_folder_path = input_path + "History" + splitter
    for sqlite3_file_path, results, results_log, log_exists in iterate_folder_sqlite3_files(history_folder_path):

        parsed_history += 1
        basename = os.path.split(sqlite3_file_path)[1]

        print(f"Processed history file: {basename}")
        log_to_file(log_path, f"Processed history DB: {basename}")

        job_info = main_dict.get(basename)
        if job_info:
            job_start = job_info.get("job_start")
            job_end = job_info.get("job_end")
            src_path = job_info.get("src_path")
            target_path = job_info.get("target_path")
            raw_op = job_info.get("operation")
            operation = OPERATION_MAP.get(raw_op, f"Unknown ({raw_op})")
        else:
            job_start = job_end = operation = src_path = target_path = "N/A"

        if results:
            parsed_files_total += len(results)
            for row in results:
                og_file_path = row[0]
                file_state = STATE_MAP.get(row[1], f"Unknown ({row[1]})")
                is_folder = IS_FOLDER_MAP.get(row[3], f"Unknown ({row[3]})")
                marked = MARKED_MAP.get(row[10], f"Unknown ({row[10]})")
                hidden = HIDDEN_MAP.get(row[11], f"Unknown ({row[11]})")
                og_db_path = sqlite3_file_path.replace('\\\\?\\','')

                data_list.append((
                    job_start, job_end, operation,
                    og_file_path, src_path, target_path,
                    file_state, row[2], is_folder,
                    row[4], row[5], row[6],
                    row[7], row[8], row[9],
                    marked, hidden, og_db_path
                ))
        else:
            print(f"History file {basename} has no rows in Files table.")
            log_to_file(log_path, f"Files table empty in: {basename}")

        # Process Log
        if log_exists:
            print(f"Processed Log for: {basename}")
            log_to_file(log_path, f"Processed Log table for: {basename}")

            if results_log:
                parsed_logs_total += len(results_log)
                for row in results_log:
                    data_list_log.append((row[0], row[1], basename))
        else:
            print(f"{basename} has no Log table.")
            log_to_file(log_path, f"No Log table found in: {basename}")

    # Output TSVs
    try:
        tsv_files_path = os.path.join(out_folder, f"TeraLogger_Teracopy_History_Files_{output_ts}.tsv")
        with open(tsv_files_path, "w", encoding="utf-8", newline="") as f:
            w = csv.writer(f, delimiter="\t")
            w.writerow(data_headers)
            w.writerows(data_list)

        if data_list_log:
            tsv_log_path = os.path.join(out_folder, f"TeraLogger_Teracopy_History_Log_{output_ts}.tsv")
            with open(tsv_log_path, "w", encoding="utf-8", newline="") as f:
                w = csv.writer(f, delimiter="\t")
                w.writerow(data_headers_log)
                w.writerows(data_list_log)
            log_to_file(log_path, "Log TSV file written successfully.")
        else:
            log_to_file(log_path, "Skipping Log TSV file output (0 log entries found).")
    except Exception as ex:
        print(f"Error writing TSVs: {ex}")
        log_to_file(log_path, f"Error writing TSVs: {ex}")

    # Summary
    print("\n===== SUMMARY =====")
    print(f"History DB files processed: {parsed_history}")
    print(f"Total file entries parsed: {parsed_files_total}")
    print(f"Total log entries parsed: {parsed_logs_total}")
    print(f"Output folder: {out_folder_str}")
    print(f"Log file: {log_path_str}")
    print(f"Time: {time.time() - start_time:.2f} sec")

    log_to_file(log_path, "TeraLogger run completed.")
    print("\n****JOB FINISHED****")


if __name__ == "__main__":
    main()