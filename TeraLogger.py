import argparse
import csv
import glob
import os
import sqlite3
import sys
import time

ascii_art = '''  
  _____                  _                                     
 |_   _|___  _ __  __ _ | |     ___    __ _   __ _   ___  _ __ 
   | | / _ \| '__|/ _` || |    / _ \  / _` | / _` | / _ \| '__|
   | ||  __/| |  | (_| || |___| (_) || (_| || (_| ||  __/| |   
   |_| \___||_|   \__,_||_____|\___/  \__, | \__, | \___||_|   
                                      |___/  |___/             
    
        TeraLogger v0.0.2
        https://github.com/stark4n6/TeraLogger
        @KevinPagano3 | @stark4n6 | startme.stark4n6.com
                                                                     '''
def is_platform_windows():
    # Returns True if running on Windows
    return os.name == 'nt'

def open_sqlite_db_readonly(path):
    # Opens an sqlite db in read-only mode, so original db (and -wal/journal are intact)
    if is_platform_windows():
        if path.startswith('\\\\?\\UNC\\'): # UNC long path
            path = "%5C%5C%3F%5C" + path[4:]
        elif path.startswith('\\\\?\\'):    # normal long path
            path = "%5C%5C%3F%5C" + path[4:]
        elif path.startswith('\\\\'):       # UNC path
            path = "%5C%5C%3F%5C\\UNC" + path[1:]
        else:                               # normal path
            path = "%5C%5C%3F%5C" + path
    return sqlite3.connect(f"file:{path}?mode=ro", uri=True)

def main():
    # SQLite query for history dbs
    sql_query_history = """
    select 
    Source,
    case
        when State = 0 then 'Added'
        when State = 1 then 'OK'
        when State = 2 then 'Verified'
        when State = 3 then 'Error'
        when State = 4 then 'Skipped'
        when State = 5 then 'Deleted'
        when State = 6 then 'Moved'
    end,
    Size, 
    case 
        when IsFolder = 0 then 'No'
        when IsFolder = 1 then 'Yes'
    end, 
    datetime(julianday(Creation)), 
    datetime(julianday(Access)), 
    datetime(julianday(Write)), 
    SourceCRC, 
    TargetCRC,
    Message,
    case
        when Marked = 0 then ''
        when Marked = 0 then 'Yes'
    end, 
    case
        when Hidden = 0 then ''
        when Hidden = 0 then 'Yes'
    end
    from Files"""
    
    # SQLite query for main db
    sql_query_main = '''
    select
    Name AS "name",
    datetime(julianday(Started)) as "job_start",
    datetime(julianday(Finished)) as "job_end",
    source AS "src_path",
    target AS "target_path"
    from list
    '''        

    start_time = time.time()
    print(ascii_art)
    
    # Command line arguments
    parser = argparse.ArgumentParser(description='TeraLogger v0.0.2 by @KevinPagano3 | @stark4n6 | https://github.com/stark4n6/TeraLogger')
    parser.add_argument('-i', '--input_path', required=True, type=str, action="store", help='Input file/folder path')
    parser.add_argument('-o', '--output_path', required=True, type=str, action="store", help='Output folder path')
    
    args = parser.parse_args()
    
    input_path = args.input_path
    output_path = args.output_path
    
    if args.output_path is None:
        parser.error('No OUTPUT folder path provided')
        return
    else:
        output_path = os.path.abspath(args.output_path)
    
    if output_path is None:
        parser.error('No OUTPUT folder selected. Run the program again.')
        return
    
    if input_path is None:
        parser.error('No INPUT file or folder selected. Run the program again.')
        return
    
    if not os.path.exists(input_path):
        parser.error('INPUT folder does not exist! Run the program again.')
        return
    
    if not os.path.exists(output_path):
        parser.error('OUTPUT folder does not exist! Run the program again.')
        return  
    
    # File system extractions can contain paths > 260 char, which causes problems
    # This fixes the problem by prefixing \\?\ on each windows path.
    if is_platform_windows():
        if input_path[1] == ':': input_path = '\\\\?\\' + input_path.replace('/', '\\')
        if not input_path.endswith('\\'):
            input_path = input_path + '\\'        
        
        if output_path[1] == ':': output_path = '\\\\?\\' + output_path.replace('/', '\\')
        
        if not output_path.endswith('\\'):
            output_path = output_path + '\\'
    
    platform = is_platform_windows()
    if platform:
        splitter = '\\'
    else:
        splitter = '/'
    #-------------------------------   
    
    print('-'* (len('Source: '+ input_path)))
    print('Source: '+ input_path.replace('\\\\?\\',''))
    print('Destination: '+ output_path.replace('\\\\?\\',''))
    
    data_headers = ('Job Start','Job End','Source File Path','Source Folder','Destination Folder','Status','File Size','Is Folder','File Creation Date','File Access Date','File Write Date','Source CRC','Target CRC','Message','Marked','Hidden','Job File Path')
    
    base = 'TeraLogger_Out_'
    db_name = ''
    job_start = ''
    job_end = ''
    source_path = ''
    dest_path = ''
    main_dict = {}
    data_list = []
    value_list = []
    count = 0
        
    history_folder_path = input_path + 'History' + splitter
    
    output_ts = time.strftime("%Y%m%d-%H%M%S")
    out_folder = output_path + base + output_ts + splitter
    os.makedirs(out_folder)

    # Parsing main.db job database
    for main_file_path in glob.glob(f"{input_path}main.db"):
        try:
            connection = open_sqlite_db_readonly(main_file_path)
            # Execute the PRAGMA integrity check.
            cursor = connection.cursor()
            cursor.execute("PRAGMA integrity_check")
    
            # If the integrity check passes, execute the SQL query.
            if cursor.fetchone()[0] == "ok":
                cursor.execute(sql_query_main)
                main_results = cursor.fetchall()
                
                column_names = [row[0] for row in cursor.description]
    
                if main_results:
                    for row in main_results:
                        main_dict[row[0]] = {}
                        for i, column_name in enumerate(column_names):
                            main_dict[row[0]][column_name] = row[i]
                        
                    cursor.close()
                    connection.close()
            else:
                main_results = []
        except sqlite3.DatabaseError:
            # If the database is corrupted, skip the file.
            main_results = []
    
    for sqlite3_file_path, results in iterate_folder_sqlite3_files(history_folder_path, sql_query_history):
        folder, basename = os.path.split(sqlite3_file_path) 
        # Do something with the results of the SQL query, or skip the file if the database is corrupted.
        if results:     
            for row in results:
                og_file_path = row[0]
                file_state = row[1]
                file_size = row[2]
                is_folder = row[3]
                create_ts = row[4]
                access_ts = row[5]
                write_ts = row[6]
                source_crc = row[7]
                target_crc = row[8]
                message = row[9]
                marked = row[10]
                hidden = row[11]            
                
                og_db_path = sqlite3_file_path.replace('\\\\?\\','')
                
                for key in main_dict.keys():
                    if basename == key:
                        db_name = main_dict[key].get('name')
                        job_start = main_dict[key].get('job_start')
                        job_end = main_dict[key].get('job_end')
                        src_path = main_dict[key].get('src_path')
                        target_path = main_dict[key].get('target_path')
       
                        data_list.append((job_start,job_end,og_file_path,src_path,target_path,file_state,file_size,is_folder,create_ts,access_ts,write_ts,source_crc,target_crc,message,marked,hidden,og_db_path))
    
                count += 1
        else:
            print(f"Skipping file {sqlite3_file_path} because it is corrupted.")
    
    # Create CSV file output
    with open(out_folder + 'TeraLogger_Teracopy_History_' + output_ts +'.tsv', 'w', encoding="utf-8", newline='') as f_output:
        tsv_writer = csv.writer(f_output, delimiter='\t')
        tsv_writer.writerow(data_headers)
        for i in data_list:
            tsv_writer.writerow(i)
    
    print()
    print('****JOB FINISHED****')
    print(str(count) + ' entries found')

def iterate_folder_sqlite3_files(history_folder_path, sql_query_history):
    for sqlite3_file_path in glob.glob(f"{history_folder_path}/*.db"):
        try:
            connection = open_sqlite_db_readonly(sqlite3_file_path)
            # Execute the PRAGMA integrity check.
            cursor = connection.cursor()
            cursor.execute("PRAGMA integrity_check")

            # If the integrity check passes, execute the SQL query.
            if cursor.fetchone()[0] == "ok":
                cursor.execute(sql_query_history)
                results = cursor.fetchall()
            else:
                results = []
        except sqlite3.DatabaseError:
            # If the database is corrupted, skip the file.
            results = []

        # Close the cursor and connection.
        cursor.close()
        connection.close()

        # Yield the results of the SQL query, or an empty tuple if the database is corrupted.
        yield sqlite3_file_path, results
        
if __name__ == '__main__':
    main()