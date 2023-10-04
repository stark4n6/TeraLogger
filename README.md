# TeraLogger
<p align="center">
  <img width="300" height="300" src="https://github.com/stark4n6/TeraLogger/blob/main/Artwork/TeracopyLogo.jpg">
</p>

TeraLogger is a simple script to parse the history log files from TeraCopy. It does some correlation between databases found at the paths:

The main database that keeps records of all jobs (see my [blog post](https://www.stark4n6.com/2018/11/teracopy-forensic-analysis-part-1.html) for details).
```
C:\Users\<USERNAME>\AppData\Roaming\Teracopy\main.db
```

Each job gets it's own History database file. Inside is a listing of all files that were touched in the job (see my [blog post](https://www.stark4n6.com/2018/11/teracopy-forensic-analysis-part-2.html) for details).
```
C:\Users\<USERNAME>\AppData\Roaming\Teracopy\History\*.db
```

## Usage
The script is simple in that it only takes two things, an input path of the TeraCopy folder (-i) and the output folder of where you'd like the export report made (-o).

I would recommend collecting the whole TeraCopy folder from the user's AppData folder, either via the [KAPE target](https://github.com/stark4n6/KapeFiles/blob/master/Targets/Apps/TeraCopy.tkape) or some other forensic sound means.

### Help
```
  _____                  _
 |_   _|___  _ __  __ _ | |     ___    __ _   __ _   ___  _ __
   | | / _ \| '__|/ _` || |    / _ \  / _` | / _` | / _ \| '__|
   | ||  __/| |  | (_| || |___| (_) || (_| || (_| ||  __/| |
   |_| \___||_|   \__,_||_____|\___/  \__, | \__, | \___||_|
                                      |___/  |___/

        TeraLogger v0.0.1
        https://github.com/stark4n6/TeraLogger
        @KevinPagano3 | @stark4n6 | startme.stark4n6.com

usage: TeraLogger.py [-h] -i INPUT_PATH -o OUTPUT_PATH

TeraLogger v0.0.1 by @KevinPagano3 | @stark4n6 | https://github.com/stark4n6/TeraLogger

options:
  -h, --help            show this help message and exit
  -i INPUT_PATH, --input_path INPUT_PATH
                        Input file/folder path
  -o OUTPUT_PATH, --output_path OUTPUT_PATH
                        Output folder path
```

### To-Do List
Add main.db parser as second CSV file output
Error checks for missing files or incorrect paths