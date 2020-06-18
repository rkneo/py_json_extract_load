# py_json_extract_load
Python script to generate dummy JSON Data , extract the data and load it into SQLLITE

ticket-gen.py - Generates the JSON Dummy Data
etl-sqllite.py - Extracts and loads the JSON dummy data
SQLScript.sql - SQL to generate the following attributes for each ticket:
                ○ Time spent Open
                ○ Time spent Waiting on Customer
                ○ Time spent waiting for response (Pending Status)
                ○ Time till resolution
                ○ Time to first response
execMainFile.bat - Batch file for automating execution of above programs.
