import sys
import sqlite3
from sqlite3 import Error
from datetime import datetime, timedelta
from argparse import ArgumentParser,FileType
import json

def create_connection(db_file):
    """ create a database connection to a SQLite database """
    conn = None
    try:
        conn = sqlite3.connect(db_file)
        return conn
    except Error as e:
        print(e)
    
def create_tables(conn):
    """ Create tables for loading JSON Data"""
    
    metadata_table_sql = """CREATE TABLE IF NOT EXISTS metadata(
        json_file_name text NOT NULL,
        unqiue_load_id integer NOT NULL,
        start_at text NOT null,
        end_at text NOT NULL,
        activities_count integer,
        load_datetime text not null
        )"""
    activities_data_table_sql = """CREATE TABLE IF NOT EXISTS activities_data(
        json_file_name text NOT NULL,
        unqiue_load_id integer NOT NULL,
        activities_skey integer NOT NULL,
        performed_at text,
        ticket_id text ,
        performer_type text ,
        performer_id text,
        load_datetime  text not null
        )"""    
    activities_table_sql = """CREATE TABLE IF NOT EXISTS activity(
        json_file_name text NOT NULL,
        unqiue_load_id integer NOT NULL,
        activities_skey integer NOT NULL,
        note text,
        shipping_address text,
        shipment_date text,
        category text,
        contacted_customer integer,
        issue_type text,
        source integer,
        status text,
        priority text,
        groups text,
        agent_id integer,
        requester integer,
        product text,
        load_datetime  text not null
         )"""
    try:
        cur = conn.cursor()
        cur.execute(metadata_table_sql)
        cur.execute(activities_data_table_sql)
        cur.execute(activities_table_sql)
        cur.close()
    except Error as e:
        print(e)
        cur.close()
         
def generate_load_context(conn):
    load_datetime = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S +0000')
    cur = conn.cursor()
    for row in cur.execute('SELECT max(unqiue_load_id) unqiue_load_id FROM metadata'):
        """do nothing"""    

    """ identify new loads """
    if(row[0] is None):
        unqiue_load_id = 1 
    else:
        unqiue_load_id = row[0]+1
    
    cur.close()
    return load_datetime, unqiue_load_id

def extract_load_json_data(jsonfile,unqiue_load_id,load_datetime,conn):
    """Read Json file"""
    data = jsonfile.read()
    dataset = json.loads(data)
    dataframe_activities_data=[]
    dataframe_activity=[]
    dataframe_metadata=[]
    activity_id = 0 

    """extracting Metadata """
    datarow_metadata = (     
                            jsonfile.name,
                            unqiue_load_id,
                            dataset['metadata']['start_at'],
                            dataset['metadata']['end_at'],
                            dataset['metadata']['activities_count'],
                            load_datetime
                        )
    dataframe_metadata.append(datarow_metadata)


    """extracting activities_data """
    for row in dataset['activities_data']:
        datarow = (     
                        jsonfile.name,
                        unqiue_load_id,
                        activity_id,
                        str(row['performed_at']),
                        row['ticket_id'],
                        row['performer_type'],
                        row['performer_id'],
                        load_datetime
                    )
        dataframe_activities_data.append(datarow)

        """extracting activity """

        if 'note' in row['activity']:
            activitydatarow = (
                        jsonfile.name,
                        unqiue_load_id,
                        activity_id,
                        str(row['activity']['note']),
                        None,
                        None,
                        None,
                        None,
                        None,
                        None,
                        None,
                        None,
                        None,
                        None,
                        None,
                        None,
                        load_datetime
            )
        else:
             activitydatarow = (
                        jsonfile.name,
                        unqiue_load_id,
                        activity_id,
                        None,
                        row['activity']['shipping_address'],
                        row['activity']['shipment_date'],
                        row['activity']['category'],
                        row['activity']['contacted_customer'],
                        row['activity']['issue_type'],
                        row['activity']['source'],
                        row['activity']['status'],
                        row['activity']['priority'],
                        row['activity']['group'],
                        row['activity']['agent_id'],
                        row['activity']['requester'],
                        row['activity']['product'],
                        load_datetime
            )
        dataframe_activity.append(activitydatarow)
        activity_id += 1
    try:
        """Load Json Objects into DB"""
        cur = conn.cursor()
        cur.executemany('insert into activities_data values(?,?,?,?,?,?,?,?)',dataframe_activities_data)
        cur.executemany('insert into activity values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)',dataframe_activity)
        cur.executemany('insert into metadata values(?,?,?,?,?,?)',dataframe_metadata)
    except Exception as E:
        print('Error :', E)
    else:
        conn.commit()
        print('data inserted')    
    
def main():
    conn=create_connection(r"ticketHelpDeskSQLLite.db")
    create_tables(conn)
    parser = ArgumentParser()
    parser.add_argument("-i", "--json_file", help="Specify name of json file for the output", type=FileType('r'),default=sys.stdout,required=True)  
    args = parser.parse_args()
    outfile = args.json_file

    load_datetime, unqiue_load_id = generate_load_context(conn)
    extract_load_json_data(outfile,unqiue_load_id,load_datetime,conn)

if __name__ == '__main__':
    main()
    
    

    