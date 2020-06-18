import sys
from argparse import ArgumentParser,FileType
import random
import json
from datetime import datetime, timedelta
from random import randint,randrange,choice

"""Default Activity Status List"""
default_activity_status =  ["Open" ,"Closed", "Resolved", "Waiting for Customer", "Waiting for Third Party" ,"Pending"]

"""Default random Dates"""
d1 = datetime.strptime('2020-01-01 00:00:00 +0000', '%Y-%m-%d %H:%M:%S +0000')
d2 = datetime.strptime('2020-03-30 23:59:59 +0000', '%Y-%m-%d %H:%M:%S +0000')

#performed_at_time = datetime.strptime(str(datetime.utcfromtimestamp), '%Y-%m-%d %H:%M:%S %z')


"""Default performer id"""
performer_id = 149018
MAXSIZE = 1000

def metadata_value(Startdt,enddt,activities_count):
    """
    This function will returns a metadata Schema for the file.
    """
    return {
        "start_at": str(Startdt),
        "end_at" : str(enddt),
        "activities_count": activities_count
}

def random_date(start, end):
    """
    This function will return a random datetime between two datetime 
    objects.
    """
    delta = end - start 
    int_delta = (delta.days * 24 * 60 * 60) + delta.seconds
    random_second = randrange(int_delta)
    return start + timedelta(seconds=random_second)

def random_activity(d1,d2):
    """
    This function generates random activity details i.e. Note or a phone based activity 
    """
    return [
                        {
                          "note" : {   
                              "id": randint(4000, 5000000),
                              'type': randint(1, 4)}
                        },
                        {
                            "shipping_address": "N/A", 
                            "shipment_date": str(random_date(d1,d2)),
                            "category": choice(['Phone','Text']), # change be phone or text
                            "contacted_customer": choice([True,False]), 
                            "issue_type": "Incident", # default to Incident
                            "source": randint(1,4), # Random 1-4
                            "status": choice(default_activity_status), # Random  pick from default_activity_status
                            "priority": randint(1,4), # Random 1-4
                            "group": "refund", # default to refund
                            "agent_id": performer_id, # default random number
                            "requester": randint(14000,146000), # default random number
                            "product": "mobile" # default mobile
                        }
                        ]
def activities_data_inline(ticketId):
    """
    This function generates random activity  
    """
    return {
                "performed_at": datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S +0000'),
                "ticket_id": ticketId,
                "performer_type": "user", #default performer type is user
                "performer_id": performer_id, 
                "activity" : choice(random_activity(d1,d2))
    }

def main():
    try:
        start_at = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S +0000')
        parser = ArgumentParser()
        parser.add_argument("-n", "--no_of_activities", help="Specify no of data to be generated", type=int,required=True)
        parser.add_argument("-o", "--output_json_file", help="Specify name of json file for the output", type=FileType('w'),default=sys.stdout,required=True)  
        args = parser.parse_args()
        no_of_activities = args.no_of_activities
        outfile = args.output_json_file
        outfile.write('{"activities_data": [')
        iter=0
        totalactivities = ''
        for i in range(no_of_activities):
            totalactivities = json.dumps(activities_data_inline(randint(600,610))) + "," +  totalactivities   
            if(iter==MAXSIZE): #iteraton to limit the memory buffer and IO
                    outfile.write(totalactivities)
                    iter=0
                    totalactivities = ''
            else:
                iter+=1 
        outfile.write(totalactivities[:-1])
        outfile.write('],')

        """Generating metadata"""

        end_at = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S +0000')
        metadata= json.dumps(metadata_value(start_at,end_at,no_of_activities))
        outfile.write(' "metadata":' + metadata)
        outfile.write('}')
        
    except Exception as e:
        print(str(e))
        print("usage : python .\\ticket-gen.py -n <<No of Activities>> -o <<.json filename>")

if __name__ == "__main__":  
    main()