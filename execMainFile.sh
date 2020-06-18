#!/bin/bash
python ticket-gen.py -n 10000 -o out.json
python etl-sqllite.py -i out.json 

