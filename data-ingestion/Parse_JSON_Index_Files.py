'''
Created on Sep 15, 2015
@author: MatthewRubashkin
'''

import simplejson as json
import os

#Global Variables
key = "filename" #filename=WARC directory
Dir = ''
filename = 'wikipedia-json-index-data-page1-first100-150916'
filename_write = filename+'-'+key+'Only'

#Function to get out key value defined in global variable
def json_get_key_value(json_string,key):
    json_object = json.loads(json_string)
    json_parsed = json_object[key]
    return json_parsed

#Function to make a WAT location file fram a WARC location file
def json_make_WAT_from_WARC(json_string):
    json_string = json_string.replace('/warc/','/wat/')
    json_string = json_string.replace('warc.gz','warc.wat.gz')
    return json_string

#Function to make a WET location file fram a WARC location file
def json_make_WET_from_WARC(json_string):
    json_string = json_string.replace('/warc/','/wet/')
    json_string = json_string.replace('warc.gz','warc.wet.gz')
    return json_string
    

#Write parsed json value from file, specified by filename
with open(filename_write, 'w') as file:
    with open(filename, 'r') as f:
        json_files = f.readlines()
        for line in json_files:
            #Get WET file
            json_parsed=json_get_key_value(line,key)
            file.write(str(json_parsed)+'\n')
            #Get WAT file
            json_parsed_WAT=json_make_WAT_from_WARC(json_parsed)
            file.write(str(json_parsed_WAT)+'\n')
            #Get WET file
            json_parsed_WET=json_make_WET_from_WARC(json_parsed)
            file.write(str(json_parsed_WET)+'\n')
            
print 'end'


