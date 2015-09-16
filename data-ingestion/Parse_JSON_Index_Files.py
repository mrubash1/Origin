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

#Write parsed json value from file, specified by filename
with open(filename_write, 'w') as file:
    with open(filename, 'r') as f:
        json_files = f.readlines()
        for line in json_files:
            json_parsed=json_get_key_value(line,key)
            file.write(str(json_parsed)+'\n')
            #print json_parsed+'\n'
            #data.append(key_value)
print 'end'
        
