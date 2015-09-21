'''
Created on Sep 15, 2015
@author: MatthewRubashkin
'''

import simplejson as json
import os

#Global Variables
key = "filename" #filename=WARC directory
filename = 'domain-wikipedia.org-'
rootdir = 'Wikipedia_CDX_index_results_January_2015'

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
def batch_WARC_file_destination(filename,rootdir):
    processed_directory = rootdir + '_processed'
    with open(processed_directory+'/'+filename, 'w+') as file: #file to write
        with open(rootdir+'/'+filename, 'r') as f: #file to read
            try:
                json_files = f.readlines()
                for line in json_files:
                    #Get WARC file
                    json_parsed=json_get_key_value(line,key)
                    file.write(str(json_parsed)+'\n')
            except:
                pass

def Iterator_for_WET_and_WAT_files(filename, rootdir):
    processed_directory = rootdir + '_processed'
    if not os.path.exists(processed_directory):
        os.makedirs(processed_directory)
    for dirs, subdirs, files in os.walk(rootdir):
        for file in files:
            if file != '.DS_Store':
                batch_WARC_file_destination(str(file),rootdir)
            print 'This file is complete: ', file

print 'running...'
Iterator_for_WET_and_WAT_files(filename, rootdir)

print 'end'

