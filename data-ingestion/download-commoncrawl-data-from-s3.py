# Matthew Rubashkin 150916
# Input to Kafka from S3 Common Crawl Server

# tools to open WARC files from common crawl
import warc, boto, json, ast,re
from gzipstream import GzipStreamFile
from itertools import *
from boto.s3.key import Key

#create WARC file from s3 WARC like file
def create_WARC_file_from_Common_Crawl(file_name, pds):
  k=Key(pds)
  k.key=file_name
  return warc.WARCFile(fileobj=GzipStreamFile(k))

#Using URL descriptor, get text information form WARC file
def get_text_from_WET_file(WET_file_name, WARC, descriptor):
  list_of_json_files=[]
  counter=0
  total_records_counter=0
  file_name=WET_file_name.replace('/','_')+'.json'
  file = open(file_name, "w")
  for num, record in enumerate(WARC):
    url = record.header.get('warc-target-uri', None)
    if descriptor in str(url):
      text = record.payload.read()
      plaintext = re.sub(r'<.*?>', '', text)
      counter = counter + 1
      data = {}
      data['url'] = str(url)
      data['plaintext'] = str(plaintext)
      WET_json_data = json.dumps(data)
      print WET_json_data
      print
      list_of_json_files.append(WET_json_data)
      if type(WET_json_data) == str:
      	file.write(WET_json_data + '\n') # WRITE OUT TO FILE
  file.close()
  return list_of_json_files

def get_links_from_Messy_WAT_file_plaintext(url, plaintext):
	#Convert text to a dict type, making sure to capitalize true and false to avoid error
	plaintext_dict = ast.literal_eval((plaintext.replace('true','True')).replace('false','False')) #update later to one faster pass
	#print plaintext_dict
	#Variables for creating end json file
	url_base='http://en.wikipedia.org'
	analyze_wikipedia_only= True
	list_of_json_files = []
	data = {}
	data['url'] = str(url) #instantiate dict for data
	link_list = []
	try: #make sure that the WAT file acutally has links
		links = plaintext_dict["Envelope"]["Payload-Metadata"]["HTTP-Response-Metadata"]["HTML-Metadata"]["Links"]
		#base link to add - will only analyze wikipedia
		#data['links'] = link_list
		for link in links:
			link= link.values()[link.keys().index('url')]
			link_is_valid=False #instantiate first as false then modify if true
			if link[0:5] in '/wiki': #check that it is a hyperlink from wikipedia
				full_link=url_base+link
				#print full_link
				link_list.append(full_link)
				link_is_valid=True
			if link_is_valid == True:
				link_list.append(full_link)
				#print link_list
		#Construct json_data that includes URL and link
		data['links'] = link_list
		WAT_json_data = json.dumps(data)
		#print list_of_json_files
		print 'These are the links for: ', url
		return WAT_json_data
	except:
		pass
		print 'There are no links for', url 
	print #blank

#Using URL descriptor, get metadata information form WARC file
def process_WAT_file(WAT_file_name, WARC, descriptor):
	list_of_json_files=[]
	#Create file to write data
	file_name=WAT_file_name.replace('/','_')+'.json'
	file = open(file_name, "w")
	#Process WAT file
	for num, record in enumerate(WARC):
		url = record.header.get('warc-target-uri', None)
		if descriptor in str(url):
			plaintext = record.payload.read()
			WAT_json_data=get_links_from_Messy_WAT_file_plaintext(url, plaintext) # run function
			print WAT_json_data
			print type(WAT_json_data)
			list_of_json_files.append(WAT_json_data)
			if type(WAT_json_data) == str:
				file.write(WAT_json_data + '\n') # WRITE OUT TO FILE
	file.close()
	return list_of_json_files




#Connect to amazon S3 containing commoncrawl, no authenitcation necessary
conn = boto.connect_s3(anon=True, debug=2)
pds = conn.get_bucket('aws-publicdatasets')

#Insert file names WET: plain text; WAT: hyperlinks
WET_file_name = 'common-crawl/crawl-data/CC-MAIN-2015-06/segments/1422122086301.64/wet/CC-MAIN-20150124175446-00197-ip-10-180-212-252.ec2.internal.warc.wet.gz'
WAT_file_name = 'common-crawl/crawl-data/CC-MAIN-2015-06/segments/1422122086301.64/wat/CC-MAIN-20150124175446-00197-ip-10-180-212-252.ec2.internal.warc.wat.gz'
descriptor = "en.wikipedia"

#Open file for writing JSON files
#file_name=WET_file_name.replace('/','_')+'.json'
#file = open(file_name, "w")

#Run WAT
WARC_WAT=create_WARC_file_from_Common_Crawl(WAT_file_name,pds)
process_WAT_file(WAT_file_name,WARC_WAT,descriptor)
#Run WET
WARC_WET=create_WARC_file_from_Common_Crawl(WET_file_name,pds)
get_text_from_WET_file(WET_file_name, WARC_WET,descriptor)
