import sys,requests
import exceptions
import boto3, botocore
import datetime,time,math,os
import httplib
import xml.etree.ElementTree as ET
import pandas as pd
import json
import datetime,time,math,os

httplib.HTTPConnection._http_vsn = 11
httplib.HTTPConnection._http_vsn_str = 'HTTP/1.1'
AUTH_KEY = "6779daa9ad1816361e05628483d89eb"
Img_recgn_url="http://35.187.109.239:9999/StoreCMS/api/images/all"
Description_url="http://35.187.109.239:9999/StoreCMS/api/descriptive/"
headers = {'content-type': 'application/xml'}
# s3Path='s3://qubole-poc-new-markets/content_integration'

file_name=sys.argv[1]
dfs = pd.read_csv(file_name,encoding='utf-8-sig')

GoalID_list = dfs["Goal ID"].tolist()
Provider_list=dfs["Provider"].tolist()

print(GoalID_list)
print (Provider_list)

for Hotel_Id,Provider in zip(GoalID_list,Provider_list):
	CS_Img_Req = """<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
			<ContentSearchRequest >
				<Content>
					<Products>
						<Product Supplier='"""+Provider+"""'/>
					</Products>
					<GoalObjects>
						<GoalObject Id='"""+str(Hotel_Id)+"""'>
							<Suppliers>
								<Supplier>HOB</Supplier>
							</Suppliers>
						</GoalObject>
					</GoalObjects>
				</Content>
			</ContentSearchRequest>"""
	print ('ContentSearch Image Request for Hotel ID' +str(Hotel_Id)+':'+CS_Img_Req)
	#print (CS_Img_Req)
	maxretries = 3
	attempt = 0
	while attempt < maxretries:
		try:
			CS_Img_Resp = requests.post(Img_recgn_url,data=CS_Img_Req,headers=headers)
			# CS_Img_Resp = requests.Request('POST',url, data=CS_Img_Req ,headers=headers)
		except (httplib.IncompleteRead) as e:
			attempt += 1
			CS_Img_Resp = e.partial
		except (exceptions):
			attempt += 1
		else:
			break
	#print(str(CS_Img_Resp))
	flg1 = 0
	try:
		if str(CS_Img_Resp):
			flg1 = 1
	except:
		flg1 = 0
	if flg1:
		# print(str(CS_Img_Resp))
		print ('Writing the response for Hotel ID '+str(Hotel_Id)+' to a file\n')
		fname_xml = 'CS_Image_Response_'+Provider+'_'+str(Hotel_Id)+'.xml'
		xml_file=open(fname_xml,"w")
		xml_file.write(CS_Img_Resp.text)
		xml_file.close()
		print ('Writing Completed')
		# print ('\nGetting All the Image URLs:\n')
		# xml_file = open(fname_xml,"r")
		# tree = ET.parse("/tmp/"+fname_xml)
		# xml_file.close()
		# root = tree.getroot()
		# URLs=[]
		# for URL in root.iter('URL'):
		# 	URLs.append(URL.text)
		# print('\nPrinting List of Image URLs:\n',URLs)
		print ('\n\nStarting to Upload ContentSearch Image Response for'+str(Hotel_Id)+'_'+Provider+' on S3')
		s3Path='content_integration/Content_Integration_Responses/' + datetime.datetime.now().strftime('%Y-%m-%d')+'/Image_Response/'+fname_xml
		s3 = boto3.resource('s3',region_name='eu-central-1')
		s3.meta.client.upload_file("/tmp/Content_scripts/"+fname_xml,"qubole-poc-new-markets", s3Path )
		print ('\nContentSearch Image Response for'+str(Hotel_Id)+'_'+Provider+' uploaded on S3')
	print ('\nContentSearch Image Response for'+str(Hotel_Id)+'_'+Provider+':\n'+str(CS_Img_Resp)+'\n')

	# Image Search End--------------------------------------------------------------------------------------------------

	CS_Description_Req = """<?xml version="1.0" encoding="UTF-8"?>
<ContentSearchRequest>
   <Content>
      <Products>
         <Product Supplier='"""+Provider+"""'>
            <Rooms>
               <Room RoomCode="ST04-DO" />
            </Rooms>
         </Product>
      </Products>
      <GoalObjects>
         <GoalObject Id='"""+str(Hotel_Id)+"""'>
            <Suppliers>
               <Supplier>HOB</Supplier>
            </Suppliers>
         </GoalObject>
      </GoalObjects>
   </Content>
   <Languages>
       <Language Language="en" />  
    </Languages>
</ContentSearchRequest>"""
	print ('ContentSearch Description Request for Hotel ID' +str(Hotel_Id)+':'+CS_Description_Req)
	#print (CS_Description_Req)
	maxretries = 3
	attempt = 0
	while attempt < maxretries:
		try:
			CS_Description_Resp = requests.post(Description_url,data=CS_Description_Req,headers=headers)

		except (httplib.IncompleteRead) as e:
			attempt += 1
			CS_Description_Resp = e.partial
		except (exceptions):
			attempt += 1
		else:
			break
	#print(str(CS_Description_Resp))
	flg1 = 0
	try:
		if str(CS_Description_Resp):
			flg1 = 1
	except:
		flg1 = 0
	if flg1:
		# print(str(CS_Description_Resp))
		print ('Writing the response for Hotel ID '+str(Hotel_Id)+' to a file\n')
		fname_xml = 'CS_Description_Response_'+Provider+'_'+str(Hotel_Id)+'.xml'
		xml_file=open(fname_xml,"w")
		xml_file.write(CS_Description_Resp.content)
		xml_file.close()
		print ('Writing Completed')
		# print ('\nGetting All the Image URLs:\n')
		# xml_file = open(fname_xml,"r")
		# tree = ET.parse("/tmp/"+fname_xml)
		# xml_file.close()
		# root = tree.getroot()
		# URLs=[]
		# for URL in root.iter('URL'):
		# 	URLs.append(URL.text)
		# print('\nPrinting List of Image URLs:\n',URLs)
		print ('\n\nStarting to Upload ContentSearch Description Response for'+str(Hotel_Id)+'_'+Provider+' on S3')
		s3Path='content_integration/Content_Integration_Responses/' + datetime.datetime.now().strftime('%Y-%m-%d')+'/Description_Response/'+fname_xml
		s3 = boto3.resource('s3',region_name='eu-central-1')
		s3.meta.client.upload_file("/tmp/Content_scripts/"+fname_xml,"qubole-poc-new-markets", s3Path )
		print ('\nContentSearch Description Response for'+str(Hotel_Id)+' uploaded on S3')
	# print ('\nContentSearch Description Response for'+str(Hotel_Id)+'_'+Provider+':\n'+CS_Description_Resp.content+'\n')
