import exceptions
from wsgiref import headers
from requests.exceptions import ConnectionError,ConnectTimeout,ReadTimeout
from PIL import Image
import io
import xml.etree.cElementTree as ET
import sys,requests
import time
import ntpath
import boto3
import pandas as pd
import datetime,time,math,os
import csv
import sys,requests
import httplib
from urllib3.exceptions import TimeoutError
import numpy as np

httplib.HTTPConnection._http_vsn = 11
httplib.HTTPConnection._http_vsn_str = 'HTTP/1.1'

# reload(sys)
# sys.setdefaultencoding('utf8')

file_name=sys.argv[1]
print(file_name)

sourceEncoding = "utf-8-sig"
targetEncoding = "utf-8"
source = open(file_name)
input_file="input_"+datetime.datetime.now().strftime('%Y-%m-%d')+".csv"
target = open(input_file, "w")
target.write(unicode(source.read(), sourceEncoding).encode(targetEncoding))
target.close()
dfs = pd.read_csv(input_file)
print  dfs

GoalID_list = dfs["Goal ID"].tolist()
Provider_list=dfs["Provider"].tolist()

print(GoalID_list)
print (Provider_list)

for Hotel_Id,Provider in zip(GoalID_list,Provider_list):
    fname_xml='CS_Image_Response_'+Provider+'_'+str(Hotel_Id)+'.xml'
    tree = ET.parse(fname_xml)
    root = tree.getroot()

    print Hotel_Id
    URLs=[]
    sequence=[]
    Img_width=[]
    Img_height=[]
    HotelID=[]
    Providerr=[]
    temp_img_miss=[]
    miss_img=[]
    for seq,URL in enumerate((root.iter('URL')),start=1):
        URLs.append(URL.text)
        sequence.append(seq)
        print URL.text
        maxretries = 3
        attempt = 0

        while attempt < maxretries:
            try:
                image_content = requests.get(URL.text).content
                image_stream = io.BytesIO(image_content)
            except (httplib.IncompleteRead) as e:
                attempt += 1
                image_content = e.partial
            except (ConnectionError , TimeoutError, requests.Timeout, ConnectTimeout, ReadTimeout):
                attempt += 1
            else:
                break
        try:
            img = Image.open(image_stream)
            Img_width.append(img.size[0])
            Img_height.append(img.size[1])
        except IOError:
            Img_width.append('Image Missing')
            Img_height.append('Image Missing')
        continue
    temp_img_miss=zip(URLs,Img_width,Img_height)

    Missing_Image_URL = list(tup for tup in temp_img_miss if tup[1] == 'Image Missing')
    print Missing_Image_URL
    print Img_width
    print Img_height
    xml_file=open(fname_xml, "r")
    file_contents= xml_file.read()

    tree1=ET.fromstring(str(file_contents))

    for i in range(len(tree1)):
        HotelID.append(tree1[i].attrib['GoalId'])
        Providerr.append(tree1[i].attrib['SupplierCode'])

    print HotelID
    print Providerr
    print URLs
    print sequence
    print zip(sequence,URLs,Img_width,Img_height)

    def Enquiry(lis1):
        if len(lis1) == 0:
            return 0
        else:
            return 1

    if Enquiry(URLs):
        Hotel=HotelID[0]
        providerx=Providerr[0]
        print(Hotel)
        lenght=len(sequence)
        print(len(sequence))
        import numpy as np
        toadys_date=datetime.datetime.now().strftime('%Y-%m-%d')
        percentile_list = pd.DataFrame(np.column_stack([URLs, Img_width, Img_height]),columns=['PW_URL', 'Img_width', 'Img_height'])
        percentile_list = percentile_list.assign(HotelID =[Hotel]*lenght,Provider=[providerx]*lenght,Search_date=[toadys_date]*lenght).reset_index(drop=True)
        print(percentile_list)

        token_url = "https://tui-nonprod-fm-test.apigee.net/oauth/client_credential/accesstoken"
        token_querystring = {"grant_type":"client_credentials"}
        token_payload = "client_id=AOcedfnKaNcuOQAkXgz5st48jllrj3dX&client_secret=GZ6GAKWDxABbM3AO"
        token_headers = {
            'content-type': "application/x-www-form-urlencoded",
            'cache-control': "no-cache"
        }

        maxretries = 3
        attempt = 0
        while attempt < maxretries:
            try:
                token_response = requests.request("POST", token_url, data=token_payload, headers=token_headers, params=token_querystring)
                print(token_response.text)
                time.sleep(60)
            except (httplib.IncompleteRead) as e:
                attempt += 1
                token_response = e.partial
            except (ConnectionError , TimeoutError, requests.Timeout, ConnectTimeout, ReadTimeout):
                attempt += 1
            else:
                break

        goodhotel_url = "https://tui-nonprod-fm-test.apigee.net/goodhotelservice"
        goodhotel_payload = "{\"urls\":"+str(URLs).replace("'","\"")+"}"
        goodhotel_headers = {
            'authorization': "Bearer "+token_response.json().get('access_token'),
            'cache-control': "no-cache"
        }


        maxretries = 3
        attempt = 0
        while attempt < maxretries:
            try:
                goodhotel_response = requests.request("POST", goodhotel_url, data=goodhotel_payload, headers=goodhotel_headers)
                print(goodhotel_response.text)
                time.sleep(60)
            except (httplib.IncompleteRead) as e:
                attempt += 1
                goodhotel_response = e.partial
            except (ConnectionError , TimeoutError, requests.Timeout, ConnectTimeout, ReadTimeout):
                attempt += 1
            else:
                break


        maxretries = 3
        attempt = 0
        while attempt < maxretries:
            try:
                goodhotel_response_url = goodhotel_response.json().get('GetResults')
                time.sleep(60)
            except (httplib.IncompleteRead) as e:
                attempt += 1
                goodhotel_response_url = e.partial
            except (ConnectionError , TimeoutError, requests.Timeout, ConnectTimeout, ReadTimeout):
                attempt += 1
            else:
                break

        goodhotel_response_headers = {
        'authorization': "Bearer "+token_response.json().get('access_token'),
        'cache-control': "no-cache"
        }

        maxretries = 3
        attempt = 0
        while attempt < maxretries:
            try:
                goodhotel_response_output = requests.request("GET", goodhotel_response_url, headers=goodhotel_response_headers)
                time.sleep(60)
                print(goodhotel_response_output.text)
            except (httplib.IncompleteRead) as e:
                attempt += 1
                goodhotel_response_output = e.partial
            except (ConnectionError , TimeoutError, requests.Timeout, ConnectTimeout, ReadTimeout):
                attempt += 1
            else:
                break
    # print(goodhotel_response_output.text)

    # Reading Json into Dataframe and droping some columns-------------------------------------------------------------------
        import pandas as pd
        df_json = pd.read_json(goodhotel_response_output.content)
        print (df_json)

        if not df_json.empty:
            import numpy as np
            dfinal = pd.merge(percentile_list, df_json, left_on='PW_URL', right_on='url', how='left')
            data1=dfinal.drop(['developerEmail', 'imageID','rawLabels'], axis=1)
            data1["is_duplicate"]= data1['PW_URL'].duplicated()
            data1.index = np.arange(1,len(data1)+1)
            print (data1)
            filename='CS_Image_Response_Converted_'+Provider+'_'+str(Hotel_Id)+'.csv'
            data1.to_csv(filename)

    else:
        import numpy as np
        toadys_date=datetime.datetime.now().strftime('%Y-%m-%d')
        data = [{'PW_URL':'NULL','Img_width':'NULL','Img_height':'NULL','HotelID':'NULL','Provider':'NULL','Search_date':toadys_date,'classification':'NULL','jobID':'NULL','url':'NULL','is_duplicate':'NULL'}]
        df_json = pd.DataFrame(data)
        df_json = df_json[['PW_URL','Img_width','Img_height','HotelID','Provider','Search_date','classification','jobID','url','is_duplicate']]
        df_json.index = np.arange(1,len(df_json)+1)
        print(df_json)
        filename='CS_Image_Response_Converted_'+Provider+'_'+str(Hotel_Id)+'.csv'
        df_json.to_csv(filename)

        print("Empty URLs")

    # s3 Upload----------------------------------------------------------------------------------------------------------------
    import subprocess
    def subprocess_cmd(command):
        process = subprocess.Popen(command,stdout=subprocess.PIPE, shell=True)
        proc_stdout = process.communicate()[0].strip()
        print proc_stdout

    commandx="s3cmd put --recursive --force /tmp/Content_scripts/"+filename+" "+"s3://qubole-poc-new-markets/content_integration/Content_Integration_Responses/"+datetime.datetime.now().strftime('%Y-%m-%d')+"/Image_Response_csv/"

    commandy="s3cmd put --recursive --force /tmp/Content_scripts/"+filename+" "+"s3://qubole-poc-new-markets/content_integration/Content_Integration_Responses/External_table/Image/"

    subprocess_cmd(commandx)

    subprocess_cmd(commandy)

    # END-------------------------------------------------------------------------------------------------------------------



