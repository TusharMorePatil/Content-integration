# -*- coding: utf-8 -*-
import sys, requests
import exceptions
import csv
#import boto3, botocore
import datetime, time, math, os
import xml.etree.ElementTree as ET
# from langdetect import detect
import pandas as pd
from langdetect import detect

import os
sys.path.append('absolute_path to your module.py file')


file_name= sys.argv[1]
print (file_name)
sourceEncoding = "utf-8-sig"
targetEncoding = "utf-8"
source = open(file_name)
input_file="input_"+datetime.datetime.now().strftime('%Y-%m-%d')+".csv"
target = open(input_file, "w")
target.write(unicode(source.read(), sourceEncoding).encode(targetEncoding))
target.close()
dfs = pd.read_csv(input_file)
print  dfs

# dfs = pd.read_csv(file_name)

GoalID_list = dfs["Goal ID"].tolist()
Provider_list=dfs["Provider"].tolist()

print(GoalID_list)
print (Provider_list)

for Hotel_Id,Providerx in zip(GoalID_list,Provider_list):
    fname_xml='CS_Description_Response_'+Providerx+'_'+str(Hotel_Id)+'.xml'
    tree = ET.parse(fname_xml)
    root = tree.getroot()
    #print(root)
    Descn=[]
    HotelID=[]
    Language=[]
    Provider=[]
    xml_file=open(fname_xml, "r")
    file_contents= xml_file.read()

    tree1=ET.fromstring(str(file_contents))

    for i in range(len(tree1)):
        HotelID.append(tree1[i].attrib['GoalId'])
        Provider.append(tree1[i].attrib['SupplierCode'])

    print("\nHotelIds:")
    print (HotelID)
    print (Provider)

    for Desc in root.iter('Text'):
        Descn.append(str(Desc.text.encode('utf-8')).strip())
        Language.append(detect(Desc.text).encode('utf-8'))

	print ("\nDescriptions:")
    print Descn
    print ("\nLanguages:")
    print Language

    def word_count(str):
		counts = dict()
		words = str.lower().split()
		for word in words:
			if word in counts:
				counts[word] += 1
			else:
				counts[word] = 1
                return counts

    print ("\nCalculating WordCounts:")
    WordCounts=[]
    TotalWordCount=[]
    for n in range(len(Descn)):
        WordCount_temp = word_count(Descn[n])
        WordCount = {word: count for word, count in WordCount_temp.items() if not word.isdigit()}
        WordCounts.append(WordCount)
        TotalWordCount.append(len(Descn[n].split()))

	print ("\nWordCounts:")
    print WordCounts
    print ("\nTotal Number of Words:")
    print TotalWordCount

    from bs4 import BeautifulSoup
    is_html_present=[]
    for x in Descn:
        flag= bool(BeautifulSoup(x, "html.parser").find())
        cleantext = BeautifulSoup(x, "html.parser")
        if flag is True:
            cleantext.text
            is_html_present.append(True)
            for idx, item in enumerate(Descn):
                if x in item:
                    Descn[idx] = cleantext.text
        else:
            is_html_present.append(False)

    print is_html_present

    # from bs4 import BeautifulSoup
    # is_html_present=[]
    # for x in Descn:
    #     flag= bool(BeautifulSoup(x, "html.parser").find())
    #     cleantext = BeautifulSoup(x, "html.parser")
    # if flag is True:
    #     cleantext.text
    #     is_html_present.append(True)
    # else:
    #     is_html_present.append(False)
    #
    # print is_html_present


    print ("\nWriting records to a CSV file:")
    fname_csv='CS_Description_Response_'+Providerx+'_'+str(Hotel_Id)+'.csv'
    with open(fname_csv, 'w') as csvfile:
        spamwriter = csv.writer(csvfile, delimiter='|', quoting=csv.QUOTE_MINIMAL)
        data = list(zip(HotelID,Provider,Language,Descn,TotalWordCount,WordCounts,is_html_present))
        for row in data:
            print row
            row = list(row)
            print row
            spamwriter.writerow(row)

    print ("\nWriting Completed.")

    toadys_date=datetime.datetime.now().strftime('%Y-%m-%d')
    colnames=['HotelID','Provider','Language','Descn','TotalWordCount','WordCounts','is_html_present']
    df = pd.read_csv(fname_csv,sep="|", names=colnames, engine='python',header=None)
    length=len(df.index)
    percentile_list = df.assign(Search_date=[toadys_date]*length).reset_index(drop=True)
    percentile_list.set_index('HotelID', inplace=True)
    print("\n\nDisplaying CSV File Contents:")
    print(percentile_list)

    filenamexyz='CS_Description_Converted_Response_'+Providerx+'_'+str(Hotel_Id)+'.csv'
    percentile_list.to_csv(filenamexyz,sep='|')

    import subprocess
    def subprocess_cmd(command):
        process = subprocess.Popen(command,stdout=subprocess.PIPE, shell=True)
        proc_stdout = process.communicate()[0].strip()
        print proc_stdout

    commandx="s3cmd put --recursive --force /tmp/Content_scripts/"+filenamexyz+" "+"s3://qubole-poc-new-markets/content_integration/Content_Integration_Responses/"+datetime.datetime.now().strftime('%Y-%m-%d')+"/Description_Response_csv/"

    commandy="s3cmd put --recursive --force /tmp/Content_scripts/"+filenamexyz+" "+"s3://qubole-poc-new-markets/content_integration/Content_Integration_Responses/External_table/Description_v1/"

    subprocess_cmd(commandx)

    subprocess_cmd(commandy)
