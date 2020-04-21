import subprocess
import datetime,time
import sys, getopt

def subprocess_cmd(command):
    process = subprocess.Popen(command,stdout=subprocess.PIPE, shell=True)
    proc_stdout = process.communicate()[0].strip()
    print proc_stdout

command_x1="rm -rf /tmp/Content_scripts"
command_x2="mkdir -p /tmp/Content_scripts"
command_x3="s3cmd get --recursive --force s3://qubole-poc-new-markets/content_integration/Content_Integration_Responses/Content_Scripts/ /tmp/Content_scripts/"
command_x4="s3cmd get --recursive --force s3://qubole-poc-new-markets/content_integration/Content_Integration_Responses/Input_file/"+sys.argv[1]+" /tmp/Content_scripts/"
command_x5="chmod +x /tmp/Content_scripts/*"
command_x6="cd /tmp/Content_scripts/; python ContentIntegration_final.py "+sys.argv[1]
command_x7="cd /tmp/Content_scripts/; python Content_Integration_Stage_Two.py "+sys.argv[1]


subprocess_cmd(command_x1)
time.sleep(10)
subprocess_cmd(command_x2)
time.sleep(10)
subprocess_cmd(command_x3)
time.sleep(20)
subprocess_cmd(command_x4)
time.sleep(20)
subprocess_cmd(command_x5)
time.sleep(10)
subprocess_cmd(command_x6)
time.sleep(60)
subprocess_cmd(command_x7)
time.sleep(30)
print("Task Complted")
