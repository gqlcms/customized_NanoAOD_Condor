import commands
import os

path = os.getcwd()+"/../tasks/"
files=os.listdir(path)
outputfiles = "../submit_all_file.sh"

output = ""
for i in files:
    
    # CondorTask_ST_s-channel_4f_leptonDecays_13TeV-amcatnlo-pythia8_TuneCUETP8M1_RunIISummer16NanoAODv7-PUMoriond17_Nano02Apr2020_102X_mcRun2_asymptotic_v8-v1_NANOAODSIM_test
    i = i.replace(" ","").replace("\n","")
    if "Condor" in i:
    	submit_str = "condor_submit tasks/"+i+"/submit.cmd"
    	print submit_str
    	output += submit_str+"\n"
    
with open(outputfiles,"w") as f:
    f.write(output)
    

