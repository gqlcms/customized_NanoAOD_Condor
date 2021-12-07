import os
import commands

from metis.Sample import DBSSample
from metis.LocalMergeTask import LocalMergeTask
from CondorTask_V2 import CondorTask_V2 as CondorTask
from metis.StatsParser import StatsParser
import samples
import argparse

import time
from time import sleep
import sys


# Avoid spamming too many short jobs to condor
# Less dileptn pairs = faster = more input files per job
def split_func(dsname):
    if "Run201" in dsname:
        return 1
    else:
        return 1
    # TODO: To be implemented later
    # if any(x in dsname for x in [
    #     "/W","/Z","/TTJets","/DY","/ST",
    #     ]):
    #     return 5
    # elif "Run201" in dsname:
    #     return 7
    # else:
    #     return 2


 
def Create_Submit_Scripts(data_mc = False):
    path = condorpath+"/tasks/"
    files=os.listdir(path)
    # outputfiles = "submit_"+time.strftime("y%Y_m%m_d%d_H%H_M%M_S%S", time.localtime())+".sh"
    if data_mc:
        outputfiles = "submit_all_data.sh"
    else:
        outputfiles = "submit_all_mc.sh"
    output = ""
    for i in files:
        i = i.replace(" ","").replace("\n","")
        if "Condor" in i:
            if data_mc:
                if "Run201" in i:
                    submit_str = "condor_submit tasks/"+i+"/submit.cmd"
                    output += submit_str+"\n"
            if not data_mc:
                if "Run201" in i:
                    continue
                submit_str = "condor_submit tasks/"+i+"/submit.cmd"
                output += submit_str+"\n"
        
    with open(outputfiles,"w") as f:
        f.write(output)


if __name__ == "__main__":

    parser = argparse.ArgumentParser(description="Submit jobs for VVV analysis")
    parser.add_argument('-m' , '--mode'        , dest='mode'      , help='tag of the job'            , type=int, required=True                                     )
    parser.add_argument('-a' , '--addflags'    , dest='addflags'  , help='adding flags to metis'     , type=str,                default=""                         )
    parser.add_argument('-d' , '--datamc'      , dest='datamc'    , help='data or mc'                ,                          default=False , action='store_true')
    parser.add_argument('-y' , '--year'        , dest='year'      , help='data year'                 , type=int, required=True                                     )
    parser.add_argument('-t' , '--thetag'      , dest='thetag'    , help='tag'                       , type=str,                default="test"                     )
    parser.add_argument('-s' , '--mysample'    , dest='mysample'  , help='don\'t do autmoated sample',                          default=False , action='store_true')
    parser.add_argument('-dr' , '--dryrun'    , dest='dryrun'  , help='only generate submit file',                          default=True , action='store_true')
    parser.add_argument('-p' , '--gridpassword'      , dest='gridpassword'    , help='gridpassword'                       , type=str,                default=""  )
    parser.add_argument('-o' , '--outputdir'      , dest='outputdir'    , help='outputdir'                       , type=str,                default=None  )
    parser.add_argument('-su' , '--supplement_datasets'      , dest='supplement_datasets'    , help='supplement datasets'                ,                          default=False , action='store_true')

    # Argument parser
    args = parser.parse_args()
    args.mode
    
    if not args.outputdir:
        sys.exit("outputdir is not defined")

    # grid password
    grid_password = args.gridpassword
    condorpath = os.path.dirname(os.path.realpath(__file__))
    os.system("echo "+grid_password+" | voms-proxy-init -voms cms -valid 192:00;cp /tmp/x509up_u{0} ".format(os.getuid())+condorpath) 
    
    if  args.year==2016:
        if args.mode == 1:
            if args.datamc:
                sample_map = samples.Lepton1_2fatJets_2016_NanoAODv8_data # See condor/samples.py
            if not args.datamc:
                sample_map = samples.Lepton1_2fatJets_2016_NanoAODv8_mc # See condor/samples.py
    elif args.year==2017:
        if args.mode == 1:
            sample_map = samples.Lepton1_2fatJets_2016_NanoAODv8 # See condor/samples.py
    elif args.year==2018:
        if args.mode == 1:
            sample_map = samples.Lepton1_2fatJets_2016_NanoAODv8 # See condor/samples.py
        if args.mode == 0:
            sample_map = samples.Lepton0_2fatJets_2018_MINIAODv2 # See condor/samples.py

    # submission tag
    tag = args.thetag 

    # Task summary for printing out msummary
    task_summary = {}

    if args.dryrun :
        for ds,shortname in sample_map.items():
            # skip_tail = True
            skip_tail = False
            print(shortname)
            task = CondorTask(
                    sample = ds,
                    files_per_output = split_func(ds.get_datasetname()),
                    output_name = "output.root",
                    tag = tag,
                    output_dir = args.outputdir+shortname+"/",
                    condor_submit_params = {
                        "sites": "T2_US_UCSD,UAF",
                        "use_xrootd":True,
                        "requireHAS_SINGULARITY" : False,
                        "classads": [
                            # need to add quota here \"
                            # ["metis_extraargs", "\"--mode {} {}\"".format(args.mode,args.addflags)]
                            # for MaxRuntime, this is required to be integer
                            ["MaxRuntime", "172800"]
                            ]
                        },
                    cmssw_version = "CMSSW_10_2_13",
                    scram_arch = "slc7_amd64_gcc700",
                    input_executable = "{}/customized_NanoAOD.sh".format(condorpath), # your condor executable here
                    tarfile = "{}/package.tar.xz".format(condorpath), # your tarfile with assorted goodies here
                    special_dir = "VVVAnalysis/{}/{}".format(tag,args.year), # output files into /hadoop/cms/store/<user>/<special_dir>
                    min_completion_fraction = 0.50 if skip_tail else 1.0,
                    # additional arguments are passed in the arguments
                    # how the white space is treated in the condor:
                    # https://research.cs.wisc.edu/htcondor/manual/v7.7/condor_submit.html
                    arguments = "",
                    # max_jobs = 10,
                    # add additional_input_files
                    # proxypath should be the full path of the file that you want Condor to ship with the job, its value has to be the full resolved AFS path, i.e. it cannot be $HOME/private/x509up, nor ~/private/x509, nor a path on /eos/.
                    # https://batchdocs.web.cern.ch/tutorial/exercise2e_proxy.html
                    additional_input_files = ['%s/%s'%( condorpath , "x509up_u%s"%(os.getuid()) ) , '%s/%s'%(condorpath,"SearchSite.py") , '%s/%s'%(condorpath,"ValidSite.py") ],
            )
            task.process()
            
    Create_Submit_Scripts(args.datamc)
    sys.exit(0)