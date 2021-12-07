#!/bin/bash

INPUTFILENAMES=$1
IFILE=$2
OUTPUTFILENAMES=$3

BASEPATH=`pwd`
pwd
ls -lth

export SCRAM_ARCH=slc7_amd64_gcc700

source /cvmfs/cms.cern.ch/cmsset_default.sh
if [ -r CMSSW_10_6_26/src ] ; then
  echo release CMSSW_10_6_26 already exists
else
  scram p CMSSW CMSSW_10_6_26
fi
cd CMSSW_10_6_26/src
eval `scram runtime -sh`

git clone https://github.com/gqlcms/Customized_NanoAOD.git .

scram b -j 16

mv $BASEPATH/SearchSite.py .
mv $BASEPATH/ValidSite.py .

python SearchSite.py $INPUTFILENAMES
cat test_ValidSite.log
XRDSITE=`cat ValidSite.txt`
INPUTFILENAMES=${XRDSITE}${INPUTFILENAMES}
echo $INPUTFILENAMES
xrdcp $INPUTFILENAMES .
LOCALInputFile=`cat Localfile.txt`
echo $LOCALInputFile

cmsDriver.py mc2016 \
-n -1 \
--mc \
--eventcontent NANOAODSIM \
--datatier NANOAODSIM \
--conditions 106X_mcRun2_asymptotic_v17 \
--step NANO \
--nThreads 1 \
--era Run2_2016,run2_nanoAOD_106Xv2 \
--customise PhysicsTools/NanoTuples/nanoTuples_cff.nanoTuples_customizeMC \
--filein file:$LOCALInputFile \
--fileout file:$OUTPUTFILENAMES \
--no_exec

cmsRun mc2016_NANO.py

pwd
ls -lth

mv $OUTPUTFILENAMES $BASEPATH
echo $BASEPATH
ls -lth $BASEPATH