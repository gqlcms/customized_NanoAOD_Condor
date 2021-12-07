import sys,os
import argparse
import re
import optparse

def getValidSite(filepath, retry = 5):

    print "Searching for valid site"
    for i in range(retry):
        os.system("cmsRun ${CMSSW_BASE}/src/ValidSite.py inputFiles=\""+filepath+"\" > test_ValidSite.log 2>&1")
        with open ("test_ValidSite.log","r") as test:
            if 'Successfully opened file ' in test.read():
                break

    with open('Localfile.txt','w') as f:
        f.write(filepath.split("/")[-1])

    with open ("test_ValidSite.log","r") as test:
        test_lines = test.read()
        if 'Successfully' in test_lines:
            if '/store/mc' in test_lines:
                valid_site = test_lines.split('Successfully opened file ')[1].split('/store/mc')[0]
            if '/store/data' in test_lines:
                valid_site = test_lines.split('Successfully opened file ')[1].split('/store/data')[0]
            if "root:/" not in valid_site:
                valid_site = 'root://cmsxrootd.fnal.gov/'
            print "valid site for ", filepath, ": ", valid_site, "\n"
            return(valid_site)
        else:
            print "found no accessable site for "+filepath
            return('root://cmsxrootd.fnal.gov/')


if __name__ == '__main__':
    with open('ValidSite.txt','w') as f:
        f.write(getValidSite(sys.argv[1]))
