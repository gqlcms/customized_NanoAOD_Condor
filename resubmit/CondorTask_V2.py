import os
import time

from metis.Constants import Constants
from metis.Task import Task
from metis.File import EventsFile
import metis.Utils as Utils
import Utils_V2
from metis.CondorTask import CondorTask


class CondorTask_V2(CondorTask):

    def __init__(self, **kwargs):
        
        self.dryrun = kwargs.get("dryrun", True)
        
        super(CondorTask_V2, self).__init__(**kwargs)

        print("self.sample",self.sample)

        # set the output dir
        User = os.environ.get("USER")
        default_output_dir = "/eos/user/{0}/{1}/PKUVVV/NanoAOD/{2}/".format(User[0],User,time.strftime("y%Y_m%m_d%d_H%H_M%M_S%S", time.localtime()))
        self.output_dir = kwargs.get("output_dir",default_output_dir)
        os.system("mkdir -p "+self.output_dir)


    def run(self, fake=False, optimizer=None):
        """
        Main logic for looping through (inputs,output) pairs. In this
        case, this is where we submit, resubmit, etc. to condor
        If fake is True, then we mark the outputs as done and never submit
        """
        condor_job_dicts = self.get_running_condor_jobs()
        print("condor_job_dicts")
        print(condor_job_dicts)
        condor_job_indices = set([int(rj["jobnum"]) for rj in condor_job_dicts])
        print("condor_job_indices")
        print(condor_job_indices)

        nfiles_reset = self.recache_outputs()
        if nfiles_reset > 0:
            self.logger.info("{0} files may have been deleted".format(nfiles_reset))

        to_submit = []

        # main loop over input-output map
        print("self.io_mapping")
        print(self.io_mapping)
        for iout, (ins, out) in enumerate(self.io_mapping):
            if self.max_jobs > 0 and iout >= self.max_jobs:
                break

            index = out.get_index()  # "merged_ntuple_42.root" --> 42
            on_condor = index in condor_job_indices
            done = (out.exists() and not on_condor)
            if done:
                self.handle_done_output(out)
                continue

            if fake:
                out.set_fake()

            if not on_condor:
                # Submit and keep a log of condor_ids for each output file that we've submitted
                to_submit.append({
                    "ins": ins,
                    "out": out,
                    })

            else:
                this_job_dict = next(rj for rj in condor_job_dicts if int(rj["jobnum"]) == index)
                action_type = self.handle_condor_job(this_job_dict, out)

        if to_submit:
            v_ins = [d["ins"] for d in to_submit]
            v_out = [d["out"] for d in to_submit]
            succeeded, cluster_id = self.submit_multiple_condor_jobs(v_ins, v_out, fake=fake, optimizer=optimizer)
            procids = map(str,range(len(v_out)))
            if succeeded:
                for out,procid in zip(v_out,procids):
                    index = out.get_index()  # "merged_ntuple_42.root" --> 42
                    cid = str(cluster_id).split(".")[0] + "." + procid
                    if index not in self.job_submission_history:
                        self.job_submission_history[index] = []
                    self.job_submission_history[index].append(cid)
                    ntimes = len(self.job_submission_history[index])
                    if ntimes <= 1:
                        self.logger.info("Job for ({0}) submitted to {1}".format(out, cid))
                    else:
                        self.logger.info("Job for ({0}) submitted to {1} (for the {2} time)".format(out, cid, Utils.num_to_ordinal_string(ntimes)))

    def prepare_inputs(self):

        # need to take care of executable, tarfile
        self.executable_path = "{0}/executable.sh".format(self.get_taskdir())
        

        # take care of executable. easy.
        Utils.do_cmd("cp {0} {1}".format(self.input_executable, self.executable_path))

        # take care of package tar file if we were told to. easy.
        # copy the package tar file will waste space 
        self.package_path = self.tarfile
        # if self.tarfile:
        #     Utils.do_cmd("cp {0} {1}".format(self.tarfile, self.package_path))

        self.prepared_inputs = True

    def process(self, fake=False, optimizer=None, **kwargs):
        """
        Prepare inputs
        Execute main logic
        Backup
        """

        print(self.sample)

        dryrun = kwargs.get("dryrun", self.dryrun)

        self.logger.info("Began processing {0} ({1})".format(self.sample.get_datasetname(),self.tag))
        # set up condor input if it's the first time submitting
        if (not self.prepared_inputs) or self.recopy_inputs:
            self.prepare_inputs()

        self.run(fake=fake, optimizer=optimizer)

        if not dryrun:
            self.try_to_complete()
            if self.complete():
                self.finalize()

            self.backup()

            self.logger.info("Ended processing {0} ({1})".format(self.sample.get_datasetname(),self.tag))

    def Rename_OutFile(self, inputfile):
        if "/store/mc/" in inputfile:
            return inputfile.replace("/store/mc/","").replace("MINIAODSIM/","").replace("RunIISummer20UL/","").replace("/","")

    
    def submit_multiple_condor_jobs(self, v_ins, v_out, fake=False, optimizer=None):

        outdir = self.output_dir
        outname_noext = self.output_name.rsplit(".", 1)[0]
        v_inputs_commasep = [",".join(map(lambda x: x.get_name(), ins)) for ins in v_ins] # v_ins contains all the input dataset files
        v_index = [out.get_index() for out in v_out]
        cmssw_ver = self.cmssw_version
        scramarch = self.scram_arch
        executable = self.executable_path
        index_ins = zip(v_index,v_inputs_commasep)
        v_arguments = [ [inputs_commasep,index,self.output_name,self.arguments] for (index,inputs_commasep) in index_ins]
        if optimizer: 
            v_sites = optimizer.get_sites(self, v_ins, v_out)
            v_selection_pairs = [
                    [
                        ["taskname", self.unique_name],
                        ["jobnum", index],
                        ["tag", self.tag],
                        ["metis_retries", len(self.job_submission_history.get(index,[]))],
                        ["DESIRED_Sites", sites],
                        ] 
                    for index,sites in zip(v_index,v_sites)
                    ]
        else:
            # each element is for one dataset file
            v_selection_pairs = [
                    [
                        ["+taskname", self.unique_name],
                        ["+jobnum", index],
                        ["+tag", self.tag],
                        ["+metis_retries", len(self.job_submission_history.get(index,[]))],
                        ["transfer_output_remaps", "".join( ( self.output_name, ' = ' ,self.get_outputdir(), self.Rename_OutFile(dict(index_ins)[index]) ) )], # use the rename function to rename the ouputfile, dict(index_ins)[index] return the inputfiles with comma
                    ] 
                    for index in v_index
            ]
        logdir_full = os.path.abspath("{0}/logs/".format(self.get_taskdir()))
        input_files = []
        input_files += self.additional_input_files
        extra = self.kwargs.get("condor_submit_params", {})

        # for different service, condor submit file is different 
        condor_submit = Utils_V2.condor_submit_lxplus
        
        return condor_submit(
                    executable=executable, arguments=v_arguments,
                    inputfiles=input_files, logdir=logdir_full,
                    selection_pairs=v_selection_pairs,
                    multiple=True,
                    transfer_output_files=self.output_name,
                    fake=fake, **extra
               )