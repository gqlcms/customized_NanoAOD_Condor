from metis.Utils import *

def get_proxy_file_V2():
    # on the lxplus,  /tmp/x509up_u$(id -u), please note that that file is not readable for Condor, the proxy file will transfer to condor site as input file
    return "x509up_u{0}".format(os.getuid())

def condor_submit_lxplus(**kwargs): # pragma: no cover
    """
    Takes in various keyword arguments to submit a condor job.
    Returns (succeeded:bool, cluster_id:str)
    fake=True kwarg returns (True, -1)
    multiple=True will let `arguments` and `selection_pairs` be lists (of lists)
    and will queue up one job for each element
    """
    
    # the kwargs is get from task's condor_submit_params
    if kwargs.get("fake",False):
        return True, -1

    dryrun = kwargs.get("dryrun",True)

    for needed in ["executable","arguments","inputfiles","logdir","transfer_output_files"]:
        if needed not in kwargs:
            raise RuntimeError("To submit a proper condor job, please specify: {0}".format(needed))

    params = {}

    queue_multiple = kwargs.get("multiple",False)

    params["universe"] = kwargs.get("universe", "Vanilla")
    params["executable"] = kwargs["executable"]
    params["transfer_output_files"] = kwargs["transfer_output_files"]
    params["inputfiles"] = ",".join(kwargs["inputfiles"])
    params["logdir"] = kwargs["logdir"]
    params["proxy"] = get_proxy_file_V2()
    params["timestamp"] = get_timestamp()
    params["memory"] = kwargs.get("memory",2048)


    exe_dir = params["executable"].rsplit("/",1)[0]
    if "/" not in os.path.normpath(params["executable"]):
        exe_dir = "."
    
    params["sites"] = kwargs.get("sites",",".join(good_sites))

    if queue_multiple:
        # params["arguments"] is a list, the element is for one dataset file
        if len(kwargs["arguments"]) and (type(kwargs["arguments"][0]) not in [tuple,list]):
            raise RuntimeError("If queueing multiple jobs in one cluster_id, arguments must be a list of lists")
        params["arguments"] = map(lambda x: " ".join(map(str,x)), kwargs["arguments"])
        params["extra"] = []
        if "selection_pairs" in kwargs:
            sps = kwargs["selection_pairs"] # selection_pairs is a list, elements of selection_pairs is select_pairs for each dataset file, select_pairs is a list,  elements of select_pairs is one parameter for each dataset file, one parameter is a tuple: (name, parameter contents)
            if len(sps) != len(kwargs["arguments"]):
                raise RuntimeError("Selection pairs must match argument list in length")
            for sel_pairs in sps:
                extra = ""
                for sel_pair in sel_pairs:
                    if len(sel_pair) != 2:
                        raise RuntimeError("This selection pair is not a 2-tuple: {0}".format(str(sel_pair)))
                    extra += '{0}="{1}"\n'.format(*sel_pair)
                params["extra"].append(extra)
    else:
        params["arguments"] = " ".join(map(str,kwargs["arguments"]))
        params["extra"] = ""
        if "selection_pairs" in kwargs:
            for sel_pair in kwargs["selection_pairs"]:
                if len(sel_pair) != 2:
                    raise RuntimeError("This selection pair is not a 2-tuple: {0}".format(str(sel_pair)))
                params["extra"] += '+{0}="{1}"\n'.format(*sel_pair)

    params["proxyline"] = "x509userproxy={proxy}".format(proxy=params["proxy"])

    # Require singularity+cvmfs unless machine is uaf-*. or uafino.
    # NOTE, double {{ and }} because this gets str.format'ted later on
    # Must have singularity&cvmfs. Or, (it must be uaf or uafino computer AND if a uaf computer must not have too high of slotID number
    # so that we don't take all the cores of a uaf
    # requirements_line = 'Requirements = ((HAS_SINGULARITY=?=True)) || (regexp("(uaf-[0-9]{{1,2}}|uafino)\.", TARGET.Machine) && !(TARGET.SlotID>(TotalSlots<14 ? 3:7) && regexp("uaf-[0-9]", TARGET.machine)))'
    requirements_line = 'Requirements = (HAS_SINGULARITY=?=True)'
    if kwargs.get("universe","").strip().lower() in ["local"]:
        kwargs["requirements_line"] = "Requirements = "
    if kwargs.get("requirements_line","").strip():
        requirements_line = kwargs["requirements_line"]

    template = """
universe={universe}
RequestMemory = {memory}
RequestCpus = 1
executable={executable}
transfer_executable=True
transfer_input_files={inputfiles}
transfer_output_files ={transfer_output_files}
log={logdir}/{timestamp}.log
output={logdir}/std_logs/1e.$(Cluster).$(Process).out
error={logdir}/std_logs/1e.$(Cluster).$(Process).err
notification=Never
should_transfer_files = YES
when_to_transfer_output = ON_EXIT
"""
    template += "{0}\n".format(params["proxyline"])
    # do we need the HAS_SINGULARITY? 
    if kwargs.get("requireHAS_SINGULARITY",True):
        template += "{0}\n".format(requirements_line)
    if kwargs.get("container",None):
        template += '+SingularityImage="{0}"\n'.format(kwargs.get("container",None))
    if kwargs.get("stream_logs",False):
        template += "StreamOut=True\nstream_error=True\nTransferOut=True\nTransferErr=True\n"

    # the classads is passed in the submit_multiple_condor_jobs by **extra
    for ad in kwargs.get("classads",[]):
        if len(ad) != 2:
            raise RuntimeError("This classad pair is not a 2-tuple: {0}".format(str(ad)))
        template += '+{0}={1}\n'.format(*ad)
    do_extra = len(params["extra"]) == len(params["arguments"]) 
    if queue_multiple:
        template += "\n"
        for ijob,args in enumerate(params["arguments"]):
            template += "arguments=\"{0}\"\n".format(args)
            if do_extra:
                template += "{0}\n".format(params["extra"][ijob])
            template += "queue\n"
            template += "\n"
    else:
        template += "arguments={0}\n".format(params["arguments"])
        template += "{0}\n".format(params["extra"])
        template += "queue\n"

    if kwargs.get("return_template",False):
        return template.format(**params)

    with open("{0}/submit.cmd".format(exe_dir),"w") as fhout:
        fhout.write(template.format(**params))

    if dryrun:
        succeeded = True # don't know where this will be used
        cluster_id = -1 # don't know where this will be used
    else:
        extra_cli = ""
        schedd = kwargs.get("schedd","") # see note in condor_q about `schedd`
        if schedd:
            extra_cli += " -name {} ".format(schedd)
        out = do_cmd("mkdir -p {0}/std_logs/  ; condor_submit {1}/submit.cmd {2}".format(params["logdir"],exe_dir,extra_cli))

        succeeded = False
        cluster_id = -1
        if "job(s) submitted to cluster" in out:
            succeeded = True
            cluster_id = out.split("submitted to cluster ")[-1].split(".",1)[0].strip()
        else:
            raise RuntimeError("Couldn't submit job to cluster because:\n----\n{0}\n----".format(out))

    return succeeded, cluster_id


    