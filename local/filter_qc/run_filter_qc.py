#!/usr/bin/env python

import sys, os, shutil

sys.path.append(os.path.join(os.path.dirname(__file__), '../'))
from local_common import LOCAL_DIR, LocalFile, LocalDownloader, LocalUploader, LocalLinker, FilesAndPaths

sys.path.append(os.path.join(os.path.dirname(__file__), '../../'))
from core.filter_qc import Fqc

def main():
#def main(reads1=None, reference_tar=None, bwa_aln_params=None, bwa_version=None, samtools_version=None, \
#        reads2=None, input_JSON=None, debug=False):

#    logger = logging.getLogger(__name__)
    # Main entry-point.  Parameter defaults assumed to come from dxapp.json.
    # reads1, reference_tar, reads2 are links to DNAnexus files or None

#    if debug:
#        logger.setLevel(logging.DEBUG)
#    else:
#       logger.setLevel(logging.INFO)
    # if there is input_JSON, it over-rides any explicit parameters
    outFolder = "/nfs/0_metadata@bib5/dnanexus_refactor_test/output_test/bwa/"
    os.chdir(outFolder)


    input_bam="R1.raw.srt.bam"
    samtools_params = "-q 30"
    paired_end=False

    subjob_input = {"input_bam" : input_bam,
                    "samtools_params" : samtools_params,
                    "paired_end": paired_end,
                    "LocalFile" : LocalFile}
    print "Submitting:"
    print subjob_input
    fqc = Fqc(subjob_input)
    output = fqc.process(LocalDownloader, LocalUploader, LocalLinker)
    # Create the job that will perform the "postprocess" step.  depends_on=subjobs, so blocks on all subjobs

    print "Exiting with output: %s" %(output)

if __name__ == '__main__':
    sys.exit(main())
