#!/usr/bin/env python

import sys, os, shutil

sys.path.append(os.path.join(os.path.dirname(__file__), './'))
from local_common import LOCAL_DIR, LocalFile, LocalDownloader, LocalUploader, LocalLinker, FilesAndPaths

sys.path.append(os.path.join(os.path.dirname(__file__), '../'))
from core.pool import Pooler

def main():
    outFolder="/nfs/0_metadata@bib5/dnanexus_refactor_test/output_test/bwa/"
    os.chdir(outFolder)
    inputs=["R1.raw.srt.filt.nodup.srt.SE.tagAlign.gz", "R2.raw.srt.filt.nodup.srt.SE.tagAlign.gz"]
    Pooler.process(inputs, LocalFile, LocalDownloader, LocalUploader, LocalLinker)

if __name__ == '__main__':
    sys.exit(main())
