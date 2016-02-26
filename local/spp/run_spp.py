#!/usr/bin/env python

import sys, os, shutil

sys.path.append(os.path.join(os.path.dirname(__file__), '../'))
from local_common import LOCAL_DIR, LocalFile, LocalDownloader, LocalUploader, LocalLinker, FilesAndPaths

sys.path.append(os.path.join(os.path.dirname(__file__), '../../'))
from core.spp import SPP

def testSPP():
    outFolder = "/home/pratth/_tftest/"
    os.chdir(outFolder)

    spc = SPP(LocalFile,
              "R2.raw.srt.filt.nodup.srt.SE.tagAlign.gz",
              "C2.raw.srt.filt.nodup.srt.SE.tagAlign.gz",
              "R2.raw.srt.filt.nodup.srt.SE.tagAlign.gz.SE.pr2.ccscores",
              FilesAndPaths.hg19_chrom_sizes,
              300000,
              True,
              True,
              FilesAndPaths.as_narrowPeak )

    spc.download(LocalDownloader)
    spc.process("/home/pratth/weng-lab/chip-seq-pipeline/dnanexus/spp/resources")
    spc.upload(LocalUploader)
    output = spc.output(LocalLinker)

    print "************************** output"
    for k, v in output.iteritems():
        print k, v

def main():
    print testSPP()

if __name__ == '__main__':
    sys.exit(main())
