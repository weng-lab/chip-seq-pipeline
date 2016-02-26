#!/usr/bin/env python

import sys, os, shutil

sys.path.append(os.path.join(os.path.dirname(__file__), '../'))
from local_common import LOCAL_DIR, LocalFile, LocalDownloader, LocalUploader, LocalLinker, FilesAndPaths

sys.path.append(os.path.join(os.path.dirname(__file__), '../../'))
from core.spp import SPP

def testSPP():
    outFolder = "/home/mjp/dnanexus_refactor_test/scratch/"
    os.chdir(outFolder)

    spc = SPP("R2.raw.srt.filt.nodup.srt.SE.tagAlign.gz",
              "C2.raw.srt.filt.nodup.srt.SE.tagAlign.gz",
              "R2.raw.srt.filt.nodup.srt.filt.nodup.sample.15.SE.tagAlign.gz.cc.qc",
              FilesAndPaths.mm10_chrom_sizes,
              FilesAndPaths.as_narrowPeak,
              FilesAndPaths.as_gappedPeak,
              FilesAndPaths.as_broadPeak,
              "hs", #TODO: extract out!
              LocalFile )

    spc.download(LocalDownloader)
    spc.process()
    spc.upload(LocalUploader)
    output = spc.output(LocalLinker)

    print "************************** output"
    for k, v in output.iteritems():
        print k, v

def main():
    print testSPP()

if __name__ == '__main__':
    sys.exit(main())
