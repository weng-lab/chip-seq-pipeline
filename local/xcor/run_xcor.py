#!/usr/bin/env python

import sys, os, shutil

sys.path.append(os.path.join(os.path.dirname(__file__), '../'))
from local_common import LOCAL_DIR, LocalFile, LocalDownloader, LocalUploader, LocalLinker, FilesAndPaths

sys.path.append(os.path.join(os.path.dirname(__file__), '../../'))
from core.xcor import Xcor

def testXcor():
    outFolder = "/home/mjp/dnanexus_refactor_test/scratch/"
    os.chdir(outFolder)
#The files need to be checked
    xcr = Xcor("R2.raw.srt.filt.nodup.srt.SE.tagAlign.gz",
                          "C2.raw.srt.filt.nodup.srt.SE.tagAlign.gz",
                          "R2.raw.srt.filt.nodup.srt.filt.nodup.sample.15.SE.tagAlign.gz.cc.qc",
                          FilesAndPaths.mm10_chrom_sizes,
                          FilesAndPaths.as_narrowPeak,
                          FilesAndPaths.as_gappedPeak,
                          FilesAndPaths.as_broadPeak,
                          "hs", #TODO: extract out!
                          LocalFile)

    xcr.download(LocalDownloader)
    xcr.process()
    xcr.upload(LocalUploader)
    output = xcr.output(LocalLinker)

    print "************************** output"
    for k, v in output.iteritems():
        print k, v

def main():
    print testXcor()

if __name__ == '__main__':
    sys.exit(main())
