#!/usr/bin/env python

import sys, os, shutil

sys.path.append(os.path.join(os.path.dirname(__file__), '../../'))
from core.macs2 import Macs2PeakCaller

class FilesAndPaths:
    # TODO: refactor paths out
    # TODO: autodownload?
    d = "/project/umw_zhiping_weng/0_metadata/"
    mm10_chrom_sizes = os.path.join(d, "genome", "mm10.chromInfo")
    as_narrowPeak = os.path.join(d, "tools/ucsc.v287/as/", "narrowPeak.as")

def testOverlap():
    outFolder = "/home/mjp/dnanexus_refactor_test/scratch/"
    os.chdir(outFolder)

    op = OverlapPeaks("R1.raw.srt.filt.nodup.srt.SE.tagAlign.narrowPeak.gz",
                      "R2.raw.srt.filt.nodup.srt.SE.tagAlign.narrowPeak.gz",
                      "R1.raw.srt.filt.nodup.srt.SE-R2.raw.srt.filt.nodup.srt.SE_pooled.tagAlign.narrowPeak.gz",
                      "R1.raw.srt.filt.nodup.srt.SE.SE.pr1-R2.raw.srt.filt.nodup.srt.SE.SE.pr1_pooled.tagAlign.narrowPeak.gz",
                      "R1.raw.srt.filt.nodup.srt.SE.tagAlign.gz.SE.pr2-R2.raw.srt.filt.nodup.srt.SE.tagAlign.gz.SE.pr2_pooled.tagAlign.narrowPeak.gz",
                      FilesAndPaths.mm10_chrom_sizes,
                      FilesAndPaths.as_narrowPeak,
                      "narrowPeak", LocalFile)

    op.download(LocalDownloader)
    op.process()
    op.upload(LocalUploader)
    output = op.output(LocalLinker)

    print "************************** output"
    for k, v in output.iteritems():
        print k, v

def main():
    print testOverlap()

if __name__ == '__main__':
    sys.exit(main())
