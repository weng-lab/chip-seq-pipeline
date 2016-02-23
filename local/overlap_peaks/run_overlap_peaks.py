#!/usr/bin/env python

import sys, os

sys.path.append(os.path.join(os.path.dirname(__file__), '../../core/overlap_peaks/src/'))

class FilesAndPaths:
    # TODO: refactor paths out
    # TODO: autodownload?
    d = "/project/umw_zhiping_weng/0_metadata/genome/"
    mm10_chrom_sizes = os.path.join(d, "mm10.chromInfo")
    as_narrowPeak = os.path.join(d, "tools/ucsc.v287/as", "narrowPeak.as")


class LocalDownloader:
    @staticmethod
    def download(uri, fn):
        r = requests.get(uri)
        fnpTmp = os.path.join("/tmp", fn)
        with open(fnpTmp, "wb") as f:
            f.write(r.content)
        shutil.move(fnpTmp, fnp)
        return True

class LocalUploader:
    @staticmethod
    def upload(fn):
        print "not sure how to upload", fn

class LocalLinker:
    @staticmethod
    def link(fn):
        print "not sure how to link", fn

def testOverlap():
    op = OverlapPeaks("ENCSR678FIT-chr19-ta-IDR2-201505041839/encode_macs2/ENCFF926URZ.raw.srt.filt.nodup.srt.SE.chr19.tagAlign.narrowPeak.gz",
                      "ENCSR678FIT-chr19-ta-IDR2-201505041839/encode_macs2/ENCFF593LFI-ENCFF919IQP_pooled.raw.srt.filt.nodup.srt.SE.chr19.tagAlign.narrowPeak.gz",
                      "ENCSR678FIT-chr19-ta-IDR2-201505041839/encode_macs2/ENCFF926URZ.raw.srt.filt.nodup.srt.SE.chr19-ENCFF593LFI-ENCFF919IQP_pooled.raw.srt.filt.nodup.srt.SE.chr19_pooled.tagAlign.narrowPeak.gz",
                      "ENCSR678FIT-chr19-ta-IDR2-201505041839/encode_macs2/ENCFF926URZ.raw.srt.filt.nodup.srt.SE.chr19.SE.pr1-ENCFF593LFI-ENCFF919IQP_pooled.raw.srt.filt.nodup.srt.SE.chr19.SE.pr1_pooled.tagAlign.narrowPeak.gz",
                      "ENCSR678FIT-chr19-ta-IDR2-201505041839/encode_macs2/ENCFF926URZ.raw.srt.filt.nodup.srt.SE.chr19.tagAlign.gz.SE.pr2-ENCFF593LFI-ENCFF919IQP_pooled.raw.srt.filt.nodup.srt.SE.chr19.tagAlign.gz.SE.pr2_pooled.tagAlign.narrowPeak.gz",
                      FilesAndPaths.mm10_chrom_sizes,
                      FilesAndPaths.as_narrowPeak,
                      "peak_type=narrowPeak")

    op.download(LocalDownloader)
    op.upload(LocalUploader)
    return op.output(LocalLinker)

def main():
    testOverlap()

if __name__ == '__main__':
    sys.exit(main())
