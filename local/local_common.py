import os, sys, shutil

LOCAL_DIR = "/nfs/0_metadata@bib5/dnanexus_refactor_test/histone/macs2"

class LocalFile:
    def __init__(self, fn, d = LOCAL_DIR):
        self.fnp = os.path.join(d, fn)
        self.name = fn

    def get_id(self):
        return self.fnp

    @staticmethod
    def init(fn):
        if isinstance(fn, basestring):
            return LocalFile(fn)
        return fn

class LocalDownloader:
    @staticmethod
    def download(uri, fn):
        print uri, fn
        shutil.copyfile(uri, fn)

class LocalUploader:
    @staticmethod
    def upload(fn):
        print "not sure how to upload", fn
        return fn

class LocalLinker:
    @staticmethod
    def link(fn):
        print "not sure how to link", fn
        return fn

class FilesAndPaths:
    # TODO: refactor paths out
    # TODO: autodownload?
    d = "/project/umw_zhiping_weng/0_metadata/"
    mm10_chrom_sizes = LocalFile("mm10.chromInfo", os.path.join(d, "genome"))
    hg19_chrom_sizes = LocalFile("hg19.chromInfo", os.path.join(d, "genome"))
    as_narrowPeak = LocalFile("narrowPeak.as", os.path.join(d, "tools/ucsc.v287/as/"))
    as_gappedPeak = LocalFile("gappedPeak.as", os.path.join(d, "tools/ucsc.v287/as/"))
    as_broadPeak = LocalFile("broadPeak.as", os.path.join(d, "tools/ucsc.v287/as/"))
