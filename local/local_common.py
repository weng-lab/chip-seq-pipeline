import os, sys, shutil

LOCAL_DIR = "/home/mjp/dnanexus_refactor_test/histone/macs2"

class LocalFile:
    def __init__(self, fn):
        self.fnp = os.path.join(LOCAL_DIR, fn)
        self.name = fn

    def get_id(self):
        return self.fnp

    @staticmethod
    def init(fn):
        return LocalFile(fn)

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

