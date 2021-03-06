#!/usr/bin/env python
# overlap_peaks 0.0.1
# Generated by dx-app-wizard.
#
# Basic execution pattern: Your app will run on a single machine from
# beginning to end.
#
# See https://wiki.dnanexus.com/Developer-Portal for documentation and
# tutorials on how to modify this file.
#
# DNAnexus Python Bindings (dxpy) documentation:
#   http://autodoc.dnanexus.com/bindings/python/current/

import sys, os, re
import dxpy
import common

sys.path.append(os.path.join(os.path.dirname(__file__), '../../../core/overlap_peaks'))
from overlap_peaks import OverlapPeaks

class DxFile:
        @staticmethod
        def init(fn):
                return dxpy.DXFile(fn)

class DxDownloader:
        @staticmethod
        def download(uri, fn):
                return dxpy.download_dxfile(uri, fn)

class DxUploader:
        @staticmethod
        def upload(fn):
                return dxpy.upload_local_file(fn)

class DxLinker:
        @staticmethod
        def link(fn):
                return dxpy.dxlink(fn)

@dxpy.entry_point('main')
def main(rep1_peaks, rep2_peaks, pooled_peaks, pooledpr1_peaks, pooledpr2_peaks,
         chrom_sizes, as_file, peak_type):

        op = OverlapPeaks(rep1_peaks, rep2_peaks, pooled_peaks, pooledpr1_peaks,
                          pooledpr2_peaks, chrom_sizes, as_file, peak_type,
                          DxFile)
        op.download(DxDownloader)
        op.process()
        op.upload(DxUploader)
        return op.output(DxLinker)

dxpy.run()
