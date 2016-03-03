#!/usr/bin/env python

import sys, os, shutil

sys.path.append(os.path.join(os.path.dirname(__file__), '../'))
from local_common import LOCAL_DIR, LocalFile, LocalDownloader, LocalUploader, LocalLinker, FilesAndPaths

sys.path.append(os.path.join(os.path.dirname(__file__), '../../'))
from core.xcor import Xcor

def testXcor(working_dir):
	outFolder = "/nfs/0_metadata@bib5/dnanexus_refactor_test/output_test/"
	os.chdir(outFolder)

	#The files need to be checked
	xcr = Xcor("R1.raw.srt.filt.nodup.srt.bam",LocalFile)
	xcr.download(LocalDownloader)
	xcr.process(working_dir)
	xcr.upload(LocalUploader)
	output = xcr.output(LocalLinker)

	print "************************** output"

	for k, v in output.iteritems():
		print k, v

def main():
	working_dir=os.path.join(os.path.dirname(os.path.realpath(__file__)), '../')
	print working_dir
	print testXcor(working_dir)

if __name__ == '__main__':
	sys.exit(main())
