#!/usr/bin/env python

import os, sys, subprocess, shlex

sys.path.append(os.path.join(os.path.dirname(__file__), '../dnanexus'))
import common

class SPP(object):

	def __init__(self, experiment, control, xcor_scores_input, npeaks, nodups, bigbed, chrom_sizes, as_file=None):

		# Initialize required data object inputs
		self.experiment        = File.init(experiment)
		self.control           = File.init(control)
		self.xcor_scores_input = File.init(xcor_scores_input)
		self.chrom_sizes       = File.init(chrom_sizes)

		# Initialize AS file only if bigbed set
		if bigbed: self.as_file = File.init(as_file)

		# Store parameters
		self.npeaks = npeaks
		self.nodups = nodups
		self.bigbed = bidbed

	def download(self, downloader):

		# Download the file inputs to the local file system.
		downloader.download(self.chrom_sizes.get_id(), self.chrom_sizes.name)
		if self.bigbed: downloader.download(self.as_file.get_id(), self.as_file.name)

		# The following line(s) download your file inputs to the local file system
		# using variable names for the filenames.
		downloader.download(self.experiment.get_id(), self.experiment.name)
		downloader.download(self.control.get_id(), self.control.name)
		downloader.download(self.xcor_scores_input.get_id(), self.xcor_scores_input.name)

	def process(self):

		# Define output directory
		peaks_dirname = "peaks_spp"
		if not os.path.exists(peaks_dirname): os.makedirs(peaks_dirname)

		# Define output filenames
		prefix = self.experiment.name.rstrip('.gz').rstrip('.tagAlign')
		self.peaks_fn       = prefix + '.regionPeak'
		self.final_peaks_fn = self.peaks_fn + '.gz'
		self.xcor_plot_fn   = prefix + '.pdf'
		self.xcor_scores_fn = prefix + '.ccscores'
		self.fixed_peaks_fn = prefix + '.fixcoord.regionPeak'

		# fragment length is third column in cross-correlation input file
		fragment_length = int(open(self.xcor_scores_input.name, 'r').readline().split('\t')[2])
		print "Read fragment length: %d" % fragment_length

		# install SPP
		spp_tarball = '/phantompeakqualtools/spp_1.10.1.tar.gz'
		run_spp = '/phantompeakqualtools/run_spp_nodups.R' if self.nodups else '/phantompeakqualtools/run_spp.R'
		print subprocess.check_output(shlex.split('R CMD INSTALL %s' % spp_tarball), stderr=subprocess.STDOUT)

		# run SPP
		spp_command = "Rscript %s -p=%d -c=%s -i=%s -npeak=%d -speak=%d -savr=%s -savp=%s -rf -out=%s" % (run_spp, cpu_count(), experiment_filename, control_filename, npeaks, fragment_length, peaks_filename, xcor_plot_filename, xcor_scores_filename)
		print spp_command
		process = subprocess.Popen(shlex.split(spp_command), stderr=subprocess.STDOUT, stdout=subprocess.PIPE)
		for line in iter(process.stdout.readline, ''):
			sys.stdout.write(line)

		# various fixes to ensure that coordinates fall within chr boundaries and are in the correct format
		common.run_pipe([
		        "gzip -dc %s" % self.final_peaks_fn,
			"tee %s" % self.peaks_fn,
			r"""awk 'BEGIN{OFS="\t"}{print $1,sprintf("%i",$2),sprintf("%i",$3),$4,$5,$6,$7,$8,$9,$10}'""",
			'slopBed -i stdin -g %s -b 0' % self.chrom_sizes.name,
			'bedClip stdin %s %s' % (self.chrom_sizes.name, self.fixed_peaks_fn) ])

	def upload(self, uploader):

                # Upload peaks
		print subprocess.check_output(shlex.split("gzip %s" % self.fixed_peaks_fn))
		self.peaks = uploader.upload(self.fixed_peaks_fn + ".gz")

		# Information about called peaks
		n_spp_peaks = common.count_lines(self.peaks_fn)
		print "%s peaks called by spp" % n_spp_peaks
		print "%s of those peaks removed due to bad coordinates" % (n_spp_peaks - common.count_lines(self.fixed_peaks_fn))
		print "First 50 peaks"
		print subprocess.check_output('head -50 %s' % self.fixed_peaks_fun, shell=True, stderr=subprocess.STDOUT)

		# Upload bigBed if applicable
		if self.bigbed:
			self.peaks_bb_fn = common.bed2bb(self.fixed_peaks_fn, self.chrom_sizes.name, self.as_file.name)
			if self.peaks_bb_fn:
				self.peaks_bb = uploader.upload(self.peaks_bb_fn)

		if not filecmp.cmp(self.peaks_fn, self.fixed_peaks_fn): print "Returning peaks with fixed coordinates"

		# Upload cross-correlations
                self.xcor_plot   = uploader.upload(self.xcor_plot)
	        self.xcor_scores = uploader.upload(self.xcor_scores)

	def output(self, linker):

		output = {
			"peaks": linker.link(self.peaks),
			"xcor_plot": linker.link(xcor_plot),
			"xcor_scores": linker.link(xcor_scores) }
		if self.bigbed and self.peaks_bb_fn: output["peaks_bb"] = linker.link(self.peaks_bb)
		return output
