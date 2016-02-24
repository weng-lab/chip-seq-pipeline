#!/usr/bin/env python

import os, subprocess, shlex, time
from multiprocessing import Pool, cpu_count
from subprocess import Popen, PIPE #debug only this should only need to be imported into run_pipe

def run_pipe(steps, outfile=None):
	#break this out into a recursive function
	#TODO:  capture stderr
	from subprocess import Popen, PIPE
	p = None
	p_next = None
	first_step_n = 1
	last_step_n = len(steps)
	for n,step in enumerate(steps, start=first_step_n):
		print "step %d: %s" %(n,step)
		if n == first_step_n:
			if n == last_step_n and outfile: #one-step pipeline with outfile
				with open(outfile, 'w') as fh:
					print "one step shlex: %s to file: %s" %(shlex.split(step), outfile)
					p = Popen(shlex.split(step), stdout=fh)
				break
			print "first step shlex to stdout: %s" %(shlex.split(step))
			p = Popen(shlex.split(step), stdout=PIPE)
			#need to close p.stdout here?
		elif n == last_step_n and outfile: #only treat the last step specially if you're sending stdout to a file
			with open(outfile, 'w') as fh:
				print "last step shlex: %s to file: %s" %(shlex.split(step), outfile)
				p_last = Popen(shlex.split(step), stdin=p.stdout, stdout=fh)
				p.stdout.close()
				p = p_last
		else: #handles intermediate steps and, in the case of a pipe to stdout, the last step
			print "intermediate step %d shlex to stdout: %s" %(n,shlex.split(step))
			p_next = Popen(shlex.split(step), stdin=p.stdout, stdout=PIPE)
			p.stdout.close()
			p = p_next
	out,err = p.communicate()
	return out,err

def Xcor(object):
    def __init__(self, input_bam, paired_end):
            
	# The following line(s) initialize your data object inputs on the platform
	# into dxpy.DXDataObject instances that you can start using immediately.

	   self.input_bam_file = File.init(input_bam)
       self.input_bam_filename = self.input_bam_file.name
	   self.input_bam_basename = self.input_bam_file.name.rstrip('.bam')
           
	# The following line(s) download your file inputs to the local file system
	# using variable names for the filenames.
    def download(self, downloader):
       downloader.download(self.input_bam_file.get_id(), self.input_bam_filename)

    def process(self):
	   self.intermediate_TA_filename = self.input_bam_basename + ".tagAlign"
	   if paired_end:
		  end_infix = 'PE2SE'
	   else:
		  end_infix = 'SE'
	   self.final_TA_filename = self.input_bam_basename + '.' + end_infix + '.tagAlign.gz'

	# ===================
	# Create tagAlign file
	# ===================

       out,err = run_pipe([
            "bamToBed -i %s" %(self.input_bam_filename),
            r"""awk 'BEGIN{OFS="\t"}{$4="N";$5="1000";print $0}'""",
            "tee %s" %(self.intermediate_TA_filename),
            "gzip -c"],
            outfile=self.final_TA_filename)
        print subprocess.check_output('ls -l', shell=True)

	# ================
	# Create BEDPE file
	# ================
        if paired_end:
            self.final_BEDPE_filename = self.input_bam_basename + ".bedpe.gz"
            #need namesorted bam to make BEDPE
            final_nmsrt_bam_prefix = self.input_bam_basename + ".nmsrt"
            self.final_nmsrt_bam_filename = self.final_nmsrt_bam_prefix + ".bam"
            subprocess.check_call(shlex.split("samtools sort -n %s %s" %(self.input_bam_filename, self.final_nmsrt_bam_prefix)))
            out,err = run_pipe([
                "bamToBed -bedpe -mate1 -i %s" %(self.final_nmsrt_bam_filename),
                "gzip -c"],
                outfile=self.final_BEDPE_filename)
            print subprocess.check_output('ls -l', shell=True)

        # =================================
        # Subsample tagAlign file
        # ================================
        NREADS=15000000
        if paired_end:
            end_infix = 'MATE1'
        else:
            end_infix = 'SE'
        subsampled_TA_filename = self.input_bam_basename + ".filt.nodup.sample.%d.%s.tagAlign.gz" %(NREADS/1000000, end_infix)
        steps = [
            'grep -v "chrM" %s' %(self.intermediate_TA_filename),
            'shuf -n %d' %(NREADS)]
        if paired_end:
            steps.extend([r"""awk 'BEGIN{OFS="\t"}{$4="N";$5="1000";print $0}'"""])
        steps.extend(['gzip -c'])
        out,err = run_pipe(steps,outfile=self.subsampled_TA_filename)
        print subprocess.check_output('ls -l', shell=True)

        # Calculate Cross-correlation QC scores
        self.CC_scores_filename = self.subsampled_TA_filename + ".cc.qc"
        self.CC_plot_filename = self.subsampled_TA_filename + ".cc.plot.pdf"

        # CC_SCORE FILE format
        # Filename <tab> numReads <tab> estFragLen <tab> corr_estFragLen <tab> PhantomPeak <tab> corr_phantomPeak <tab> argmin_corr <tab> min_corr <tab> phantomPeakCoef <tab> relPhantomPeakCoef <tab> QualityTag

        #run_spp_command = subprocess.check_output('which run_spp.R', shell=True)
        spp_tarball = '/phantompeakqualtools/spp_1.10.1.tar.gz'
        run_spp_command = '/phantompeakqualtools/run_spp_nodups.R'
        #install spp
        print subprocess.check_output(shlex.split('R CMD INSTALL %s' %(spp_tarball)))
        out,err = run_pipe([
            "Rscript %s -c=%s -p=%d -filtchr=chrM -savp=%s -out=%s" \
                %(run_spp_command, self.subsampled_TA_filename, cpu_count(), self.CC_plot_filename, self.CC_scores_filename)])
        print subprocess.check_output('ls -l', shell=True)
        out,err = run_pipe([
            r"""sed -r  's/,[^\t]+//g' %s""" %(self.CC_scores_filename)],
            outfile="temp")
        out,err = run_pipe([
            "mv temp %s" %(self.CC_scores_filename)])
            
    def upload(self,uploader):
        self.tagAlign_file = uploader.upload(self.final_TA_filename)
        # if not paired_end:
        #     final_BEDPE_filename = 'SE_so_no_BEDPE'
        #     subprocess.check_call('touch %s' %(final_BEDPE_filename), shell=True)
        if paired_end:
            self.BEDPE_file = uploader.upload(self.final_BEDPE_filename)

        self.CC_scores_file = uploader.upload(self.CC_scores_filename)
        self.CC_plot_file = uploader.upload(self.CC_plot_filename)
        
        
    def output(self,linker):
        # Return the outputs
        
        output = {}
        output["tagAlign_file"] = linker.link(self.tagAlign_file)
        if paired_end:
            output["BEDPE_file"] = linker.link(self.BEDPE_file)
        output["CC_scores_file"] = linker.link(self.CC_scores_file)
        output["CC_plot_file"] = linker.link(self.CC_plot_file)
        output["paired_end"] = paired_end

        return output

