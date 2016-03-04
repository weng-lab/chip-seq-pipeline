#!/usr/bin/env python
# filter_qc 0.0.1

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
					print "one step shlex: %s to file: %s" %(shlex.split(step),\
                                                                                         outfile)
					p = Popen(shlex.split(step), stdout=fh)
				break
			print "first step shlex to stdout: %s" %(shlex.split(step))
			p = Popen(shlex.split(step), stdout=PIPE)
			#need to close p.stdout here?
		elif n == last_step_n and outfile: #only treat the last step specially\
                            # if you're sending stdout to a file
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

class Fqc(object):
        def __init__(self, job_input):
                self.input_bam = job_input['input_bam']
                self.paired_end = job_input['paired_end']
                self.samtools_params = job_input['samtools_params']
                #there is input_JSON, it over-rides any explicit parameters
                # following line(s) initialize your data object inputs on the platform
                # into dxpy.DXDataObject instances that you can start using immediately.
                self.raw_bam_file = job_input['LocalFile'].init(self.input_bam)

        def process(self, downloader, uploader, linker):

                raw_bam_filename = self.raw_bam_file.name
                raw_bam_basename = self.raw_bam_file.name.rstrip('.bam')
                downloader.download(self.raw_bam_file.get_id(), raw_bam_filename)

                print subprocess.check_output('ls -l', shell=True)

                filt_bam_prefix = raw_bam_basename + ".filt.srt"
                filt_bam_filename = filt_bam_prefix + ".bam"
                if self.paired_end:
                        # =============================
                        # Remove  unmapped, mate unmapped
                        # not primary alignment, reads failing platform
                        # Remove low MAPQ reads
                        # Only keep properly paired reads
                        # Obtain name sorted BAM file
                        # ==================
                        tmp_filt_bam_prefix = "tmp.%s" %(filt_bam_prefix) #was tmp.prefix.nmsrt
                        tmp_filt_bam_filename = tmp_filt_bam_prefix + ".bam"
                        out,err = run_pipe([
                                #filter:  -F 1804 FlAG bits to exclude; -f 2 FLAG bits to reqire;\
                                #-q 30 exclude MAPQ < 30; -u uncompressed output
                                #exclude FLAG 1804: unmapped, next segment unmapped, secondary \
                                #alignments, not passing platform q, PCR or optical duplicates
                                #require FLAG 2: properly aligned
                                "samtools view -F 1804 -f 2 %s -u %s" %(self.samtools_params, raw_bam_filename),
                                #sort:  -n sort by name; - take input from stdin;\
                                        #out to specified filename
                                "samtools sort -n - %s" %(tmp_filt_bam_prefix)])  # Will produce name sorted BAM
                        if err:
                                #logger.error("samtools error: %s" %(err))
                                print "ERROR"
                                sys.exit(1)
                        # Remove orphan reads (pair was removed)
                        # and read pairs mapping to different chromosomes
                        # Obtain position sorted BAM
                        print subprocess.check_output('ls -l', shell=True)
                        out,err = run_pipe([
                                #fill in mate coordinates, ISIZE and mate-related flags
                                #fixmate requires name-sorted alignment; -r removes secondary and unmapped (redundant here because already done above?)
                                #- send output to stdout
                                "samtools fixmate -r %s -" %(tmp_filt_bam_filename),
                                #repeat filtering after mate repair
                                "samtools view -F 1804 -f 2 -u -",
                                #produce the coordinate-sorted BAM
                                "samtools sort - %s" %(filt_bam_prefix)])
                        print subprocess.check_output('ls -l', shell=True)
                else: #single-end data
                        # =============================
                        # Remove unmapped, mate unmapped
                        # not primary alignment, reads failing platform
                        # Remove low MAPQ reads
                        # Obtain name sorted BAM file
                        # ==================
                        with open(filt_bam_filename, 'w') as fh:
                                subprocess.check_call(shlex.split("samtools view -F 1804 %s -b %s"
                                                                  %(self.samtools_params, raw_bam_filename)), stdout=fh)
                # ========================
                # Mark duplicates
                # ======================
                tmp_filt_bam_filename = raw_bam_basename + ".dupmark.bam"
                dup_file_qc_filename = raw_bam_basename + ".dup.qc"
                subprocess.check_call(shlex.split(
                      "java -Xmx4G -jar /data/mattei/Programs/picard-tools-1.119/MarkDuplicates.jar \
                      INPUT=%s OUTPUT=%s METRICS_FILE=%s \
                      VALIDATION_STRINGENCY=LENIENT ASSUME_SORTED=true REMOVE_DUPLICATES=false"
                        %(filt_bam_filename, tmp_filt_bam_filename, dup_file_qc_filename)))
                os.rename(tmp_filt_bam_filename,filt_bam_filename)

                if self.paired_end:
                        final_bam_prefix = raw_bam_basename + ".filt.srt.nodup"
                else:
                        final_bam_prefix = raw_bam_basename + ".filt.nodup.srt"
                final_bam_filename = final_bam_prefix + ".bam" # To be stored
                final_bam_index_filename = final_bam_prefix + ".bai" # To be stored
                final_bam_file_mapstats_filename = final_bam_prefix + ".flagstat.qc" # QC file

                if self.paired_end:
                        # ============================
                        # Remove duplicates
                        # Index final position sorted BAM
                        # Create final name sorted BAM
                        # ============================
                        with open(final_bam_filename, 'w') as fh:
                                subprocess.check_call(shlex.split("samtools view -F 1804 -f2 -b %s"
                                                                  %(filt_bam_filename)), stdout=fh)
                        #namesorting is needed for bam->bedPE, so moved to xcor
                        #final_nmsrt_bam_prefix = raw_bam_basename + ".filt.nmsrt.nodup"
                        #final_nmsrt_bam_filename = final_nmsrt_bam_prefix + ".bam"
                        #subprocess.check_call(shlex.split("samtools sort -n %s %s" %(final_bam_filename, final_nmsrt_bam_prefix)))
                else:
                        # ============================
                        # Remove duplicates
                        # Index final position sorted BAM
                        # ============================
                        with open(final_bam_filename, 'w') as fh:
                                subprocess.check_call(shlex.split("samtools view -F 1804 -b %s"
                                                                  %(filt_bam_filename)), stdout=fh)
                # Index final bam file
                subprocess.check_call(shlex.split("samtools index %s %s" %(final_bam_filename, final_bam_index_filename)))
                # Generate mapping statistics
                with open(final_bam_file_mapstats_filename, 'w') as fh:
                        subprocess.check_call(shlex.split("samtools flagstat %s"
                                                          %(final_bam_filename)), stdout=fh)

                # =============================
                # Compute library complexity
                # =============================
                # Sort by name
                # convert to bedPE and obtain fragment coordinates
                # sort by position and strand
                # Obtain unique count statistics
                pbc_file_qc_filename = final_bam_prefix + ".pbc.qc"
                # PBC File output
                # TotalReadPairs [tab] DistinctReadPairs [tab] OneReadPair [tab] TwoReadPairs [tab]\
                # NRF=Distinct/Total [tab] PBC1=OnePair/Distinct [tab] PBC2=OnePair/TwoPair
                if self.paired_end:
                        steps = [
                        "samtools sort -no %s -" %(filt_bam_filename),
                        "bedtools bamtobed -bedpe -i stdin",
                        r"""awk 'BEGIN{OFS="\t"}{print $1,$2,$4,$6,$9,$10}'"""]
                else:
                        steps = [
                        "bedtools bamtobed -i %s" %(filt_bam_filename),
                        #for some reason 'bedtools bamtobed' does not work but bamToBed does
                        r"""awk 'BEGIN{OFS="\t"}{print $1,$2,$3,$6}'"""]
                # these st
                steps.extend([
                        "grep -v 'chrM'",
                        #TODO this should be implemented as an explicit list of allowable names, so that mapping can be done to a complete reference
                        "sort",
                        "uniq -c",
                        r"""awk 'BEGIN{mt=0;m0=0;m1=0;m2=0} \
                        ($1==1){m1=m1+1} \
                        ($1==2){m2=m2+1} \
                        {m0=m0+1} \
                        {mt=mt+$1} \
                        END{if(m1==0 && m2>0){res="-inf"}else if(m1>0 && m2==0){res="inf"}else{res=m1/m2}; \
                        printf "%d\t%d\t%d\t%d\t%f\t%f\t"res"\n",mt,m0,m1,m2,m0/mt,m1/m0}'"""
                        ])

                out,err = run_pipe(steps,pbc_file_qc_filename)
                if err:
                        print "PBC file error: %s" %(err)

                print "Uploading results files to the project"
                # Use the Python bindings to upload the file outputs to the project.
                filtered_bam = uploader.upload(final_bam_filename)
                filtered_bam_index = uploader.upload(final_bam_index_filename)
                filtered_mapstats = uploader.upload(final_bam_file_mapstats_filename)
                dup_file_qc = uploader.upload(dup_file_qc_filename)
                pbc_file_qc = uploader.upload(pbc_file_qc_filename)

                # Return links to the output files
                output = {
                        "filtered_bam": linker.link(filtered_bam),
                        "filtered_bam_index": linker.link(filtered_bam_index),
                        "filtered_mapstats": linker.link(filtered_mapstats),
                        "dup_file_qc": linker.link(dup_file_qc),
                        "pbc_file_qc": linker.link(pbc_file_qc),
                        "paired_end": self.paired_end
                }
                output.update({'output_JSON': output.copy()})

                print "Exiting with output: %s" %(output)
                return output
