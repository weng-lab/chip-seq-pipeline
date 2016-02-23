#!/usr/bin/env python

import sys, os, re

sys.path.append(os.path.join(os.path.dirname(__file__), '../../../dnanexus'))
import common

class OverlapPeaks(object):
        def __init__(self, rep1_peaks, rep2_peaks, pooled_peaks, pooledpr1_peaks, pooledpr2_peaks,
                     chrom_sizes, as_file, peak_type):
                self.rep1_peaks = rep1_peaks
                self.rep2_peaks = rep2_peaks
                self.pooled_peaks = pooled_peaks
                self.pooledpr1_peaks = pooledpr1_peaks
                self.pooledpr2_peaks = pooledpr2_peaks
                self.chrom_sizes = chrom_sizes
                self.as_file = as_file
                self.peak_type = peak_type

                #Input filenames - necessary to define each explicitly because input files could have the same name, in which case subsequent
                #file would overwrite previous file
                self.rep1_peaks_fn		= 'rep1-%s' %(rep1_peaks.name)
                self.rep2_peaks_fn		= 'rep2-%s' %(rep2_peaks.name)
                self.pooled_peaks_fn 	= 'pooled-%s' %(pooled_peaks.name)
                self.pooledpr1_peaks_fn	= 'pooledpr1-%s' %(pooledpr1_peaks.name)
                self.pooledpr2_peaks_fn	= 'pooledpr2-%s' %(pooledpr2_peaks.name)
                self.chrom_sizes_fn		= 'chrom.sizes'
                self.as_file_fn			= '%s.as' %(peak_type)

                # Output filenames
                m = re.match('(.*)(\.%s)+(\.((gz)|(Z)|(bz)|(bz2)))' %(peak_type), pooled_peaks.name) #strip off the peak and compression extensions
                if m:
                        basename = m.group(1)
                else:
                        basename = pooled_peaks.name
                self.overlapping_peaks_fn 	= '%s.replicated.%s' %(basename, peak_type)
                self.overlapping_peaks_bb_fn = self.overlapping_peaks_fn + '.bb'
                self.rejected_peaks_fn		= '%s.rejected.%s' %(basename, peak_type)
                self.rejected_peaks_bb_fn	= self.rejected_peaks_fn + '.bb'

                # Intermediate filenames
                self.overlap_tr_fn 	= 'replicated_tr.%s' %(peak_type)
                self.overlap_pr_fn 	= 'replicated_pr.%s' %(peak_type)

        def download(self, downloader):
                # Download file inputs to the local file system with local filenames
                downloader.download(self.rep1_peaks.get_id(), self.rep1_peaks_fn)
                downloader.download(self.rep2_peaks.get_id(), self.rep2_peaks_fn)
                downloader.download(self.pooled_peaks.get_id(), self.pooled_peaks_fn)
                downloader.download(self.pooledpr1_peaks.get_id(), self.pooledpr1_peaks_fn)
                downloader.download(self.pooledpr2_peaks.get_id(), self.pooledpr2_peaks_fn)
                downloader.download(self.chrom_sizes.get_id(), self.chrom_sizes_fn)
                downloader.download(self.as_file.get_id(), self.as_file_fn)

        def process(self):
                '''
                #find pooled peaks that are in (rep1 AND rep2)
                out, err = common.run_pipe([
                        'intersectBed -wa -f 0.50 -r -a %s -b %s' %(pooled_peaks_fn, rep1_peaks_fn),
                        'intersectBed -wa -f 0.50 -r -a stdin -b %s' %(rep2_peaks_fn)
                        ], overlap_tr_fn)
                print "%d peaks overlap with both true replicates" %(common.count_lines(overlap_tr_fn))

                #pooled peaks that are in (pooledpseudorep1 AND pooledpseudorep2)
                out, err = common.run_pipe([
                        'intersectBed -wa -f 0.50 -r -a %s -b %s' %(pooled_peaks_fn, pooledpr1_peaks_fn),
                        'intersectBed -wa -f 0.50 -r -a stdin -b %s' %(pooledpr2_peaks_fn)
                        ], overlap_pr_fn)
                print "%d peaks overlap with both pooled pseudoreplicates" %(common.count_lines(overlap_pr_fn))

                #combined pooled peaks in (rep1 AND rep2) OR (pooledpseudorep1 AND pooledpseudorep2)
                out, err = common.run_pipe([
                        'intersectBed -wa -a %s -b %s %s' %(pooled_peaks_fn, overlap_tr_fn, overlap_pr_fn),
                        'intersectBed -wa -u -a %s -b stdin' %(pooled_peaks_fn)
                        ], overlapping_peaks_fn)
                print "%d peaks overall with true replicates or with pooled pseudorepliates" %(common.count_lines(overlapping_peaks_fn))
                '''
                #the only difference between the peak_types is how the extra columns are handled
                if self.peak_type == "narrowPeak":
                        awk_command = r"""awk 'BEGIN{FS="\t";OFS="\t"}{s1=$3-$2; s2=$13-$12; if (($21/s1 >= 0.5) || ($21/s2 >= 0.5)) {print $0}}'"""
                        cut_command = 'cut -f 1-10'
                        bed_type = 'bed6+4'
                elif self.peak_type == "gappedPeak":
                        awk_command = r"""awk 'BEGIN{FS="\t";OFS="\t"}{s1=$3-$2; s2=$18-$17; if (($31/s1 >= 0.5) || ($31/s2 >= 0.5)) {print $0}}'"""
                        cut_command = 'cut -f 1-15'
                        bed_type = 'bed12+3'
                elif self.peak_type == "broadPeak":
                        awk_command = r"""awk 'BEGIN{FS="\t";OFS="\t"}{s1=$3-$2; s2=$12-$11; if (($19/s1 >= 0.5) || ($19/s2 >= 0.5)) {print $0}}'"""
                        cut_command = 'cut -f 1-9'
                        bed_type = 'bed6+3'
                else:
                        print "%s is unrecognized.  peak_type should be narrowPeak, gappedPeak or broadPeak."
                        sys.exit()

                # Find pooled peaks that overlap Rep1 and Rep2 where overlap is defined as the fractional overlap wrt any one of the overlapping peak pairs  > 0.5
                out, err = common.run_pipe([
                        'intersectBed -wo -a %s -b %s' %(self.pooled_peaks_fn, self.rep1_peaks_fn),
                        awk_command,
                        cut_command,
                        'sort -u',
                        'intersectBed -wo -a stdin -b %s' %(self.rep2_peaks_fn),
                        awk_command,
                        cut_command,
                        'sort -u'
                        ], self.overlap_tr_fn)
                print "%d peaks overlap with both true replicates" %(common.count_lines(self.overlap_tr_fn))

                # Find pooled peaks that overlap PseudoRep1 and PseudoRep2 where overlap is defined as the fractional overlap wrt any one of the overlapping peak pairs  > 0.5
                out, err = common.run_pipe([
                        'intersectBed -wo -a %s -b %s' %(self.pooled_peaks_fn, self.pooledpr1_peaks_fn),
                        awk_command,
                        cut_command,
                        'sort -u',
                        'intersectBed -wo -a stdin -b %s' %(self.pooledpr2_peaks_fn),
                        awk_command,
                        cut_command,
                        'sort -u'
                        ], self.overlap_pr_fn)
                print "%d peaks overlap with both pooled pseudoreplicates" %(common.count_lines(self.overlap_pr_fn))

                # Combine peak lists
                out, err = common.run_pipe([
                        'cat %s %s' %(self.overlap_tr_fn, self.overlap_pr_fn),
                        'sort -u'
                        ], self.overlapping_peaks_fn)
                print "%d peaks overlap with true replicates or with pooled pseudorepliates" %(common.count_lines(self.overlapping_peaks_fn))

                #rejected peaks
                out, err = common.run_pipe([
                        'intersectBed -wa -v -a %s -b %s' %(self.pooled_peaks_fn, self.overlapping_peaks_fn)
                        ], self.rejected_peaks_fn)
                print "%d peaks were rejected" %(common.count_lines(self.rejected_peaks_fn))

                self.npeaks_in 		= common.count_lines(common.uncompress(self.pooled_peaks_fn))
                self.npeaks_out 		= common.count_lines(self.overlapping_peaks_fn)
                self.npeaks_rejected = common.count_lines(self.rejected_peaks_fn)

                #make bigBed files for visualization
                self.overlapping_peaks_bb_fn = common.bed2bb(self.overlapping_peaks_fn, self.chrom_sizes_fn, self.as_file_fn, bed_type=bed_type)
                self.rejected_peaks_bb_fn 	= common.bed2bb(self.rejected_peaks_fn, self.chrom_sizes_fn, self.as_file_fn, bed_type=bed_type)

        def upload(self, uploader):
                # Upload file outputs from the local file system.

                self.overlapping_peaks 		= uploader.upload(common.compress(self.overlapping_peaks_fn))
                self.overlapping_peaks_bb 	= uploader.upload(self.overlapping_peaks_bb_fn)
                self.rejected_peaks 			= uploader.upload(common.compress(self.rejected_peaks_fn))
                self.rejected_peaks_bb 		= uploader.upload(self.rejected_peaks_bb_fn)

        def output(self, linker):
                # The following line fills in some basic dummy output and assumes
                # that you have created variables to represent your output with
                # the same name as your output fields.

                output = {
                        "overlapping_peaks" 	: linker.link(self.overlapping_peaks),
                        "overlapping_peaks_bb" 	: linker.link(self.overlapping_peaks_bb),
                        "rejected_peaks" 	: linker.link(self.rejected_peaks),
                        "rejected_peaks_bb" 	: linker.link(self.rejected_peaks_bb),
                        "npeaks_in" 		: self.npeaks_in,
                        "npeaks_out" 		: self.npeaks_out,
                        'npeaks_rejected' 	: self.npeaks_rejected
                }

                return output
