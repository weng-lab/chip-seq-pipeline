#!/usr/bin/env python

import os, time

sys.path.append(os.path.join(os.path.dirname(__file__), '../../dnanexus'))
import common

class Macs2PeakCaller(object):
        def __init__(self, experiment, control, xcor_scores_input, chrom_sizes,
                     narrowpeak_as, gappedpeak_as, broadpeak_as, genomesize, File):

                # Initialize data object inputs on the platform
                # into dxpy.DXDataObject instances.

                self.experiment        = File.init(experiment)
                self.control           = File.init(control)
                self.xcor_scores_input = File.init(xcor_scores_input)
                self.chrom_sizes       = File.init(chrom_sizes)
                self.narrowPeak_as     = File.init(narrowpeak_as)
                self.gappedPeak_as     = File.init(gappedpeak_as)
                self.broadPeak_as      = File.init(broadpeak_as)

        def download(self, downloader):
                # Download the file inputs to the local file system
                # and use their own filenames.

                downloader.download(self.experiment.get_id(),        self.experiment.name)
                downloader.download(self.control.get_id(),           self.control.name)
                downloader.download(self.xcor_scores_input.get_id(), self.xcor_scores_input.name)
                downloader.download(self.chrom_sizes.get_id(),       self.chrom_sizes.name)
                downloader.download(self.narrowPeak_as.get_id(),     self.narrowPeak_as.name)
                downloader.download(self.gappedPeak_as.get_id(),     self.gappedPeak_as.name)
                downloader.download(self.broadPeak_as.get_id(),      self.broadPeak_as.name)

        def process(self):
                #Define the output filenames

                peaks_dirname = 'peaks_macs'
                if not os.path.exists(peaks_dirname):
                        os.makedirs(peaks_dirname)
                prefix = self.experiment.name
                if prefix.endswith('.gz'):
                        prefix = prefix[:-3]

                narrowPeak_fn    = "%s/%s.narrowPeak" %(peaks_dirname, prefix)
                gappedPeak_fn    = "%s/%s.gappedPeak" %(peaks_dirname, prefix)
                broadPeak_fn     = "%s/%s.broadPeak"  %(peaks_dirname, prefix)
                self.narrowPeak_gz_fn = narrowPeak_fn + ".gz"
                self.gappedPeak_gz_fn = gappedPeak_fn + ".gz"
                self.broadPeak_gz_fn  = broadPeak_fn  + ".gz"
                self.narrowPeak_bb_fn = "%s.bb" %(narrowPeak_fn)
                self.gappedPeak_bb_fn = "%s.bb" %(gappedPeak_fn)
                self.broadPeak_bb_fn  = "%s.bb" %(broadPeak_fn)
                self.fc_signal_fn     = "%s/%s.fc_signal.bw"     %(peaks_dirname, prefix)
                self.pvalue_signal_fn = "%s/%s.pvalue_signal.bw" %(peaks_dirname, prefix)

                #Extract the fragment length estimate from column 3 of the cross-correlation scores file
                with open(self.xcor_scores_input.name,'r') as fh:
                        firstline = fh.readline()
                        fraglen = firstline.split()[2] #third column
                        print "Fraglen %s" %(fraglen)

                #===========================================
                # Generate narrow peaks and preliminary signal tracks
                #============================================

                command = 'macs2 callpeak ' + \
                                  '-t %s -c %s ' %(self.experiment.name, self.control.name) + \
                                  '-f BED -n %s/%s ' %(peaks_dirname, prefix) + \
                                  '-g %s -p 1e-2 --nomodel --shift 0 --extsize %s --keep-dup all -B --SPMR' %(genomesize, fraglen)
                print command
                returncode = common.block_on(command)
                print "MACS2 exited with returncode %d" %(returncode)
                assert returncode == 0, "MACS2 non-zero return"

                # Rescale Col5 scores to range 10-1000 to conform to narrowPeak.as format (score must be <1000)
                rescaled_narrowpeak_fn = common.rescale_scores('%s/%s_peaks.narrowPeak' %(peaks_dirname, prefix), scores_col=5)

                # Sort by Col8 in descending order and replace long peak names in Column 4 with Peak_<peakRank>
                pipe = ['sort -k 8gr,8gr %s' %(rescaled_narrowpeak_fn),
                                r"""awk 'BEGIN{OFS="\t"}{$4="Peak_"NR ; print $0}'""",
                                'tee %s' %(narrowPeak_fn),
                                'gzip -c']
                print pipe
                out,err = common.run_pipe(pipe,'%s' %(self.narrowPeak_gz_fn))

                # remove additional files
                #rm -f ${PEAK_OUTPUT_DIR}/${CHIP_TA_PREFIX}_peaks.xls ${PEAK_OUTPUT_DIR}/${CHIP_TA_PREFIX}_peaks.bed ${peakFile}_summits.bed

                #===========================================
                # Generate Broad and Gapped Peaks
                #============================================

                command = 'macs2 callpeak ' + \
                                  '-t %s -c %s ' %(self.experiment.name, self.control.name) + \
                                  '-f BED -n %s/%s ' %(peaks_dirname, prefix) + \
                                  '-g %s -p 1e-2 --broad --nomodel --shift 0 --extsize %s --keep-dup all' %(genomesize, fraglen)
                print command
                returncode = common.block_on(command)
                print "MACS2 exited with returncode %d" %(returncode)
                assert returncode == 0, "MACS2 non-zero return"

                # Rescale Col5 scores to range 10-1000 to conform to narrowPeak.as format (score must be <1000)
                rescaled_broadpeak_fn = common.rescale_scores('%s/%s_peaks.broadPeak' %(peaks_dirname, prefix), scores_col=5)

                # Sort by Col8 (for broadPeak) or Col 14(for gappedPeak)  in descending order and replace long peak names in Column 4 with Peak_<peakRank>
                pipe = ['sort -k 8gr,8gr %s' %(rescaled_broadpeak_fn),
                                r"""awk 'BEGIN{OFS="\t"}{$4="Peak_"NR ; print $0}'""",
                                'tee %s' %(broadPeak_fn),
                                'gzip -c']
                print pipe
                out,err = common.run_pipe(pipe,'%s' %(self.broadPeak_gz_fn))

                # Rescale Col5 scores to range 10-1000 to conform to narrowPeak.as format (score must be <1000)
                rescaled_gappedpeak_fn = common.rescale_scores('%s/%s_peaks.gappedPeak' %(peaks_dirname, prefix), scores_col=5)

                pipe = ['sort -k 14gr,14gr %s' %(rescaled_gappedpeak_fn),
                                r"""awk 'BEGIN{OFS="\t"}{$4="Peak_"NR ; print $0}'""",
                                'tee %s' %(gappedPeak_fn),
                                'gzip -c']
                print pipe
                out,err = common.run_pipe(pipe,'%s' %(self.gappedPeak_gz_fn))

                # remove additional files
                #rm -f ${PEAK_OUTPUT_DIR}/${CHIP_TA_PREFIX}_peaks.xls ${PEAK_OUTPUT_DIR}/${CHIP_TA_PREFIX}_peaks.bed ${peakFile}_summits.bed

                #===========================================
                # For Fold enrichment signal tracks
                #============================================

                # This file is a tab delimited file with 2 columns Col1 (chromosome name), Col2 (chromosome size in bp).

                command = 'macs2 bdgcmp ' + \
                                  '-t %s/%s_treat_pileup.bdg ' %(peaks_dirname, prefix) + \
                                  '-c %s/%s_control_lambda.bdg ' %(peaks_dirname, prefix) + \
                                  '--outdir %s -o %s_FE.bdg ' %(peaks_dirname, prefix) + \
                                  '-m FE'
                print command
                returncode = common.block_on(command)
                print "MACS2 exited with returncode %d" %(returncode)
                assert returncode == 0, "MACS2 non-zero return"

                # Remove coordinates outside chromosome sizes (stupid MACS2 bug)
                pipe = ['slopBed -i %s/%s_FE.bdg -g %s -b 0' %(peaks_dirname, prefix, self.chrom_sizes.name),
                                'bedClip stdin %s %s/%s.fc.signal.bedgraph' %(self.chrom_sizes.name, peaks_dirname, prefix)]
                print pipe
                out, err = common.run_pipe(pipe)

                #rm -f ${PEAK_OUTPUT_DIR}/${CHIP_TA_PREFIX}_FE.bdg

                # Convert bedgraph to bigwig
                command = 'bedGraphToBigWig ' + \
                                  '%s/%s.fc.signal.bedgraph ' %(peaks_dirname, prefix) + \
                                  '%s ' %(self.chrom_sizes.name) + \
                                  '%s' %(self.fc_signal_fn)
                print command
                returncode = common.block_on(command)
                print "bedGraphToBigWig exited with returncode %d" %(returncode)
                assert returncode == 0, "bedGraphToBigWig non-zero return"
                #rm -f ${PEAK_OUTPUT_DIR}/${CHIP_TA_PREFIX}.fc.signal.bedgraph

                #===========================================
                # For -log10(p-value) signal tracks
                #============================================

                # Compute sval = min(no. of reads in ChIP, no. of reads in control) / 1,000,000

                out, err = common.run_pipe([
                        'gzip -dc %s' %(self.experiment.name),
                        'wc -l'])
                chipReads = out.strip()
                out, err = common.run_pipe([
                        'gzip -dc %s' %(self.control.name),
                        'wc -l'])
                controlReads = out.strip()
                sval=str(min(float(chipReads), float(controlReads))/1000000)

                print "chipReads = %s, controlReads = %s, sval = %s" %(chipReads, controlReads, sval)

                returncode = common.block_on(
                        'macs2 bdgcmp ' + \
                        '-t %s/%s_treat_pileup.bdg ' %(peaks_dirname, prefix) + \
                        '-c %s/%s_control_lambda.bdg ' %(peaks_dirname, prefix) + \
                        '--outdir %s -o %s_ppois.bdg ' %(peaks_dirname, prefix) + \
                        '-m ppois -S %s' %(sval))
                print "MACS2 exited with returncode %d" %(returncode)
                assert returncode == 0, "MACS2 non-zero return"

                # Remove coordinates outside chromosome sizes (stupid MACS2 bug)
                pipe = ['slopBed -i %s/%s_ppois.bdg -g %s -b 0' %(peaks_dirname, prefix, self.chrom_sizes.name),
                                'bedClip stdin %s %s/%s.pval.signal.bedgraph' %(self.chrom_sizes.name, peaks_dirname, prefix)]
                print pipe
                out, err = common.run_pipe(pipe)

                #rm -rf ${PEAK_OUTPUT_DIR}/${CHIP_TA_PREFIX}_ppois.bdg

                # Convert bedgraph to bigwig
                command = 'bedGraphToBigWig ' + \
                                  '%s/%s.pval.signal.bedgraph ' %(peaks_dirname, prefix) + \
                                  '%s ' %(self.chrom_sizes.name) + \
                                  '%s' %(self.pvalue_signal_fn)
                print command
                returncode = common.block_on(command)
                print "bedGraphToBigWig exited with returncode %d" %(returncode)
                assert returncode == 0, "bedGraphToBigWig non-zero return"

                #rm -f ${PEAK_OUTPUT_DIR}/${CHIP_TA_PREFIX}.pval.signal.bedgraph
                #rm -f ${PEAK_OUTPUT_DIR}/${CHIP_TA_PREFIX}_treat_pileup.bdg ${peakFile}_control_lambda.bdg

                #===========================================
                # Generate bigWigs from beds to support trackhub visualization of peak files
                #============================================

                narrowPeak_bb_fname = common.bed2bb('%s' %(narrowPeak_fn), self.chrom_sizes.name, self.narrowpeak_as.name, bed_type='bed6+4')
                gappedPeak_bb_fname = common.bed2bb('%s' %(gappedPeak_fn), self.chrom_sizes.name, self.gappedPeak_as.name, bed_type='bed12+3')
                broadPeak_bb_fname =  common.bed2bb('%s' %(broadPeak_fn),  self.chrom_sizes.name, self.broadPeak_as.name,  bed_type='bed6+3')

                #Temporary during development to create empty files just to get the applet to exit
                for fn in [narrowPeak_fn, gappedPeak_fn, broadPeak_fn,
                           self.narrowPeak_bb_fn, self.gappedPeak_bb_fn,
                           self.broadPeak_bb_fn,
                           self.fc_signal_fn, self.pvalue_signal_fn]:
                        common.block_on('touch %s' %(fn))

        def upload(self, uploader):
                # Upload the file outputs

                self.narrowPeak    = uploader.upload(self.narrowPeak_gz_fn)
                self.gappedPeak    = uploader.upload(self.gappedPeak_gz_fn)
                self.broadPeak     = uploader.upload(self.broadPeak_gz_fn)
                self.narrowPeak_bb = uploader.upload(self.narrowPeak_bb_fn)
                self.gappedPeak_bb = uploader.upload(self.gappedPeak_bb_fn)
                self.broadPeak_bb  = uploader.upload(self.broadPeak_bb_fn)
                self.fc_signal     = uploader.upload(self.fc_signal_fn)
                self.pvalue_signal = uploader.upload(self.pvalue_signal_fn)

        def output(self, linker):

                # Build the output structure.

                output = {
                        "narrowpeaks":    linker.link(self.narrowPeak),
                        "gappedpeaks":    linker.link(self.gappedPeak),
                        "broadpeaks":     linker.link(self.broadPeak),
                        "narrowpeaks_bb": linker.link(self.narrowPeak_bb),
                        "gappedpeaks_bb": linker.link(self.gappedPeak_bb),
                        "broadpeaks_bb":  linker.link(self.broadPeak_bb),
                        "fc_signal":     linker.link(self.fc_signal),
                        "pvalue_signal": linker.link(self.pvalue_signal)
                }

                return output
