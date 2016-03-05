#!/usr/bin/env python
# pseudoreplicator 0.0.1

import os, subprocess, shlex, time, re, gzip
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

class Psr(object):
    @staticmethod
    def process(input_tags, File, downloader, uploader, linker):

        # The following line(s) initialize your data object inputs on the platform
        # into dxpy.DXDataObject instances t5Chat you can start using immediately.
        input_tags_file = File.init(input_tags)

        # The following line(s) download your file inputs to the local file system
        # using variable names for the filenames.

        input_tags_filename = input_tags_file.name
        downloader.download(input_tags_file.get_id(), input_tags_filename)

        # introspect the file to determine tagAlign (thus SE) or BEDPE (thus PE)
        # strip extension as appropriate

        print subprocess.check_output('ls', shell=True)
        #out = subprocess.check_output('gzip -dc %s | head -n 1' %(input_tags_filename), shell=True)
        #out,err = run_pipe(['gzip -dc %s' %(input_tags_filename), 'sed -n 1p'])
        with gzip.open(input_tags_filename) as f:
            firstline = f.readline()
        print firstline

        se_cols = 6
        pe_cols = 10
        if re.match('^(\S+[\t\n]){%d}$' %(se_cols), firstline):
            paired_end = False
            input_tags_basename = input_tags_filename.rstrip('.tagAlign.gz')
            filename_infix = 'SE'
            print "Single-end data"
        #elif re.match('^([a-zA-Z0-9_:-+]+[\t\n]){%d}$' %(pe_cols), firstline):
        elif re.match('^(\S+[\t\n]){%d}$' %(pe_cols), firstline):
            paired_end = True
            input_tags_basename = input_tags_filename.rstrip('.bedpe.gz')
            filename_infix = 'PE2SE'
            print "Paired-end data"
        else:
            raise IOError("%s is neither a BEDPE or tagAlign file" %(input_tags_filename))

        pr_ta_filenames = [input_tags_basename + ".%s.pr1.tagAlign.gz" %(filename_infix),
                           input_tags_filename + ".%s.pr2.tagAlign.gz" %(filename_infix)]
##Are we sure that the two names hvae to be different? shouldn't both be input_tags_basename?
        #count lines in the file
        out,err = run_pipe([
            'gzip -dc %s' %(input_tags_filename),
            'wc -l'])
        #number of lines in each split
        nlines = (int(out)+1)/2
        # Shuffle and split BEDPE file into 2 equal parts
        splits_prefix = 'temp_split'
        out,err = run_pipe([
            'gzip -dc %s' %(input_tags_filename),
            'shuf',
            'split -a 2 -d -l %d - %s' %(nlines, splits_prefix)]) # gives two files named splits_prefix0n, n=1,2
        # Convert read pairs to reads into standard tagAlign file
        for i,index in enumerate(['00', '01']): #this should be made multi-threaded
            steps = ['cat %s' %(splits_prefix+index)]
            if paired_end:
                steps.extend([r"""awk 'BEGIN{OFS="\t"}{printf "%s\t%s\t%s\tN\t1000\t%s\n%s\t%s\t%s\tN\t1000\t%s\n",$1,$2,$3,$9,$4,$5,$6,$10}'"""])
            steps.extend(['gzip -c'])
            out,err = run_pipe(steps, outfile=pr_ta_filenames[i])

        pseudoreplicate1_file = uploader.upload(pr_ta_filenames[0])
        pseudoreplicate2_file = uploader.upload(pr_ta_filenames[1])

        # Return the outputs.

        output = {}
        output["pseudoreplicate1"] = linker.link(pseudoreplicate1_file)
        output["pseudoreplicate2"] = linker.link(pseudoreplicate2_file)

        return output

