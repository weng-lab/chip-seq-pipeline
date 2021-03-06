#!/usr/bin/env python
# pool 0.0.1


import os, subprocess, shlex, time, re, gzip
from os.path import splitext
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

class Pooler(object):
    @staticmethod
    def process(inputs, File, downloader, uploader, linker):
        input_filenames = []
        for input_file in inputs:
            dxf = File.init(input_file)
            input_filenames.append(dxf.name)
            downloader.download(dxf.get_id(), dxf.name)
        extension = splitext(splitext(input_filenames[-1])[0])[1] #uses last extension - presumably they are all the same
        pooled_filename = '-'.join([splitext(splitext(fn)[0])[0] for fn in input_filenames]) + "_pooled%s.gz" %(extension)
        out,err = run_pipe([
                'gzip -dc %s' %(' '.join(input_filenames)),
                'gzip -c'],
                           outfile=pooled_filename)

        pooled = uploader.upload(pooled_filename)

    # The following line fills in some basic dummy output and assumes
    # that you have created variables to represent your output with
    # the same name as your output fields.

        output = {}
        output["pooled"] = linker.link(pooled)

        return output


