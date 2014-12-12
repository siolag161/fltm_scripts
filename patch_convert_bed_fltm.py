#!/usr/bin/python

import sys,os
import glob
import argparse
import subprocess
import ntpath

import multiprocessing
from datetime import datetime
################
def display(time_diff):
    s = time_diff.seconds
    return '{:02}:{:02}:{:02}'.format(s // 3600, s % 3600 // 60, s % 60)

def process_bed_files(bf, script_path, plink_path, out_dir):
    subprocess.call([script_path, "-p", plink_path, "-i", bf, "-o", out_dir, "-g", "2"])

def get_bed_files(in_dir):
    pattern = os.path.join(in_dir, "*.bed")
    bed_files = glob.glob(pattern)
    for bf in bed_files:
	basename = os.path.basename(bf)
	yield os.path.splitext(basename)[0]

# todo, if evolved then change also
if __name__ == "__main__":
    parser = argparse.ArgumentParser()
   
    parser.add_argument( '-i', '--rac_dir',
		        help='input RAC dir', required=True )
    parser.add_argument( '-s', '--script_path', 
			 help='link to processing script', required=True )    
    parser.add_argument( '-p', '--plink_path', 
			 help='link to plink', required=True )
    parser.add_argument( '-o', '--out_dir', 
			 help='out dir', required=True )

    args = parser.parse_args()

    inDir = args.rac_dir
    outDir = args.out_dir
    script_path = args.script_path
    plink_path = args.plink_path

    start = datetime.now()
    processes = []
    for bf in get_bed_files(inDir):
	filepath_we = os.path.join(inDir, bf)
	process = multiprocessing.Process( target=process_bed_files,
					 	 args=(filepath_we, script_path, plink_path, outDir))
	process.bf = bf
	processes.append()

    for process in processes:
	process.start()
	
    for process in processes:
	process.join()
	elapsed = datetime.now() - start
	statOut = os.path.join(outDir, "stats_%s.log" % process.bf)
	#l =
	with open(statOut, "w") as f:
	    print >> f, "time taken: %s" % display(elapsed)

    globalElapsed = datetime.now() - start
    statOut = os.path.join(outDir, "stats.log")
    with open(statOut, "w") as f:
	print >> f, "total time taken: %s" % display(globalElapsed)
