#!/usr/bin/python

import sys,os
import glob
import argparse
import subprocess
import ntpath

import multiprocessing

from datetime import datetime
################

'''	-i [ --dinput ]     	Data Input filename
	-l [ --lpinput ]    	Label-Pos Input filename
	-o [ --outputDir ]  	Output Directory
	-s [ --simi ]       	Similarity
	-m [ --maxDist ]    	Max Distance
	-c [ --configFile ] 	configFile
'''

def display(time_diff):
    s = time_diff.seconds
    return '{:02}:{:02}:{:02}'.format(s // 3600, s % 3600 // 60, s % 60)

algos = [0,1,2]

def main():    
    parser = argparse.ArgumentParser()
    
    parser.add_argument( '-m', '--mode',
			 help='patch mode. (0) genome mode (1) params mode ', required=True )
    
    parser.add_argument( '-d', '--in_dir',
			 help='genome genotype directory', required=True )
    
    parser.add_argument( '-g', '--genotype_file',
			 help='genotype filename o', required=True )
    parser.add_argument( '-l', '--label_file',
			 help='label filename o', required=True )

    parser.add_argument( '-c', '--in_card', 
			 help='input cardinality', required=True )    
    parser.add_argument( '-e', '--fltm', 
			 help='path to executable fltm program', required=True )    
    parser.add_argument( '-s', '--simi', 
			 help='similarity, -1 real, 1 binary', required=True )    
    parser.add_argument( '-x', '--max_dist', 
			 help='max distance', required=True ) 
    parser.add_argument( '-A', '--algo_config', 
			 help='algo config_file', required=True )
    parser.add_argument( '-F', '--fltm_config', 
			 help='fltm config_file', required=True )
    parser.add_argument( '-o', '--out_dir', 
			 help='out dir', required=True )

    parser.add_argument( '-p', '--processes', 
			 help='nbr processes', required=True )

    args = parser.parse_args()
    
    args.out_dir = add_time(args.out_dir)

    mode = args.mode
    if mode == 0: # genome patching
	process_args = process_args_seq(args)
    else:
	process_args = process_args_seq(args)

    pool = multiprocessing.Pool(processes = int(args.processes))
    
    # print len(process_args)
    # for arg in process_args:
    start = datetime.now()
    results = pool.map(fltm_subprocess_proxy, process_args)
    pool.close() # no more tasks
    pool.join()  # wrap up current tasks
        
    statOut = os.path.join(out_dir, "stats.log")
    for idx, elapsed in enumerate(results):
	print process_args[idx], display(elapsed)
	with open(statOut, "a") as f:
	    print >> f, "%s executed. time taken: %s" % ( process_args[idx], display(elapsed))

    with open(statOut, "a") as f:
    	elapsed = datetime.now() - start
    	print >> f, "all executed. total time taken: %s" % (display(elapsed))
   

    print "List processing complete."

	
#################################################################################
class ClusteringAlgorithm:

    def __init__(self, **kwargs):
	self.params = { 'dbscan_minpts': 2, 'dbscan_eps': 0.5, 'cast_cast': 0.5, 'clust': 0 }
	algo = kwargs['algo']

	if algo == "DBSCAN":
	    self.params['clust'] = 0
	    self.params['dbscan_minpt'] = kwargs['minPts']
	    self.params['dbscan_eps'] = kwargs['eps']	    
	elif algo == "CAST":
	    self.params['clust'] = 1
	    self.params['cast_cast'] == kwargs['t']
	else:
	    self.params['clust'] = 2
	    
    def __str__(self):			    
	return "-c %(clust)s -M %(dbscan_minpts)s -E %(dbscan_eps)s -C %(cast_cast)s" % (self.params)

class FLTM:
    def __init__(self, **kwargs):
	self.params = kwargs
	
    def __str__(self):
	pattern = ''.join(['--%s %%(%s)s ' % (s,s) for s in self.params.keys()])
	return pattern % (self.params)

	
def fltm_one_input_source( fltm_path, dat_file, lab_file, algo, simi, max_dist ):
    pass
    simi = double(args.simi)    
    
def process_algo_patch(args):
    pass

def get_fltm_params(cfg_file):
    from xml.dom import minidom
    xmldoc = minidom.parse(cfg_file)
    params = xmldoc.getElementsByTagName('parameter')

    rs = []
    for param in params:
	fltm_params = {}
	args = param.getElementsByTagName('param')
	for arg in args:	    
	    name, val = arg.getAttribute('name'), arg.childNodes[0].data
	    fltm_params[name] = val
	fltm = FLTM(**fltm_params)
	rs.append(fltm)
    return rs

def get_algos(cfg_file):
    from xml.dom import minidom
    xmldoc = minidom.parse(cfg_file)
    algolist = xmldoc.getElementsByTagName('algorithm')
    algos = []
    for algo in algolist:
	kwargs = {}
	kwargs['algo'] = str(algo.getElementsByTagName('name')[0].childNodes[0].data)
	params = algo.getElementsByTagName('parameter')
	for param in params:
	    kwargs[param.getAttribute('name')] = param.childNodes[0].data
	algorithm = ClusteringAlgorithm(**kwargs)
	algos.append(algorithm)

    return algos

def get_input_files(in_dir):
    pattern = os.path.join(in_dir, "*_label.csv")
    lab_files = glob.glob(pattern)
    infiles = []
    for lf in lab_files:
	df = lf.replace("label", "data")
	infiles.append("-d %s -l %s" % (df, lf))
    return infiles

def fltm_subprocess(fltm_path, args_str):
    start = datetime.now()
    shell_command = "%s %s" %(fltm_path, args_str)
    subprocess.call( shell_command, shell=True)
    delta = datetime.now() - start
    print "------------------------------------------------ %s -------------------------------------" % delta
    return delta

def add_time(st):
    time_str = datetime.now().strftime("%Y_%m_%d_%H_%M_%S")
    return "%s_%s" % (st, time_str)

def fltm_subprocess_proxy(fltm_args):
    fltm_path, args_str = fltm_args.split(';')
    return fltm_subprocess(fltm_path, args_str)

def process_args_seq(args):
    process_args = []
    algos = get_algos(args.algo_config)
    fltm_params = get_fltm_params(args.fltm_config)
    inDir = args.in_dir
    fns = get_input_files(inDir)

    in_card = "-N %s" % (args.in_card)
    max_dist = "-x %s" % (args.max_dist)
    simi = "-t %s" % (args.simi)
    out_dir = "-o %s" % (args.out_dir)

    fltm_path = os.path.join(args.fltm, "fltm")
    print fltm_path
    for algo in algos:
	print algo, len(algos)
    	for fltm_param in fltm_params:
	    print fltm_param, len(fns)
	    for fn in fns:
	       args_str = " ".join([ in_card, max_dist, simi, str(fn), str(fltm_param), str(algo), out_dir])
	       # process = multiprocessing.Process( target=fltm_subprocess,
	       # 						 args=(fltm_path,args_str) )
	       # #process.command = "fltm %s" % args_str
	       # process_args.append(process)
	       arg = '%s;%s' % (fltm_path,args_str)
	       process_args.append(arg)
    return process_args
    
# todo, if evolved then change also
if __name__ == "__main__":
    main()
