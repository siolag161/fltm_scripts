#!/usr/bin/python

import sys,os
import fnmatch
import argparse
import subprocess
import ntpath

import multiprocessing
from datetime import datetime
#######################################################

def main():
    parser = argparse.ArgumentParser()   
    parser.add_argument( '-i', '--lab_dir',
		        help='input lab dir', required=True )
    parser.add_argument( '-c', '--chr',
		        help='chromosome', required=True )
    args = parser.parse_args()
    labs = find_recursively_labs(args.lab_dir)    
    fix_label_files(labs, args.chr)


#######################################################
def find_recursively_labs(inputDir):
    matches = []
    for root, dirnames, filenames in os.walk(inputDir):
	for filename in fnmatch.filter(filenames, '*.lab'):
	    matches.append((root,filename))
    return matches


## convention root_dir = 
def fix_label_files(lab_files, chro):
    for root_dir, filename in lab_files:
	fix_lab_file(root_dir, filename, chro)
	
def fix_lab_file(root_dir, filename, chro):
    import csv
    file_path = os.path.join(root_dir,filename)
   
    with open(file_path, 'rb') as fn:
	reader = csv.reader(fn, delimiter=',', quotechar='\"')
	try:
	    frow = reader.next()	
	    if frow:
		if frow[1] == 0: return
	except StopIteration:
	    return

    rows = []
    idx = 0
    with open(file_path, 'rb') as fn:
	print file_path
	reader = csv.reader(fn, delimiter=',', quotechar='\"', skipinitialspace=True)
	for row in reader:
	    if len(row) == 4:
		r = [int(chro), idx] + [row[1], int(row[2]), int(row[3])]
	    elif len(row) == 5:
		r = [int(row[0]), idx] + [row[2], int(row[3]), (row[4]) ]
	    rows.append(r)
	    #print r, row, file_path
	    idx += 1

    #fn_bk = file_path + '_'
    # print fn_bk
    with open(file_path,'w') as fn:
      	writer = csv.writer(fn, delimiter=',', quotechar='"', quoting=csv.QUOTE_NONNUMERIC, skipinitialspace=True)
     	for row in rows:
	    writer.writerow(row)
     	#print row, file_path
  

########################################## MAIN ##########################################
#                                                                                        #
##########################################################################################
# todo, if evolved then change also
if __name__ == "__main__":
    main()
