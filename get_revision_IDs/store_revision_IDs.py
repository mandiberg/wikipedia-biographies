#!/usr/bin/python

import pandas as pd
import time
import numpy as np
import os


#untested.
#will read csvs, and store them in database by month
#all rev,time in revisions table
#page, rev (by month) pages table
#needs to repeat some revs in pages table 

start = time.time()

#regular 31.8s
#concurrent 

#declariing path and image before function, but will reassign in the main loop
#location of source and output files outside repo
ROOT= os.path.join(os.environ['HOME'], "Documents/projects-active/facemap_production") 
folder ="gettyimages"
http="https://media.gettyimages.com/photos/"
# folder ="files_for_testing"
outputfolder = os.path.join(ROOT,folder+"_output")

#comment this out to run testing mode with variables above
SOURCEFILE="_SELECT_FROM_faceimages_query_mouthopen.csv"
# SOURCEFILE="test2000.csv"


def touch(folder):
    if not os.path.exists(folder):
        os.makedirs(folder)
touch(outputfolder)

def get_dir_files(folder):
    # counter = 1

    # directory = folder
    directory = os.path.join(ROOT,folder)
    print(directory)

    meta_file_list = []
    os.chdir(directory)
    print(len(os.listdir(directory)))
    for filename in os.listdir(directory):
    # for item in os.listdir(root):
        # print (counter)

        if not filename.startswith('.') and os.path.isfile(os.path.join(directory, filename)):
            meta_file_list.append(filename)
    return meta_file_list






print("--- %s seconds ---" % (time.time() - start_time))
