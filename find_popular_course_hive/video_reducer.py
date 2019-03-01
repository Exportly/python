#!/usr/bin/env python

import sys 

videodict = {}

for line in sys.stdin: 
    line = line.strip() 
    courseId, videoPosition, count = line.split('\t') 

    try:
        videodict[courseId] = videodict[courseId] + ',' + videoPosition + ':' + count
    except:
        videodict[courseId] = videoPosition + ':' + count

for course in videodict.keys():
    print '%s [%s] '% ( course, videodict[course] )
    udata =  open('/Users/nest/Downloads/tsv_udemy/' +  course  + '.tsv', 'w') 
    udata.write('"[%s]"' % videodict[course]) 
    udata.close()
