from pyspark import SparkContext

import re, sys

sc = SparkContext("local", "Empty Lines")
file = sc.textFile(sys.argv[1])
blank_lines = sc.accumulator(0)

def extractCallSigns(line):
    global blank_lines
    if (line == ""):
        blank_lines +=1
    return line.split(" ")

call_signs = file.flatMap(extractCallSigns)
call_signs.saveAsTextFile(sys.argv[2] + "/callsigns")
print "Blank lines: %d" % blank_lines.value