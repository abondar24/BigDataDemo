from pyspark import SparkContext

import re, sys

sc = SparkContext("local", "Error counter")
file = sc.textFile(sys.argv[1])
valid_sign_count = sc.accumulator(0)
invalud_sign_count = sc.accumulator(0)


def extractCallSigns(line):
    global blank_lines
    if (line == ""):
        blank_lines += 1
    return line.split(" ")


call_signs = file.flatMap(extractCallSigns)


def validateSign(sign):
    global valid_sign_count, invalud_sign_count
    if re.match(r"\A\d?[a-zA-Z]{1,2}\d{1,4}[a-zA-Z]{1,3}\Z", sign):
        valid_sign_count += 1
        return True
    else:
        invalud_sign_count += 1
        return False


valid_signs = call_signs.filter(validateSign)
contact_count = valid_signs.map(lambda sign: (sign, 1)).reduceByKey(lambda (x, y): x + y)

contact_count.count()
if invalud_sign_count.value < 0.1 * valid_sign_count.value:
    contact_count.saveAsTextFile(sys.argv[2] + "/contactCount")
else:
    print "Too many errors: %d in %d" % (invalud_sign_count.value, valid_sign_count.value)
