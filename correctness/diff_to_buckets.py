######################################################################
#
# File: diff_to_buckets.py
#
# Copyright 2015 TiVo Inc. All Rights Reserved.
#
######################################################################
"""
Usage: diff_to_buckets.py <left> <right> <output-left-only> <output-right-only> <output-same> <output-different>
"""
import heapq
import sys
import json

def usage():
    print __doc__
    sys.exit(1)

def decorate(f, which):
    for line in f:
        js = json.loads(line)
        yield (js['primary_key_values'], which, js)
    
def accumulate_by_key(merged_list):
    prev_key = None
    prev_data = {}

    for record in merged_list:

        (key, which, js) = record
    #    print (key, which)

        if prev_key is not None:
            if key != prev_key:
                yield (prev_key, prev_data)
                prev_data.clear()
        prev_key = key
        assert which not in prev_data
        prev_data[which] = js

    if prev_key is not None:
        yield (prev_key, prev_data)

def main():
    if len(sys.argv) != 7:
        usage()

    left_filename = sys.argv[1]
    right_filename = sys.argv[2]

    left = open(left_filename)
    right = open(right_filename)

    decorated_left = decorate(left, 'left')
    decorated_right = decorate(right, 'right')

    merged = heapq.merge(decorated_left, decorated_right)

    # Usage: diff_to_buckets.py <left> <right> <output-left-only> <output-right-only> <output-same> <output-different>
    output_left = open(sys.argv[3], "w")
    output_right = open(sys.argv[4], "w")
    output_same = open(sys.argv[5], "w")
    output_different = open(sys.argv[6], "w")

    for (_key, data) in accumulate_by_key(merged):
        if data.get('right') is None:
            # only in the left
            output_left.write(json.dumps( { "key" : data['left']['primary_key'],
                                "row" : data['left']['row'] }))
            output_left.write("\n")
        elif data.get('left') is None:
            # only in the right
            output_right.write(json.dumps( { "key" : data['right']['primary_key'],
                                "row" : data['right']['row'] }))
            output_right.write("\n")
        elif data.get('left') == data.get('right'):
            # same on both sides
            output_same.write(json.dumps( { "key" : data['right']['primary_key'],
                                "row" : data['right']['row'] }))
            output_same.write("\n")
        else:
            # exist on both sides, but are different
            output_different.write(json.dumps( { "key" : data['left']['primary_key'],
                                "left" : data['left']['row'],
                                "right" : data['right']['row'] }))
            output_different.write("\n")

if __name__ == "__main__":
    main()
