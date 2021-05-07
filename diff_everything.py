#!/usr/bin/env python3

import sys, glob, os
leftdir = sys.argv[1]
rightdir = sys.argv[2]

os.chdir(leftdir)
all_file_left = glob.glob("*.*")
os.chdir("../")

os.chdir(rightdir)
all_file_right = glob.glob("*.*")
os.chdir("../")

for fname in all_file_left:
    if fname not in all_file_right:
        print(f"[ERR] {fname} unmatched on the right!")

for fname in all_file_right:
    if fname not in all_file_left:
        print(f"[ERR] {fname} unmatched on the left!")

print("TODO: diff")
