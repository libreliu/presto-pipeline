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

error = False
for fname in all_file_left:
    if fname not in all_file_right:
        print(f"[ERR] {fname} unmatched on the right!")
        error = True

for fname in all_file_right:
    if fname not in all_file_left:
        print(f"[ERR] {fname} unmatched on the left!")
        error = True

if error:
    sys.exit(1)

for fname in all_file_left:
    os.system(f"diff  {leftdir}/{fname} {rightdir}/{fname}")


