#!/usr/bin/env python3

import time, os, glob, sys

def show_time(func, name, *params):
    start = time.perf_counter()
    func(*params)
    end = time.perf_counter()

    print(f"{name}: {end - start:.2f}s")

def load_hosts():
    if len(sys.argv) >= 2 and sys.argv[1] == 'local':
        hostfiles = ["./configurations/hostfile.local"]
    else:
        hostfiles = glob.glob("./configurations/*")
    return hostfiles

def realfft_normal(hostfile, duration):
    hname = pretty_name(hostfile)
    dname = f"/presto-contest/workdir_realfft_normal_{duration}_{hname}"
    if os.path.exists(dname):
        import shutil
        shutil.rmtree(dname)
    os.mkdir(dname)
    show_time(os.system, f"realfft_normal_{hname}", f"python3 ./pipeline-optim/PrestoRealfftContest.py {dname} /presto-contest/B1516+02_{duration}_2bit/input/ {hostfile}")

def accelsearch_normal(hostfile, duration):
    hname = pretty_name(hostfile)
    dname = f"/presto-contest/workdir_accelsearch_normal_{duration}_{hname}"
    if os.path.exists(dname):
        import shutil
        shutil.rmtree(dname)
    os.mkdir(dname)
    os.system(f"cp /presto-contest/PRESTO/B1516+02_{duration}_2bit/accelinput/ {dname}")
    show_time(os.system, f"accelsearch_normal_{hname}", f"python3 ./pipeline-optim/PrestoAccelsearchContest.py {dname} {hostfile}")

def prepsubband_normal(hostfile):
    hname = pretty_name(hostfile)
    hname = pretty_name(hostfile)
    dname = f"./workdir_prepsubband_normal_{hname}"
    if os.path.exists(dname):
        import shutil
        shutil.rmtree(dname)
    os.mkdir(dname)
    os.system(f"cp ./TestData/prepsubband-bench/gbt/* {dname}")
    show_time(os.system, f"realfft_prepsubband_{hname}", f"python3 ./pipeline-optim/PrestoPrepsubband.py {dname} gbt {hostfile}")

    print(f"VALIDATION: {dname}")
    os.system(f"python3 ./pipeline-optim/PrestoReferencePipeline.py ./TestData/GBT_Lband_PSR.fil REALFFT {dname}")

def pretty_name(path):
    return os.path.split(path)[-1].replace(".", "_")

def run():
    hosts = load_hosts()
    for host in hosts:
        for case in ['300s', '900s', '1500s']:
            hname = pretty_name(host)
            realfft_normal(host, case)
            #accelsearch_normal(host, case)
            # prepsubband_normal_gbt(host)

if __name__ == '__main__':
    run()