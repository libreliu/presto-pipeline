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

def realfft_normal_gbt(hostfile):
    hname = pretty_name(hostfile)
    dname = f"./workdir_realfft_normal_{hname}"
    if os.path.exists(dname):
        import shutil
        shutil.rmtree(dname)
    os.mkdir(dname)
    show_time(os.system, f"realfft_normal_{hname}", f"python3 ./pipeline-optim/PrestoRealfft.py {dname} ./TestData/realfft-bench/gbt {hostfile}")

    os.system(f"cp ./TestData/realfft-bench/gbt/*.dat {dname}")
    os.system(f"cp ./TestData/realfft-bench/gbt/*.inf {dname}")
    print(f"VALIDATION: {dname}")
    os.system(f"python3 ./pipeline-optim/PrestoReferencePipeline.py ./TestData/GBT_Lband_PSR.fil ACCELSEARCH {dname}")

def accelsearch_normal_gbt(hostfile):
    hname = pretty_name(hostfile)
    dname = f"./workdir_accelsearch_normal_{hname}"
    if os.path.exists(dname):
        import shutil
        shutil.rmtree(dname)
    os.mkdir(dname)
    os.system(f"cp ./TestData/accelsearch-bench/gbt/* {dname}")
    show_time(os.system, f"accelsearch_normal_{hname}", f"python3 ./pipeline-optim/PrestoAccelsearch.py {dname} {hostfile} 0")

    os.system(f"cp ./TestData/accelsearch-bench/gbt/*.dat {dname}")
    os.system(f"cp ./TestData/accelsearch-bench/gbt/*.inf {dname}")
    print(f"VALIDATION: {dname}")
    os.system(f"python3 ./pipeline-optim/PrestoReferencePipeline.py ./TestData/GBT_Lband_PSR.fil SIFTING {dname}")

def prepsubband_normal_gbt(hostfile):
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
        hname = pretty_name(host)
        #realfft_normal_gbt(host)
        #accelsearch_normal_gbt(host)
        prepsubband_normal_gbt(host)

if __name__ == '__main__':
    run()