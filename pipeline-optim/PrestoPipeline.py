#!/usr/bin/env python3

""""""

import asyncio
import logging
import datetime
import json
import sys
import numpy as np
from functools import reduce
from RemoteExecutor import ExecutorClient
from LocalExecutor import LocalExecutor
from HostManager import HostManager
from ParallelTaskExecutor import ParallelTaskExecutor

logger = logging.getLogger(__name__)

def parse_readfile(output):
    header = {}
    for line in output.split('\n'):
        items = line.split("=")
        if len(items) > 1:
            header[items[0].strip()] = items[1].strip()

    Nchan = int(header['Number of channels'])
    tsamp = float(header['Sample time (us)']) * 1.e-6
    BandWidth = float(header['Total Bandwidth (MHz)'])
    fcenter = float(header['Central freq (MHz)'])
    Nsamp = int(header['Spectra per file'])

    return Nchan, tsamp, BandWidth, fcenter, Nsamp

def parse_ddplan(output):
    planlist = output.split('\n')
    ddplan = []
    planlist.reverse()
    for plan in planlist:
        if plan == '':
            continue
        elif plan.strip().startswith('Low DM'):
            break
        else:
            ddplan.append(plan)
    ddplan.reverse()

    ret = []
    for line in ddplan:
        ddpl = line.split()

        lowDM = float(ddpl[0])
        hiDM = float(ddpl[1])
        dDM = float(ddpl[2])
        DownSamp = int(ddpl[3])
        NDMs = int(ddpl[6])
        calls = int(ddpl[7])
        ret.append({
            'lowDM': lowDM,
            'hiDM': hiDM,
            'dDM': dDM,
            'DownSamp': DownSamp,
            'NDMs': NDMs,
            'calls': calls
        })

    return ret

def parse_accel_sift(output):
    start = output.find("JSON_BEGIN") + len("JSON_BEGIN")
    end = output.find("JSON_END")
    return json.loads(output[start:end].strip())

async def pipeline(fbfilename, hostfilename, workdir_prefix, rootname, maxDM, Nsub, Nint, Tres, zmax):

    host_manager = HostManager.from_hostfile(hostfilename)
    base_executor = host_manager.get_base_executor()
    await host_manager.connect_remote()

    # TODO: change all remote & local executor's working directory
    workdir = "./" + workdir_prefix + "_" + datetime.datetime.now().strftime("%Y%m%d_%H%M%S") + "/"
    ret, output = await base_executor.execute(f"mkdir {workdir}")
    assert(ret == 0)
    
    logger.info(f"Created working directory {workdir}")

    ret, output = await base_executor.execute(f"readfile {fbfilename}")
    assert(ret == 0)

    Nchan, tsamp, BandWidth, fcenter, Nsamp = parse_readfile(output)
    logger.info(f"Nchan={Nchan}, tsamp={tsamp}, BandWidth={BandWidth}, fcenter={fcenter}, Nsamp={Nsamp}")

    ret, output = await base_executor.execute(
        'DDplan.py -d %(maxDM)s -n %(Nchan)d -b %(BandWidth)s -t %(tsamp)f -f %(fcenter)f -s %(Nsub)s -o %(workdir)s/DDplan.ps' % {
            'maxDM':maxDM,
            'Nchan':Nchan,
            'tsamp':tsamp,
            'BandWidth':BandWidth,
            'fcenter':fcenter,
            'Nsub':Nsub,
            'workdir':workdir
        }
    )
    ddplan = parse_ddplan(output)
    logger.info(f"ddplan: {ddplan}")

    # prep, realfft & accelsearch
    # Todo: figure out ddm decision.
    stage_1_task_executor = ParallelTaskExecutor()
    for executor in host_manager.all_executors():
        stage_1_task_executor.add_runner(
            "",
            executor,
            host_manager.get_slot(executor)
        )
    
    # prep tasks
    assert(len(ddplan) == 1)
    current_ddplan = ddplan[0]
    Nout = Nsamp / current_ddplan['DownSamp']
    Nout -= (Nout % 500)

    # [array([ 0. ,  0.5,  1. ,  1.5,  2. ,  2.5,  3. ,  3.5,  4. ,  4.5,  5. ,
    #     5.5,  6. ,  6.5,  7. ,  7.5,  8. ,  8.5,  9. ,  9.5, 10. , 10.5,
    #    11. , 11.5]), array([12. , 12.5, 13. , 13.5, 14. , 14.5, 15. , 15.5, 16. , 16.5, 17. ,
    #    17.5, 18. , 18.5, 19. , 19.5, 20. , 20.5, 21. , 21.5, 22. , 22.5,
    #    23. , 23.5]), array([24. , 24.5, 25. , 25.5, 26. , 26.5, 27. , 27.5, 28. , 28.5, 29. ,
    #    29.5, 30. , 30.5, 31. , 31.5, 32. , 32.5, 33. , 33.5, 34. , 34.5,
    #    35. , 35.5]), array([36. , 36.5, 37. , 37.5, 38. , 38.5, 39. , 39.5, 40. , 40.5, 41. ,
    #    41.5, 42. , 42.5, 43. , 43.5, 44. , 44.5, 45. , 45.5, 46. , 46.5,
    #    47. , 47.5]), array([48. , 48.5, 49. , 49.5, 50. , 50.5, 51. , 51.5, 52. , 52.5, 53. ,
    #    53.5, 54. , 54.5, 55. , 55.5, 56. , 56.5, 57. , 57.5, 58. , 58.5,
    #    59. , 59.5]), array([60. , 60.5, 61. , 61.5, 62. , 62.5, 63. , 63.5, 64. , 64.5, 65. ,
    #    65.5, 66. , 66.5, 67. , 67.5, 68. , 68.5, 69. , 69.5, 70. , 70.5,
    #    71. , 71.5]), array([72. , 72.5, 73. , 73.5, 74. , 74.5, 75. , 75.5, 76. , 76.5, 77. ,
    #    77.5, 78. , 78.5, 79. , 79.5, 80. , 80.5, 81. , 81.5, 82. , 82.5,
    #    83. , 83.5])]
    dmlist = np.split(
        np.arange(
            current_ddplan['lowDM'],
            current_ddplan['hiDM'],
            current_ddplan['dDM']
        ),
        current_ddplan['calls']
    )

    logger.info(f"dmlist: {dmlist}")
    subdownsamp = current_ddplan['DownSamp'] / 2
    datdownsamp = 2
    if current_ddplan['DownSamp'] < 2:
        subdownsamp = datdownsamp = 1
    
    prep_task_uuid = []

    # index by dm, todo: string or np.float notation?
    fft_tasks = {}
    accel_search_tasks = {}
    for i, dml in enumerate(dmlist):
        # generates Sband_DM*.dat & inf
        lodm = dml[0]
        subDM = np.mean(dml)
        subnames = rootname + "_DM%.2f.sub[0-9]" % subDM
        prepsubcmd = "cd %(workdir)s && prepsubband -nsub %(Nsub)d -lodm %(lowdm)f -dmstep %(dDM)f -numdms %(NDMs)d -numout %(Nout)d -downsamp %(DownSamp)d -o %(root)s ../%(filfile)s" % {
            'workdir': workdir,
            'Nsub': Nsub,
            'lowdm': lodm,
            'dDM': current_ddplan['dDM'],
            'NDMs': current_ddplan['NDMs'],
            'Nout': Nout,
            'DownSamp': datdownsamp,
            'root': rootname,
            'filfile': fbfilename
            }

        prep_task = stage_1_task_executor.add_task(f"prep_batch_{i}", [], [prepsubcmd], 1)
        prep_task_uuid.append(prep_task)

        for dm in dml:
            fft_task = stage_1_task_executor.add_task(
                f"realfft_dm{dm}",
                [prep_task],
                [f"cd {workdir} && realfft {rootname}_DM{dm:.2f}.dat"],
                1
            )
            fft_tasks[dm] = fft_task
        
            accel_search_task = stage_1_task_executor.add_task(
                f"accelsearch_dm{dm}",
                [fft_task],
                [f"cd {workdir} && accelsearch -zmax 0 {rootname}_DM{dm:.2f}.fft"],
                1
            )
            accel_search_tasks[dm] = accel_search_task
        

    stage_1_task_executor.update_alloc()
    stage_1_task_executor.start_runners()

    await stage_1_task_executor.wait_until_finish()

    # sifting & prepfold
    logger.info("Stage 1 done.")

    ret, output = await base_executor.execute(f"cp PrestoSifting.py {workdir} && cd {workdir} && python3 PrestoSifting.py 0")
    assert(ret == 0)
    logger.debug(f"Sifting output: {output}")

    cands = parse_accel_sift(output)
    logger.info(f"Sifting cands: {cands}")

    stage_2_task_executor = ParallelTaskExecutor()
    for executor in host_manager.all_executors():
        stage_2_task_executor.add_runner(
            "",
            executor,
            host_manager.get_slot(executor)
        )

    for i, cand in enumerate(cands):
        foldcmd = "cd %(workdir)s && prepfold -dm %(dm)f -accelcand %(candnum)d -accelfile %(accelfile)s %(datfile)s -noxwin" % {
            'workdir': workdir,
            'dm': cand['DM'],
            'accelfile': cand['filename'] + '.cand',
            'candnum': cand['candnum'],
            'datfile': ('%s_DM%s.dat' % (rootname, cand['DMstr']))
        }

        cand_task = stage_2_task_executor.add_task(f"cand_{i}", [], [foldcmd], 1)
    
    stage_2_task_executor.update_alloc()
    stage_2_task_executor.start_runners()

    await stage_2_task_executor.wait_until_finish()

    # sifting & prepfold
    logger.info("Stage 2 done.")

    await host_manager.close_remote()



if __name__ == '__main__':
    if len(sys.argv) != 2:
        print("Usage: PrestoPipeline.py filterbank_filename")

    logging.basicConfig(
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S',
        level=logging.INFO
    )

    logger.info(f"Run with: filfile={sys.argv[1]}")
    asyncio.run(pipeline(
        sys.argv[1],
        'hostfile',
        'workdir',
        'Sband',
        80,
        32,
        64,
        0.5,
        0
    ))
