#!/usr/bin/env python3

""""""

import asyncio
import logging
from functools import reduce
from RemoteExecutor import ExecutorClient
from LocalExecutor import LocalExecutor
from HostManager import HostManager

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

async def pipeline(fbfilename, hostfilename, rootname, maxDM, Nsub, Nint, Tres, zmax):
    logging.basicConfig(
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S',
        level=logging.DEBUG
    )

    host_manager = HostManager.from_hostfile(hostfilename)
    base_executor = host_manager.get_base_executor()
    await host_manager.connect_remote()

    ret, output = await base_executor.execute(f"readfile {fbfilename}")
    assert(ret == 0)

    Nchan, tsamp, BandWidth, fcenter, Nsamp = parse_readfile(output)
    logger.info(f"Nchan={Nchan}, tsamp={tsamp}, BandWidth={BandWidth}, fcenter={fcenter}, Nsamp={Nsamp}")

    ret, output = await base_executor.execute(
        'DDplan.py -d %(maxDM)s -n %(Nchan)d -b %(BandWidth)s -t %(tsamp)f -f %(fcenter)f -s %(Nsub)s -o DDplan.ps' % {
            'maxDM':maxDM,
            'Nchan':Nchan,
            'tsamp':tsamp,
            'BandWidth':BandWidth,
            'fcenter':fcenter,
            'Nsub':Nsub
        }
    )
    ddplan = parse_ddplan(output)
    logger.info(f"ddplan: {ddplan}")

    # prep, realfft & accelsearch
    

    # prepfold

    await host_manager.close_remote()



if __name__ == '__main__':
    asyncio.run(pipeline(
        "GBT_Lband_PSR.fil",
        'hostfile',
        'Sband',
        80,
        32,
        64,
        0.5,
        0
    ))
