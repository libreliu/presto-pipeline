#!/usr/bin/env python3
"""
A reference pipeline used for checking
"""

"""
A simple pipelien for demostrating presto
Weiwei Zhu
2015-08-14
Max-Plank Institute for Radio Astronomy
zhuwwpku@gmail.com
"""
import os, sys, glob, re
from presto import sifting
from operator import attrgetter
from subprocess import getoutput, Popen, PIPE
import numpy as np

#Tutorial_Mode = True
Tutorial_Mode = False

# ----
RUN_PREPSUBBAND = True
RUN_REALFFT = True
RUN_ACCELSEARCH = True
RUN_SIFTING = True
RUN_PREPFOLD = True
if sys.argv[2] == 'PREPSUBBAND':
    pass
elif sys.argv[2] == 'REALFFT':
    RUN_PREPSUBBAND = False
elif sys.argv[2] == 'ACCELSEARCH':
    RUN_PREPSUBBAND = False
    RUN_REALFFT = False
elif sys.argv[2] == 'SIFTING':
    RUN_PREPSUBBAND = False
    RUN_REALFFT = False
    RUN_ACCELSEARCH = False
elif sys.argv[2] == 'PREPFOLD':
    RUN_PREPSUBBAND = False
    RUN_REALFFT = False
    RUN_ACCELSEARCH = False
    RUN_SIFTING = False
else:
    raise Exception("Unknown stage exception")

SUBDIRNAME = sys.argv[3]
FBFILEPATH = sys.argv[1]
FBFILEPATH = os.path.realpath(FBFILEPATH)
print(f"SUBDIRNAME={SUBDIRNAME} FBFILEPATH={FBFILEPATH}")
# ----

rootname = 'Sband'
maxDM = 80 #max DM to search
Nsub = 32 #32 subbands
Nint = 64 #64 sub integration
Tres = 0.5 #ms
zmax = 0

#if len(sys.argv) > 2:
#    maskfile = sys.argv[2]
#else:
#    maskfile = None
maskfile = None

def query(question, answer, input_type):
    print("Based on output of the last step, answer the following questions:")
    Ntry = 3
    while not input_type(input("%s:" % question)) == answer and Ntry > 0:
        Ntry -= 1
        print("try again...")
    if Ntry == 0:print("The correct answer is:", answer)

#"""

print('''

====================Read Header======================

''')

#try:
#myfil = filterbank(FBFILEPATH)

readheadercmd = 'readfile %s' % FBFILEPATH
print(readheadercmd)
output = getoutput(readheadercmd)
print(output)
header = {}
for line in output.split('\n'):
    items = line.split("=")
    if len(items) > 1:
        header[items[0].strip()] = items[1].strip()

#print header
#except:
    #print 'failed at reading file %s.' % filename
    #sys.exit(0)


print('''

============Generate Dedispersion Plan===============

''')

try:
    Nchan = int(header['Number of channels'])
    tsamp = float(header['Sample time (us)']) * 1.e-6
    BandWidth = float(header['Total Bandwidth (MHz)'])
    fcenter = float(header['Central freq (MHz)'])
    Nsamp = int(header['Spectra per file'])

    if Tutorial_Mode:
        query("Input file has how many frequency channel?", Nchan, int)
        query("what is the total bandwidth?", BandWidth, float)
        query("what is the size of each time sample in us?", tsamp*1.e6, float)
        query("what's the center frequency?", fcenter, float)
        print('see how these numbers are used in the next step.')
        print('')

    ddplancmd = 'DDplan.py -d %(maxDM)s -n %(Nchan)d -b %(BandWidth)s -t %(tsamp)f -f %(fcenter)f -s %(Nsub)s -o DDplan.ps' % {
            'maxDM':maxDM, 'Nchan':Nchan, 'tsamp':tsamp, 'BandWidth':BandWidth, 'fcenter':fcenter, 'Nsub':Nsub}
    print(ddplancmd)
    ddplanout = getoutput(ddplancmd)
    print(ddplanout)
    planlist = ddplanout.split('\n')
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
except:
    print('failed at generating DDplan.')
    sys.exit(0)


if Tutorial_Mode:
    calls = 0
    for line in ddplan:
        ddpl = line.split()
        calls += int(ddpl[7])
    query("According to the DDplan, how many times in total do we have to call prepsubband?", calls, int)
    print('see how these numbers are used in the next step.')
    print('')

if RUN_PREPSUBBAND:

    print('''

    ================Dedisperse Subbands==================

    ''')

    cwd = os.getcwd()
    try:
        if not os.access(SUBDIRNAME, os.F_OK):
            os.mkdir(SUBDIRNAME)
        os.chdir(SUBDIRNAME)
        logfile = open('dedisperse.log', 'wb')
        for line in ddplan:
            ddpl = line.split()
            lowDM = float(ddpl[0])
            hiDM = float(ddpl[1])
            dDM = float(ddpl[2])
            DownSamp = int(ddpl[3])
            NDMs = int(ddpl[6])
            calls = int(ddpl[7])
            Nout = Nsamp/DownSamp 
            Nout -= (Nout % 500)
            dmlist = np.split(np.arange(lowDM, hiDM, dDM), calls)

            #copy from $PRESTO/python/Dedisp.py
            subdownsamp = DownSamp/2
            datdownsamp = 2
            if DownSamp < 2: subdownsamp = datdownsamp = 1

            handles = []
            for i, dml in enumerate(dmlist):
                lodm = dml[0]
                subDM = np.mean(dml)
                if maskfile:
                    prepsubband = "prepsubband -sub -subdm %.2f -nsub %d -downsamp %d -mask ../%s -o %s %s" % (subDM, Nsub, subdownsamp, maskfile, rootname, FBFILEPATH)
                else:
                    prepsubband = "prepsubband -sub -subdm %.2f -nsub %d -downsamp %d -o %s %s" % (subDM, Nsub, subdownsamp, rootname, FBFILEPATH)
                print(prepsubband)
                handles.append(Popen(prepsubcmd, shell=True, stdout=PIPE))
                # output = getoutput(prepsubband)
                # logfile.write(output)
            for h in handles:
                logfile.write(h.stdout.read())

            handles = []
            for i, dml in enumerate(dmlist):
                lodm = dml[0]
                subDM = np.mean(dml)

                subnames = rootname+"_DM%.2f.sub[0-9]*" % subDM
                #prepsubcmd = "prepsubband -nsub %(Nsub)d -lodm %(lowdm)f -dmstep %(dDM)f -numdms %(NDMs)d -numout %(Nout)d -downsamp %(DownSamp)d -o %(root)s ../%(filfile)s" % {
                        #'Nsub':Nsub, 'lowdm':lodm, 'dDM':dDM, 'NDMs':NDMs, 'Nout':Nout, 'DownSamp':datdownsamp, 'root':rootname, 'filfile':filename}
                prepsubcmd = "prepsubband -nsub %(Nsub)d -lodm %(lowdm)f -dmstep %(dDM)f -numdms %(NDMs)d -numout %(Nout)d -downsamp %(DownSamp)d -o %(root)s %(subfile)s" % {
                        'Nsub':Nsub, 'lowdm':lodm, 'dDM':dDM, 'NDMs':NDMs, 'Nout':Nout, 'DownSamp':datdownsamp, 'root':rootname, 'subfile':subnames}
                print(prepsubcmd)
                # output = getoutput(prepsubcmd)
                # logfile.write(output)
                handles.append(Popen(prepsubcmd, shell=True, stdout=PIPE))
            for h in handles:
                logfile.write(h.stdout.read())

        os.system('rm *.sub*')
        logfile.close()
        os.chdir(cwd)

    except:
        print('failed at prepsubband.')
        os.chdir(cwd)
        sys.exit(0)

if RUN_REALFFT or RUN_ACCELSEARCH:
    cwd = os.getcwd()

    print('''

    ================fft-search subbands==================

    ''')

    os.chdir(SUBDIRNAME)
    if RUN_REALFFT:
        datfiles = glob.glob("*.dat")
        logfile = open('fft.log', 'wb')
        handles = []
        for df in datfiles:
            fftcmd = "realfft %s" % df
            print(fftcmd)
            handles.append(Popen(fftcmd, shell=True, stdout=PIPE))
            # output = getoutput(fftcmd)
            # logfile.write(output)
        for h in handles:
            logfile.write(h.stdout.read())
    print("cwd: " + os.getcwd())
    if RUN_ACCELSEARCH:
        logfile = open('accelsearch.log', 'wb')
        fftfiles = glob.glob("*.fft")
        #print(fftfiles)
        handles = []
        for fftf in fftfiles:
            searchcmd = "accelsearch -zmax %d %s"  % (zmax, fftf)
            print(searchcmd)
            # output = getoutput(searchcmd)
            # logfile.write(output)
            handles.append(Popen(searchcmd, shell=True, stdout=PIPE))
        for h in handles:
            logfile.write(h.stdout.read())
        logfile.close()
    os.chdir(cwd)
#"""


def ACCEL_sift(zmax):
    '''
    The following code come from PRESTO's ACCEL_sift.py
    '''

    globaccel = "*ACCEL_%d" % zmax
    globinf = "*DM*.inf"
    # In how many DMs must a candidate be detected to be considered "good"
    min_num_DMs = 2
    # Lowest DM to consider as a "real" pulsar
    low_DM_cutoff = 2.0
    # Ignore candidates with a sigma (from incoherent power summation) less than this
    sifting.sigma_threshold = 4.0
    # Ignore candidates with a coherent power less than this
    sifting.c_pow_threshold = 100.0

    # If the birds file works well, the following shouldn't
    # be needed at all...  If they are, add tuples with the bad
    # values and their errors.
    #                (ms, err)
    sifting.known_birds_p = []
    #                (Hz, err)
    sifting.known_birds_f = []

    # The following are all defined in the sifting module.
    # But if we want to override them, uncomment and do it here.
    # You shouldn't need to adjust them for most searches, though.

    # How close a candidate has to be to another candidate to                
    # consider it the same candidate (in Fourier bins)
    sifting.r_err = 1.1
    # Shortest period candidates to consider (s)
    sifting.short_period = 0.0005
    # Longest period candidates to consider (s)
    sifting.long_period = 15.0
    # Ignore any candidates where at least one harmonic does exceed this power
    sifting.harm_pow_cutoff = 8.0

    #--------------------------------------------------------------

    # Try to read the .inf files first, as _if_ they are present, all of
    # them should be there.  (if no candidates are found by accelsearch
    # we get no ACCEL files...
    inffiles = glob.glob(globinf)
    candfiles = glob.glob(globaccel)
    # Check to see if this is from a short search
    if len(re.findall("_[0-9][0-9][0-9]M_" , inffiles[0])):
        dmstrs = [x.split("DM")[-1].split("_")[0] for x in candfiles]
    else:
        dmstrs = [x.split("DM")[-1].split(".inf")[0] for x in inffiles]
    dms = list(map(float, dmstrs))
    dms.sort()
    dmstrs = ["%.2f"%x for x in dms]

    # Read in all the candidates
    cands = sifting.read_candidates(candfiles)

    # Remove candidates that are duplicated in other ACCEL files
    if len(cands):
        cands = sifting.remove_duplicate_candidates(cands)

    # Remove candidates with DM problems
    if len(cands):
        cands = sifting.remove_DM_problems(cands, min_num_DMs, dmstrs, low_DM_cutoff)

    # Remove candidates that are harmonically related to each other
    # Note:  this includes only a small set of harmonics
    if len(cands):
        cands = sifting.remove_harmonics(cands)

    # Write candidates to STDOUT
    if len(cands):
        cands.sort(key=attrgetter('sigma'), reverse=True)
        #for cand in cands[:1]:
            #print cand.filename, cand.candnum, cand.p, cand.DMstr
        #sifting.write_candlist(cands)
    return cands

if RUN_SIFTING:
    print('''

    ================sifting candidates==================

    ''')

    #try:
    cwd = os.getcwd()
    os.chdir(SUBDIRNAME)
    cands = ACCEL_sift(zmax)
    os.chdir(cwd)
    #except:
        #print 'failed at sifting candidates.'
        #os.chdir(cwd)
        #sys.exit(0)

if RUN_PREPFOLD:

    print('''

    ================folding candidates==================

    ''')

    try:
        cwd = os.getcwd()
        os.chdir(SUBDIRNAME)
        #os.system('ln -s ../%s %s' % (filename, filename))
        logfile = open('folding.log', 'wb')
        handles = []
        for cand in cands:
            #foldcmd = "prepfold -dm %(dm)f -accelcand %(candnum)d -accelfile %(accelfile)s %(datfile)s -noxwin " % {
            #'dm':cand.DM,  'accelfile':cand.filename+'.cand', 'candnum':cand.candnum, 'datfile':('%s_DM%s.dat' % (rootname, cand.DMstr))} #simple plots
            foldcmd = "prepfold -n %(Nint)d -nsub %(Nsub)d -dm %(dm)f -p %(period)f %(filfile)s -o %(outfile)s -noxwin -nodmsearch" % {
                    'Nint':Nint, 'Nsub':Nsub, 'dm':cand.DM,  'period':cand.p, 'filfile':FBFILEPATH, 'outfile':rootname+'_DM'+cand.DMstr} #full plots
            print(foldcmd)
            #os.system(foldcmd)
            handles.append(Popen(foldcmd, shell=True, stdout=PIPE))
            # output = getoutput(foldcmd)
            # logfile.write(output)
        for h in handles:
            logfile.write(h.stdout.read())

        logfile.close()
        os.chdir(cwd)
    except:
        print('failed at folding candidates.')
        os.chdir(cwd)
        sys.exit(0)
