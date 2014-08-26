import re
from bs4 import BeautifulSoup
import json
import os
import codecs
import multiprocessing
import time

## PARAMETERS
absPath = 'd:/patent_data/abstracts'
mthAbsPath = 'd:/patent_data/abstracts/month_files'
dataPath = 'd:/patent_data/all_files'

## METHODS
def splitter(seq, chnks):
    """ Splits a list into roughly equal parts """
    if chnks>len(seq):
        raise Exception('List too short or too many chunks')
    newseq = []
    splitsize = 1.0/chnks*len(seq)
    for i in range(chnks):
            newseq.append(seq[int(round(i*splitsize)):int(round((i+1)*splitsize))])
    return newseq

def extPatAbs7601(content):
    """
    Returns dictionary with patent number, patent title, and patent abstract. 
    Works on USPTO yearly .dat files from 1976 thorugh 2001. 
    """
    # Create dictionary to hold patent data
    patDict = {}
    
    # Find the linenumber where each patent entry starts in the file
    patRecs = []
    for rec in range(len(content)):
        if content[rec].startswith('PATN'):
            patRecs.append(rec)
    patRecs.append(len(content))
    
    # Find patent number, title, and abstract for each patent 
    for i in range(len(patRecs)-1):
        pat = content[patRecs[i]:patRecs[i+1]]
        abstract = 'NULL'
        for j in range(len(pat)):
            if pat[j].startswith('WKU'):
                num = pat[j].strip()
                num = re.sub('WKU\s+','',num)
                if num.startswith('0'):
                    num = num[1:-1]
            if pat[j].startswith('TTL'):
                title = pat[j].strip()
                title = re.sub('TTL\s+','',title)
                title = unicode(title, errors = 'ignore')
            if pat[j].startswith('ABST'):
                abstract = pat[j+1:]
                if abstract:
                    abstract[0] = re.sub('PAL\s+','',abstract[0])
                    abstract[0] = re.sub('PAR\s+','',abstract[0])
                    abstract = map(lambda x: x.strip(), abstract)
                    abstract = ' '.join(abstract)
                    abstract = unicode(abstract, errors = 'ignore')
                else:
                    abstract = 'NULL'
        patDict[num] = {'title':title,'abstract':abstract}
    return patDict
    
def extPatAbs0204(content):
    """
    Returns dictionary with patent number, patent title, and patent abstract. 
    Works on monthly USPTO .xml files from 2002 thorugh 2004. 
    """
    # Create dictionary to hold patent data
    patDict = {}
    
    # Find the linenumber where each patent entry starts in the file
    patRecs = []
    for rec in range(len(content)):
        if content[rec].startswith('<PATDOC'):
            patRecs.append(rec)
    patRecs.append(len(content))
    
    # Find patent number, title, and abstract for each utility patent 
    for i in range(len(patRecs)-1):
        patXML = ' '.join(content[patRecs[i]:patRecs[i+1]])
        soup = BeautifulSoup(patXML)
        # Cut leading zero if utility patent
        if soup.find('b110').pdat.get_text()[0]=='0':
            num = soup.find('b110').pdat.get_text()[1:]
        else:
            num = soup.find('b110').pdat.get_text()
        title = soup.find('stext').pdat.get_text()
        # Some design patents do not have abstracts
        if soup.find('btext'):
            abstract = soup.find('btext').pdat.get_text()
        else:
            abstract = 'NULL'
        patDict[num] = {'title':title,'abstract':abstract}
    return patDict 

def extPatAbs0514(content):
    """
    Returns dictionary with patent number, patent title, and patent abstract. 
    Works on monthly USPTO .xml files from 2005 thorugh 2014. 
    """
    # Create dictionary to hold patent data
    patDict = {}
    
    # Find the linenumber where each patent entry starts in the file
    patRecs = []
    for rec in range(len(content)):
        if content[rec].startswith('<us-patent-grant'):
            patRecs.append(rec)
    patRecs.append(len(content))
    
    # Find patent number, title, and abstract for each utility patent 
    for i in range(len(patRecs)-1):
        patXML = ' '.join(content[patRecs[i]:patRecs[i+1]])
        soup = BeautifulSoup(patXML)
        num = soup.find('publication-reference').find('doc-number').get_text()
        # Strip leading zero if utility patent
        if num[0] == '0':
            num = num[1:]
        title = soup.find('invention-title').get_text()
        # Some design patents do not have an abstract
        if soup.find('abstract'):
            abstract = soup.find('abstract').get_text()
        else:
            abstract = 'NULL'
        patDict[num] = {'title':title,'abstract':abstract}
    return patDict 
    
def multi_procPatFiles7601(yearFiles):
    """ 
    Processes the patent files for 1976 to 2001. Takes yearly patent .dat files as input 
    and outputs .json files with python dictionaries containing the patent number, patent 
    title, and patent abstract.
    Used to spawn multiprocess workers to work on a subset of the total yearly files.
    """
    t0 = time.clock()
    print 'Starting', multiprocessing.current_process().name, 'minutes:{0}'.format(round(float(time.clock()-t0)/60.0,2)), '\n'
    # Extract patent number, title, and abstract for all patents in each file and export to json
    progSplit = splitter(yearFiles,4) # for progress reporting
    for fileName in yearFiles:
        # Report progress
        if fileName in [i[-1] for i in progSplit]:
            prct = round(yearFiles.index(fileName)/float(len(yearFiles)),2)
            prct = str(prct*100)[:4 + 2]
            print '{0} is {1}% complete'.format(multiprocessing.current_process().name, prct)
        fileYear = fileName[0:4]
        f = codecs.open(dataPath+'/{0}'.format(fileName), encoding='ISO-8859-1')
        f = open(dataPath+'/{0}'.format(fileName),'r')
        content = f.readlines()
        
        absDict = extPatAbs7601(content)
        f.close()
        
        jf = open(absPath+'/patAbs{0}.json'.format(fileYear),'w')
        json.dump(absDict,jf)
        jf.close()
    print '\n', 'Exiting', multiprocessing.current_process().name, 'minutes:{0}'.format(round(float(time.clock()-t0)/60.0,2)), '\n' 

def multi_procPatFiles0204(monthFiles):
    """ 
    Processes the patent files for 2002 to 2004. Takes monthly patent .xml files as input 
    and outputs .json files with python dictionaries containing the patent number, patent 
    title, and patent abstract.
    Used to spawn multiprocess workers to work on a subset of the total monthly files.
    """
    t0 = time.clock()
    print 'Starting', multiprocessing.current_process().name, 'minutes:{0}'.format(round(float(time.clock()-t0)/60.0,2)), '\n'
    # Extract patent number, title, and abstract for all patents in each file and export to json
    progSplit = splitter(monthFiles,4) # for progress reporting
    for fileName in monthFiles:
        #print '{0} is starting'.format(multiprocessing.current_process().name), fileName
        # Report progress
        if fileName in [i[-1] for i in progSplit]:
            prct = round(monthFiles.index(fileName)/float(len(monthFiles)),2)
            prct = str(prct*100)[:4 + 2]
            print '{0} is {1}% complete'.format(multiprocessing.current_process().name, prct)
        fileDate = fileName[3:11]
        f = open(dataPath+'/{0}'.format(fileName),'r')
        content = f.readlines()
        
        absDict = extPatAbs0204(content)
        f.close()
        
        jf = open(mthAbsPath+'/patAbs{0}.json'.format(fileDate),'w')
        json.dump(absDict,jf)
        jf.close()
    print '\n', 'Exiting', multiprocessing.current_process().name, 'minutes:{0}'.format(round(float(time.clock()-t0)/60.0,2)), '\n'

def multi_procPatFiles0514(monthFiles):
    """ 
    Processes the patent files for 2005 to 2014. Takes monthly patent .xml files as input 
    and outputs .json files with python dictionaries containing the patent number, patent 
    title, and patent abstract.
    Used to spawn multiprocess workers to work on a subset of the total monthly files.
    """
    t0 = time.clock()
    print 'Starting', multiprocessing.current_process().name, 'minutes:{0}'.format(round(float(time.clock()-t0)/60.0,2)), '\n'
    # Extract patent number, title, and abstract for all patents in each file and export to json
    progSplit = splitter(monthFiles,20) # for progress reporting
    for fileName in monthFiles:
        # Report progress
        if fileName in [i[-1] for i in progSplit]:
            prct = round(monthFiles.index(fileName)/float(len(monthFiles)),3)
            prct = str(prct*100)[:4 + 2]
            print '{0} is {1}% complete'.format(multiprocessing.current_process().name, prct)
        fileDate = fileName[4:12]
        f = open(dataPath+'/{0}'.format(fileName),'r')
        content = f.readlines()
        
        absDict = extPatAbs0514(content)
        f.close()
        
        jf = open(mthAbsPath+'/patAbs{0}.json'.format(fileDate),'w')
        json.dump(absDict,jf)
        jf.close()
    print '\n', 'Exiting', multiprocessing.current_process().name, 'minutes:{0}'.format(round(float(time.clock()-t0)/60.0,2)), '\n' 

def multi_collapseYears(years):
    """ 
    Processes the output from processing the monthly patent files. Takes the monthly .json
    files as input and outputs yearly .json dictionary files.
    Used to spawn multiprocess workers to work on a subset of the years to collapse.
    """
    t0 = time.clock()
    print 'Starting', multiprocessing.current_process().name, time.clock() - t0, '\n'
    monthFiles = sorted((fn for fn in os.listdir(mthAbsPath) if fn.startswith('patAbs') and len(fn)>15))
    # Combine monthly files into year files
    progSplit = splitter(years,3) # for progress reporting
    for yr in years:
        # Report progress
        if yr in [i[-1] for i in progSplit]:
            prct = round(years.index(yr)/float(len(years)),3)
            prct = str(prct*100)[:4 + 2]
            print '{0} is {1}% complete'.format(multiprocessing.current_process().name, prct)
        filesToCollapse = [x for x in monthFiles if x[6:10]==yr]
        baseFile = open(mthAbsPath+'/{0}'.format(filesToCollapse[0]),'r')
        baseDict = json.load(baseFile)
        for fileName in filesToCollapse:
            fileToAdd = open(mthAbsPath+'/{0}'.format(fileName),'r')
            dictToAdd = json.load(fileToAdd)
        
            baseDict.update(dictToAdd)
            fileToAdd.close()
        baseFile.close()
        
        jf = open(absPath+'/patAbs{0}.json'.format(yr),'w')
        json.dump(baseDict,jf)
        jf.close()
    print '\n', 'Exiting', multiprocessing.current_process().name, 'minutes:{0}'.format(round(float(time.clock()-t0)/60.0,2)), '\n' 

def main():
    # User input of processes to run
    params = {'do7601':'null', 'do0204':'null', 'do0514':'null','doCollapse':'null'}
    for p in params.keys():
        print 'would you like to {0} (y/n)? '.format(p),
        answer = 'null'
        while answer not in ['y','n']:
            answer = raw_input()
            if answer in ['y','n']:
                params[p] = answer
            else:
                print 'invalid input'
    print params
    
    if params['do7601']=='y': 
        print '\n', 'Start Processing 1976-2001'
        yearFiles = sorted((fn for fn in os.listdir(dataPath) if fn.endswith('.dat')))
        
        processes = []
        splitList = splitter(yearFiles,4)
        w1 = multiprocessing.Process(name='wkr_7601_1', target=multi_procPatFiles7601, args=(list(splitList[0]),))
        w2 = multiprocessing.Process(name='wkr_7601_2', target=multi_procPatFiles7601, args=(list(splitList[1]),))
        w3 = multiprocessing.Process(name='wkr_7601_3', target=multi_procPatFiles7601, args=(list(splitList[2]),))
        w4 = multiprocessing.Process(name='wkr_7601_4', target=multi_procPatFiles7601, args=(list(splitList[3]),))
        
        w1.start()
        processes.append(w1)
        time.sleep(2)
        w2.start()
        processes.append(w2)
        time.sleep(2)
        w3.start()
        processes.append(w3)
        time.sleep(2)
        w4.start()
        processes.append(w4)
        
        for p in processes:
            p.join()
        print 'Workers:',processes,'finished and joined'
    
    if params['do0204']=='y':
        print '\n', 'Start Processing 2002-2004'
        monthFiles = sorted((fn for fn in os.listdir(dataPath) if fn.startswith('pgb') and fn.endswith('.xml')))
        
        processes = []
        splitList = splitter(monthFiles,4)
        w1 = multiprocessing.Process(name='wkr_0204_1', target=multi_procPatFiles0204, args=(list(splitList[0]),))
        w2 = multiprocessing.Process(name='wkr_0204_2', target=multi_procPatFiles0204, args=(list(splitList[1]),))
        w3 = multiprocessing.Process(name='wkr_0204_3', target=multi_procPatFiles0204, args=(list(splitList[2]),))
        w4 = multiprocessing.Process(name='wkr_0204_4', target=multi_procPatFiles0204, args=(list(splitList[3]),))
        
        w1.start()
        processes.append(w1)
        time.sleep(2)
        w2.start()
        processes.append(w2)
        time.sleep(2)
        w3.start()
        processes.append(w3)
        time.sleep(2)
        w4.start()
        processes.append(w4)
        
        for p in processes:
            p.join()
        print 'Workers:',processes,'finished and joined'
    
    if params['do0514']=='y':
        print '\n', 'Start Processing 2005-2014'
        monthFiles = sorted((fn for fn in os.listdir(dataPath) if fn.startswith('ipgb') and fn.endswith('.xml')))
        
        processes = []
        splitList = splitter(monthFiles,4)
        w1 = multiprocessing.Process(name='wkr_0514_1', target=multi_procPatFiles0514, args=(list(splitList[0]),))
        w2 = multiprocessing.Process(name='wkr_0514_2', target=multi_procPatFiles0514, args=(list(splitList[1]),))
        w3 = multiprocessing.Process(name='wkr_0514_3', target=multi_procPatFiles0514, args=(list(splitList[2]),))
        w4 = multiprocessing.Process(name='wkr_0514_4', target=multi_procPatFiles0514, args=(list(splitList[3]),))
        
        w1.start()
        processes.append(w1)
        time.sleep(2)
        w2.start()
        processes.append(w2)
        time.sleep(2)
        w3.start()
        processes.append(w3)
        time.sleep(2)
        w4.start()
        processes.append(w4)
        
        for p in processes:
            p.join()
        print 'Workers:',processes,'finished and joined'
    
    if params['doCollapse']=='y':
        print '\n', 'Starting collapse process'    
        processes = []
        monthFiles = sorted((fn for fn in os.listdir(mthAbsPath) if fn.startswith('patAbs') and len(fn)>15))
        years = list(set([x[6:10] for x in monthFiles]))
        
        splitList = splitter(years,4)
        w1 = multiprocessing.Process(name='wkr_collapse_1', target=multi_collapseYears, args=(list(splitList[0]),))
        w2 = multiprocessing.Process(name='wkr_collapse_2', target=multi_collapseYears, args=(list(splitList[1]),))
        w3 = multiprocessing.Process(name='wkr_collapse_3', target=multi_collapseYears, args=(list(splitList[2]),))
        w4 = multiprocessing.Process(name='wkr_collapse_4', target=multi_collapseYears, args=(list(splitList[3]),))
        
        w1.start()
        processes.append(w1)
        time.sleep(2)
        w2.start()
        processes.append(w2)
        time.sleep(2)
        w3.start()
        processes.append(w3)
        time.sleep(2)
        w4.start()
        processes.append(w4)
        
        for p in processes:
            p.join()
        print 'Workers:',processes,'finished and joined'
        
    print 'Exiting main thread'

if __name__=="__main__":
    main()