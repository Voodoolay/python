#!/usr/bin/python
import os, subprocess, datetime, shutil
StatLog = "/backups/mysql/stats"
BackupLogDir = "/backups/mysql/logs/"
DailyDir = "/backups/mysql/daily/"
Now = datetime.datetime.now()
NowDate = Now.strftime('%Y-%m-%d-%H%M')
KeepDays = 5
FullStarthour = 19
HoursBeforeFull = 24
HoursBeforeInc = 1

class Stats:
    def __init__(self):
        global StatLog
        self.d = {}
        if os.path.isfile(StatLog):
            with open(StatLog) as f:
                for line in f:
                    (key, val) = line.split(": ")
                    self.d[key] = val.rstrip()
        else:
            self.d = {'LastIncBackup':'1970-01-01-0000','LastIncSize':'0','LastFullSize':'0','LastFullBackup':'1970-01-01-0000','LastIncPath':'none','LastFullPath':'none','Errors':'no','LastLogPath':'none'}
            with open(StatLog,"w+") as f:
              for line in self.d:
                f.write(line+": "+self.d[line]+'\n')
    def GetLastFull(self):
        return self.d['LastFullBackup']
    def GetLastInc(self):
        return self.d['LastIncBackup']
    def GetLastFullPath(self):
        return self.d['LastFullPath']
    def GetLastIncPath(self):
        return self.d['LastIncPath']
    def SetLastFull(self, arg_full):
        self.d['LastFullBackup'] = arg_full
    def SetLastInc(self, arg_inc):
        self.d['LastIncBackup'] = arg_inc
    def SetLastFullPath(self,arg_full_path):
        self.d['LastFullPath'] = arg_full_path
    def SetLastIncPath(self,arg_inc_path):
        self.d['LastIncPath'] = arg_inc_path
    def SetLastIncSize(self,arg_inc_size):
        self.d['LastIncSize'] = arg_inc_size
    def SetLastFullSize(self,arg_full_size):
        self.d['LastFullSize'] = arg_full_size
    def SetLastLogPath(self,arg_log_path):
        self.d['LastLogPath'] = arg_log_path
    def SetErrors(self):
        self.d['Errors'] = "yes"
    def GetWriten(self):
        global StatLog
        with open(StatLog,"w+") as f:
            for line in self.d:
                f.write(line+": "+self.d[line]+'\n')
    def RmBadDir(self,arg_path):
        if os.path.isdir(arg_path):
        shutil.rmtree(arg_path, ignore_errors=True, onerror=None)

class BackupLog:
    def __init__(self):
        self.FullLogFile = BackupLogDir+NowDate+'_full.log'
        self.IncLogFile = BackupLogDir+NowDate+'_inc.log'
    def AddLineFull(self,arg_line_full):
        if os.path.isfile(self.FullLogFile):
            with open(self.FullLogFile,'w') as f:
                f.write(arg_line_full+'\n')
        else:
            with open(self.FullLogFile,"w+") as f:
                f.write(arg_line_full+'\n')
    def AddLineInc(self,arg_line_inc):
        if os.path.isfile(self.IncLogFile):
            with open(self.IncLogFile,'w') as f:
                f.write(arg_line_inc+'\n')
        else:
            with open(self.IncLogFile,"w+") as f:
                f.write(arg_line_inc+'\n')

class MakeBackup:
    def __init__(self):
        global BackupLogDir
        global DailyDir
        global NowDate
        self.comm_full = "innobackupex --slave-info --parallel=3 --no-timestamp "+DailyDir+NowDate+"_full >> "+BackupLogDir+NowDate+"_full.log 2>&1"
        self.comm_inc = "innobackupex --slave-info --parallel=3 --no-timestamp --incremental "+DailyDir+NowDate+"_inc --incremental-basedir="
    def MkFull(self):
        a = subprocess.call(self.comm_full, shell=True)    
        return a
    def MkInc(self,arg_last_inc):
        a = subprocess.call(self.comm_inc+arg_last_inc+" >> "+BackupLogDir+NowDate+"_inc.log 2>&1", shell=True)
        return a
    def MkFinc(self,arg_last_full):
        a = subprocess.call(self.comm_inc+arg_last_full+" >> "+BackupLogDir+NowDate+"_inc.log 2>&1", shell=True)
        return a

class Rotate:
    def __init__(self):
        global KeepDays
        global BackupLogDir
        global DailyDir
        self.tnow = time.time()
        self.old = self.now - KeepDays * 24 * 60 * 60
    def Bakups(self):
        for d in os.listdir(DailyDir):
            if os.stat(DailyDir+d).st_ctime < self.old:
        shutil.rmtree(DailyDir+d,ignore_errors=True, onerror=None)
    def Logs(self):
    for f in os.listdir(BackupLogDir):
            if os.path.isfile(BackupLogDir+f):
               if os.stat(BackupLogDir+f).st_ctime < self.old:
                  os.remove(BackupLogDir+f)        
Stts = Stats()
BkpLg = BackupLog()
MkBkp = MakeBackup()
Past = datetime.datetime.strptime(Stts.GetLastFull(), '%Y-%m-%d-%H%M')
IPast = datetime.datetime.strptime(Stts.GetLastInc(), '%Y-%m-%d-%H%M')
if Stts.GetLastFull() == '1970-01-01-0000':
    BkpLg.AddLineFull('Brain: No Full Backups found in stats. Starting new full backup')
    if MkBkp.MkFull() != 0:
        Stts.SetErrors()
        Stts.GetWriten()
        BkpLg.AddLineFull('Brain: Failed create new full backup. Look in to '+BackupLogDir+NowDate+"_full.log for a problem")
        Stts.RmBadDir(DailyDir+NowDate+"_full") 
        exit()
    else:
        Stts.SetLastLogPath(BackupLogDir+NowDate+"_full.log")
        Stts.SetLastFull(NowDate)
        Stts.SetLastFullPath(DailyDir+NowDate+"_full/")

elif ((Now - Past).total_seconds() / 60 / 60 >= HoursBeforeFull and
    int(Now.strftime('%H')) >= FullStarthour):
        BkpLg.AddLineFull('Brain: Full Backup created more than 24H ago. Starting new full backup')
        if MkBkp.MkFull() != 0:
            Stts.SetErrors()
            Stts.GetWriten()
            BkpLg.AddLineFull('Brain: Failed create new full backup. Look in to '+BackupLogDir+NowDate+"_full.log for a problem")
            Stts.RmBadDir(DailyDir+NowDate+"_full")
            exit()
        else:
            Stts.SetLastLogPath(BackupLogDir+NowDate+"_full.log")
            Stts.SetLastFull(NowDate)
            Stts.SetLastFullPath(DailyDir+NowDate+"_full/")

elif (IPast < Past and 
    int(Now.strftime('%H')) < FullStarthour and 
    ((Now - Past).total_seconds() / 60 / 60) >= HoursBeforeInc):
        BkpLg.AddLineInc('Brain: Starting new incremental from full '+Stts.GetLastInc()+' backup')
        if MkBkp.MkFinc(Stts.GetLastFullPath()) != 0:
            Stts.SetErrors()
            Stts.GetWriten()
            BkpLg.AddLineInc('Brain: Failed create new inc backup. Look in to '+BackupLogDir+NowDate+"_inc.log for a problem")
            Stts.RmBadDir(DailyDir+NowDate+"_inc")
            exit()
        else:
            Stts.SetLastLogPath(BackupLogDir+NowDate+"_inc.log")
            Stts.SetLastInc(NowDate)
            Stts.SetLastIncPath(DailyDir+NowDate+"_inc/")
elif (IPast > Past and 
    int(Now.strftime('%H')) < FullStarthour and 
    (Now - Past).total_seconds() / 60 / 60 >= HoursBeforeInc):
        BkpLg.AddLineInc('Brain: Starting new incremental from incremental '+Stts.GetLastInc()+' backup')
        if MkBkp.MkInc(Stts.GetLastIncPath()) != 0:
            Stts.SetErrors()
            Stts.GetWriten()
            BkpLg.AddLineInc('Brain: Failed create new inc backup. Look in to '+BackupLogDir+NowDate+"_inc.log for a problem")
            Stts.RmBadDir(DailyDir+NowDate+"_inc")
            exit()
        else:
            Stts.SetLastInc(NowDate)
            Stts.SetLastLogPath(BackupLogDir+NowDate+"_inc.log")
            Stts.SetLastIncPath(DailyDir+NowDate+"_inc/")
else:
    print "Nothing to do here"
    exit()
Stts.GetWriten()
Rotate.Logs()
Rotate.Backups()
exit()