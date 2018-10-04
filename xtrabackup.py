#!/usr/bin/python
import datetime, os, subprocess
StatLog = "/backups/mysql/stats"
BackupLogDir = "/backups/mysql/logs/"
DailyDir = "/backups/mysql/daily/"
Now = datetime.datetime.now()
NowDate = Now.strftime('%Y-%m-%d-%H:%M')
KeepDays = 5
FullStarthour = 19
HoursBeforeFull = 24
HoursBeforeInc = 1

class Stats:
    def __init__(self):
        self.d = {}
        if os.path.isfile(StatLog):
            with open(StatLog) as f:
                for line in f:
                    (key, val) = line.split(": ")
                    self.d[key] = val.rstrip()
        else:
            self.d = {'LastIncBackup':'0','LastIncSize':'0','LastFullSize':'0','LastFullBackup':'0','LastIncPath':'none','LastFullPath':'none','Errors':'no','LastLogPath':'none'}
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
        with open(StatLog,"w+") as f:
            for line in self.d:
                f.write(line+": "+self.d[line]+'\n')

class BackupLog:
    def __init__(self):
        self.FullLogFile = BackupLogDir+NowDate+'full.log'
        self.IncLogFile = BackupLogDir+NowDate+'inc.log'
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
        self.comm_full = "innobackupex --slave-info --parallel=4 --no-timestamp "+DailyDir+NowDate+"_full"
        self.comm_inc = "innobackupex --slave-info --parallel=4 --no-timestamp --incremental"
        self.comm_finc = "innobackupex --slave-info --parallel=4 --no-timestamp --incremental --incremental-basedir="

    def MkFull(self):
        return subprocess.call(self.comm_full)    
    def MkInc(self):
        return subprocess.call(self.comm_inc)
    def MkFinc(self):
        return subprocess.call(self.comm_finc)
        
Stts = Stats()
BkpLg = BackupLog()
MkBkp = MakeBackup()
Past = datetime.strptime(Stts.GetLastFull(), '%Y-%m-%d-%H:%M')
if Stts.GetLastFull() == '0':
    BkpLg.AddLineFull('Brain: No Full Backups found in stats. Starting new full backup')
    if MkBkp.MkFull() != 0:
        Stts.SetLastLogPath(BackupLogDir+NowDate"_full.log")
        Stts.SetErrors()
        Stts.GetWriten()
        BkpLg.AddLineFull('Brain: Failed create new full backup. Look in to '+BackupLogDir+NowDate"_full.log for a problem")
elif ((Now - Past).total_seconds() / 60 / 60 >= HoursBeforeFull and
    Now.strftime('%H') >= FullStarthour):
            BkpLg.AddLineFull('Brain: Full Backup created more than 24H ago. Starting new full backup')
            if MkBkp.MkFull() != 0:
                Stts.SetLastLogPath(BackupLogDir+NowDate"_full.log")
                Stts.SetErrors()
                Stts.GetWriten()
                BkpLg.AddLineFull('Brain: Failed create new full backup. Look in to '+BackupLogDir+NowDate"_full.log for a problem")
elif Now -
    
    
else:
        


exit 0