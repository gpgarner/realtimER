# importing required modules
import os
import subprocess
from zipfile import ZipFile
import urllib



url = "https://cg-519a459a-0ea3-42c2-b7bc-fa1143481f74.s3-us-gov-west-1.amazonaws.com/bulk-downloads/"

year = [1980,1982,1984,1986,1988,1990,1992,1994,1996,1998,2000,2002,2004,2006,2008,2010,2012,2014,2016,2018]

files = [	"cm",
			"cn",
			"oth",
			"pas2",
			"indiv"	]
			
for i in range(len(year)):
	yr_fol = str(year[i])
	
	if (os.path.isdir(yr_fol) != True):
		cmd_mkdir = "mkdir " + yr_fol
		subprocess.call(cmd_mkdir,shell=True)
		subprocess.call(cmd_mkdir+"_zipfiles",shell=True)
	
	if (year[i]%100)<10:
		lst2dgt = "0" + str(year[i]%100)
	else:
		lst2dgt = str(year[i]%100)
		
	for j in range(len(files)):
		fnm = files[j] + lst2dgt + '.zip'
		
		fileURL = url + str(year[i]) + '/' + fnm 
		
		print fileURL
		
		fileDL = urllib.URLopener()
		fileDL.retrieve(fileURL,fnm)
		
		with ZipFile(fnm, 'r') as zip:
			# printing all the contents of the zip file
		    zip.printdir()
		 
		    # extracting all the files
		    print('Extracting all the files now from zipFileNm')
		    zip.extractall(yr_fol)
		    print('Done!')
		
		cmd_mv = "mv " + fnm + " " + yr_fol + "_zipfiles"
		
		
