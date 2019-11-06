# Own
from mHLTVAPI import *
from excelhelper import *
from neuralnetwork import *
from csgoDB import *

# pip
from openpyxl import Workbook
from openpyxl import load_workbook

# Standard
import os

'''
Mines data from HLTV and writes it to sqlite db.
-Request result pages from hltv
--For each page, go through all matches,
---Write data to local sqlite database
-Request finished events from hltv
--Write all to same database
'''

# Basic example of hltvAPI + csgoDB usage
def loadTestData():
	for j in range(9, 20):
		page = GetMatchResultsPage(j)
		for i in range(0, len(page)):
			mID = SToI(page[i].split("/")[2])
			existsInDB = GetMatchByID(mID)
			if existsInDB == None:
				match = GetMatch(page[i])
				if match != None:
					suc1 = InsertMatchToDB(match[0])
					for m in match[1]:
						InsertMapToDB(m)
			else:
				print("existsInDB:", existsInDB)
			if i == 24:
				print('''25% of page done!''')
			elif i == 49:
				print('''50% of page done!''')
			elif i == 74:
				print('''75% of page done!''')
			elif i == 99:
				print('''100% of page done!''')
		print("Finished parsing page", j)


def main():
	print("\n---HLTVminer starting---")
	minerdbg = False
	dbdbg = False
	mcsgoDB = DB("mcsgo.db")
	#page1 = GetMatchResultsPage(0, minerdbg)
	#match = GetMatch(page1[0], minerdbg)

	#events = GetFinishedEvents(0, minerdbg)

	dbinit = initializeCSGODB(dbdbg)
	loadTestData()

	#res = GetTesting()
	#print(res)
	'''suc1 = InsertMatchToDB(match[0],dbdbg)
	for m in match[1]:
		suc2 = InsertMapToDB(m,dbdbg)

	res1 = GetMatchByID(match[0][0],dbdbg)
	print("matchQuery:", res1)
	for m in match[1]:
		res2 = GetMapByID(m[0],dbdbg)
		res3 = GetPlayerStatsByMapID(m[0], dbdbg)
		print("mapQuery:", res2)
		print("playerStatsQuery:", res3)

	res4 = GetMapsByMatchID(match[0][0], dbdbg)
	print("mapsByMatchIDQuery:", res4)'''

	#ongo = InsertEventToDB(["MFirstEvent", "8+", "$200,000", "Intl. LAN"], dbdbg)
	#ongo2 = InsertEventToDB(["MSecondEvent", "16+", "$10,000", "Local LAN"], dbdbg)
	#ongo3 = InsertEventToDB(["MThirdEvent", "4", "Other", "Online"], dbdbg)

	#res1 = GetEventByName("MFirstEvent", dbdbg)
	#res2 = GetEventByName("MSecondEvent", dbdbg)
	#res3 = GetEventByName("MThirdEvent", dbdbg)
	#print(res1, res2, res3)

	print("---HLTVminer quitting---")
	

main()