# Own
from ParseUtil import *
from csgoDB import MapIDsToList, MapIDsToStr

# pip
from bs4 import BeautifulSoup

# Standard
import requests

'''
Simple API to mine data from hltv.org
Important functions:
1. GetMatchResults(X)  - returns list of match urls from page X of results
2. GetMatch(matchURL)  - returns matchdata and list of mapdata
3. GetMapStats(mapURL) - returns mapdata data structure which is described below
4. GetFinishedEvents(X) - returns finished events from hltv archive page X
5. TODO: getUpcomingMatches() - return list of upcoming matchups
'''

# Form of tuple ([matchdata],[[mapdata]])
# MatchData contains [MatchID (int), Time (DateTime (str, YYYY-MM-DD HH:MM), Event (str), MapIDs (str)]
MATCHDATALENGTH = 4
MATCHID    = 0 # MatchID, same as in hltv
MATCHTIME  = 1 # Match time, form YYYY-MM-DD HH:MM
MATCHEVENT = 2 # Event name
MAPIDS     = 3 # 1-5 MapIDs as string separated by '_'

# First 11 elements in mapdata list are some general data
# Rest elements are player stats
MAPDATALENGTH = 101  # Total length of mapdata
MAPSTATOFFSET = 11   # Amount of map stat items in list
PLAYERSTATCOUNT = 9  # Amount of stats per player

# Indices in mapdata
MAPID        = 0  # MapID, same as in hltv
MAPNAME      = 1  # Map name
TEAM1NAME    = 2  # Team1 name
TEAM2NAME    = 3  # Team2 name
FIRSTHALFT1  = 4  # First half Team1 rounds
FIRSTHALFT2  = 5  # First half Team2 rounds
SECONDHALFT1 = 6  # Second half Team1 rounds
SECONDHALFT2 = 7  # Second half Team2 rounds
OTROUNDST1   = 8  # Overtime Team1 rounds
OTROUNDST2   = 9  # Overtime Team2 rounds
STARTSIDET1  = 10 # Start side of Team1

# Player stat indice offsets in mapdata
# Idx = PX * PLAYERSTATCOUNT + MAPSTATOFFSET + Offset
# Players 1-10
PNAME    = 0 # Team1 player1 name
PKILLS   = 1 # Team1 player1 kills
PASSISTS = 2 # Team1 player1 assists
PDEATHS  = 3 # Team1 player1 deaths
PADR     = 4 # Team1 player1 AverageDamageperRound
PHS      = 5 # Team1 player1 HeadShot %
PFA      = 6 # Team1 player1 FlashAssists
PFKDIFF  = 7 # Team1 player1 FirstKill difference
PRATING  = 8 # Team1 player1 Rating
#PIMPACT  = 9 # Team1 player1 Impact

# Helper function to request htlv page
def requestHLTV(url):
	url = "https://www.hltv.org" + url
	headers = {
        "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"
	}
	req = requests.get(url, headers=headers)
	return req

# Returns match URLs from 1 result page
# Parameters:
# X : page number, (int)
#
# Returns: list of match urls, [(str)]
def GetMatchResultsPage(X, debug=False):
	url = "/results"
	if X < 0:
		return []
	else:
		url = url + "?offset=" + str(X * 100)
	#print(url)
	print("Parsing results page{}: {}".format(X, url))
	req = requestHLTV(url)

	bs = BeautifulSoup(req.content, "html.parser")

	urls = []

	for rc in bs("div", "result-con"):
		for rcc in rc.children:
			if debug:
				print("Result link on page {}:{}".format(X, rcc["href"]))
			urls.append(rcc["href"])
	return urls

# Requests, parses and returns data of one match from hltv
# Parameters:
# mapURL : specific map url in hltv, (str)
#
# Returns: all map stats, [mapdata]
def GetMapStats(mapURL, debug=False):
	print("Parsing map stats page:", mapURL)

	req = requestHLTV(mapURL)

	bs = BeautifulSoup(req.content, "html.parser")
	mapData = [None] * MAPDATALENGTH

	# First parse mapID
	mapData[MAPID] = SToI(mapURL.split("/")[-2])

	# Parse teamnames, datetime and mapname
	minfobox = bs.find("div", "match-info-box-con")
	mapData[TEAM1NAME] = minfobox.find("div", "team-left").contents[0]["title"].strip("\n")
	mapData[TEAM2NAME] = minfobox.find("div", "team-right").contents[0]["title"].strip("\n")
	mapData[MAPNAME]   = minfobox.find("div", "small-text").next_sibling.strip()
	
	# Debug print
	if debug:
		print("mapID: {}\nMap: {}\nTeams: {} - {}".format(
			mapData[MAPID], mapData[MAPNAME],
			mapData[TEAM1NAME], mapData[TEAM2NAME]))

	# Parse round scores and startside
	scorebox = minfobox.find_all("div", "match-info-row")[0].find("div", "right").contents
	t1totalscore          = SToI(scorebox[0].text)
	t2totalscore          = SToI(scorebox[2].text)
	mapData[FIRSTHALFT1]  = SToI(scorebox[4].text)
	mapData[FIRSTHALFT2]  = SToI(scorebox[6].text)
	mapData[SECONDHALFT1] = SToI(scorebox[8].text)
	mapData[SECONDHALFT2] = SToI(scorebox[10].text)
	mapData[OTROUNDST1]   = 0
	mapData[OTROUNDST2]   = 0
	if t1totalscore > 16 or t2totalscore > 16:  # If there was more than 16 rounds, read also overtime rounds
		temp = SToIT(scorebox[11], ':')
		mapData[OTROUNDST1] = temp[0]
		mapData[OTROUNDST2] = temp[1]
	mapData[STARTSIDET1]  = parseStartSide(scorebox[4]["class"][0])

	# Debug print
	if debug:
		print("Total: {} - {}\nFirst half: {} ({}) - {} ({})\nSecond half: {} ({}) - {} ({})\nOvertime: {} - {}".format(
			t1totalscore, t2totalscore,
			mapData[FIRSTHALFT1], mapData[STARTSIDET1],
			15 - mapData[FIRSTHALFT1], ssNeg(mapData[STARTSIDET1]),
			mapData[SECONDHALFT1], ssNeg(mapData[STARTSIDET1]),
			mapData[SECONDHALFT2], mapData[STARTSIDET1],
			mapData[OTROUNDST1], mapData[OTROUNDST2]))

	# Parse all player stats
	statstable = bs.find_all("table", "stats-table")
	playercount = 0
	for i in range(0, 2):
		tbl = statstable[i].find("tbody").find_all("tr")
		
		for j in range(0, 5):
			player = tbl[j]
			mIdx = MAPSTATOFFSET + playercount * PLAYERSTATCOUNT

			pkills   = SToIT(player.find("td", "st-kills").text, ' ')  # String 'Kills (Headshots)'
			passists = SToIT(player.find("td", "st-assists").text, ' ') # String 'Assists (FlashAssists)'

			mapData[mIdx + PNAME]    = player.find("a", href=True).text
			mapData[mIdx + PKILLS]   = pkills[0]
			mapData[mIdx + PHS]      = pkills[1]
			mapData[mIdx + PASSISTS] = passists[0]
			mapData[mIdx + PFA]      = passists[1]
			mapData[mIdx + PDEATHS]  = SToI(player.find("td", "st-deaths").text)
			mapData[mIdx + PADR]     = SToF(player.find("td", "st-adr").text)
			mapData[mIdx + PFKDIFF]  = SToI(player.contents[-4].text)
			mapData[mIdx + PRATING]  = SToF(player.find("td", "st-rating").text)

			# Debug print
			if debug:
				print("Player: {}, K {}, HS {}, A {}, FA {}, D {}, ADR {}, FKD {}, RA {}".format(
					mapData[mIdx + PNAME], mapData[mIdx + PKILLS],
					mapData[mIdx + PHS], mapData[mIdx + PASSISTS],
					mapData[mIdx + PFA], mapData[mIdx + PDEATHS],
					mapData[mIdx + PADR], mapData[mIdx + PFKDIFF],
					mapData[mIdx + PRATING]))
			playercount += 1
	return mapData

# Requests, parses and returns match from hltv
# Parameters:
# matchURL : specific match url in hltv, (str)
#
# Returns: list of matchdata and list of mapdata lists, ([matchdata], [[mapdata]])
def GetMatch(matchURL, debug=False):
	print("Parsing match page:", matchURL)

	# Get matchID from url
	matchID = SToI(matchURL.split("/")[2])

	# From first page parse match stats link and mapstat links
	req = requestHLTV(matchURL)
	bs = BeautifulSoup(req.content, "html.parser")

	ml = bs.find("div", "small-padding stats-detailed-stats")
	if ml == None:
		print("Failed to locate match data link! matchURL:", matchURL)
		return None
	matchLink = ml.find("a")["href"]

	mapLinks = []

	score1 = bs.find("div", "team1-gradient").contents[2].text
	score2 = bs.find("div", "team2-gradient").contents[2].text
	maps = bs.find_all("div", class_="mapholder")

	playedMaps = SToI(score1) + SToI(score2)
	if playedMaps > 5:  # In case of BO1 scores are amount of rounds, not maps 
		playedMaps = 1

	for i in range(0, playedMaps):
		m = maps[i]
		link = m.find('a')
		if link != None:
			mapLinks.append(link["href"])
		elif debug:
			print("Failed to locate maplink!", link)

	# Debug print
	if debug:
		print("Match link:", matchLink)
		for i in range(0, len(mapLinks)):
			print(" Map{} stats link:{}".format(i+1, mapLinks[i]))

	# Request match stats page
	mreq = requestHLTV(matchLink)
	mbs = BeautifulSoup(mreq.content, "html.parser")

	# Find match info box which contains all data we need
	mbox = mbs.find("div", "match-info-box")

	# Find event and time
	mevent = mbox.find("a", "block text-ellipsis").text
	mtime = mbox.find("div", "small-text").contents[0].text

	mapDataList = []
	for ml in mapLinks:
		mapDataList.append(GetMapStats(ml, debug))

	mapIDlist = []
	for mdl in mapDataList:
		mapIDlist.append(mdl[MAPID])

	mapIDstr = MapIDsToStr(mapIDlist)

	matchData = [None] * 4
	matchData[MATCHID]    = matchID
	matchData[MATCHTIME]  = mtime
	matchData[MATCHEVENT] = mevent
	matchData[MAPIDS]     = mapIDstr

	if debug:
		print("Finished parsing match:", matchURL)
		print("MatchData:", matchData)

	return (matchData, mapDataList)

#
def GetUpcomingMatches():
	url = "/matches"
	print("Parsing upcoming matches page")

# Requests, parses and returns list of finished events from hltv
# Parameters:
# X : page number, (int)
#
# Returns: [Tournament name, Number of teams, Prize, Tournament Type]
def GetFinishedEvents(X, debug=False):
	if debug:
		print("Mining event data from HLTV archive page", X)

	url = "/events/archive"
	if X < 0:
		return []
	else:
		url = url + "?offset=" + str(X * 50)

	req = requestHLTV(url)

	bs = BeautifulSoup(req.content, "html.parser")

	events = []

	for e in bs("a", "a-reset small-event standard-box"):
		event = [None] * 4

		event[0] = e.find("td", "col-value event-col").text
		event[1] = e.find("td", "col-value small-col").text
		event[2] = e.find("td", "col-value small-col prizePoolEllipsis").text
		event[3] = e.find("td", "col-value small-col gtSmartphone-only").text

		events.append(event)

		if debug:
			print("EventName: {}\nTeams: {}\nPrize: {}\nType: {}".format(event[0], event[1], event[2], event[3]))

	return events