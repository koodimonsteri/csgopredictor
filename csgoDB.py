
# Standard
import sqlite3

# Local sqlite database for csgo data
# Database has 4 tables:
# MatchTable, MapTable, PlayerStatsTable and EventTable
#tableLabels       = ["MatchData", "MapData", "PlayerStats", "EventData"]
#matchTableLabels  = ["MatchID", "Time", "EventName", "MapIDs"]
#mapTableLabels    = ["MapID", "MapName", "Team1", "Team2","T1firsthalf", "T2firsthalf", "T1secondhalf", "T2secondhalf", "T1overtime", "T2overtime", "T1startside"]
#playerTableLabels = ["MapID", "Name", "Kills", "Assists", "Deaths", "ADR", "Headshots", "FlashAssists", "FirstKillDiff", "Rating"]
#eventTableLabels  = ["EventName", "EventTeams", "EventPrize", "EventType"]

csgoDBname = "csgodb.db"

class DB:
	def __init__(self, dbname, debug=False):
		if debug:
				print("Beginning CSGO database initialization!")
		try:
			self.dbconn = sqlite3.connect(dbname)
			self.dbconn.execute("PRAGMA foreign_keys = 1") # Allow foreign keys
			self.initializeCSGODB()
			q = '''INSERT OR IGNORE INTO PlayerStats VALUES (?,'''
			for i in range(0, 10 * 9):
				q = q + "?," if i < 10*9-1 else q + "?)"
			self.ps_query = q
			if debug:
				print("PlayerStats query:", query)

		except sqlite3.Error as e:
			print("Error in", dbname, "initialization! error:", e)

	# Initialize csgoDB tables
	# Returns: True if success, False in case of Error
	def initializeCSGODB(self, debug=False):
		try:
			#conn = sqlite3.connect(csgoDBname)
			c = self.dbconn.cursor()

			# MatchData table
			c.execute('''CREATE TABLE IF NOT EXISTS MatchData (
				MatchID integer primary key unique,
				MatchTime varchar,
				EventName varchar,
				MapIDs varchar,
				FOREIGN KEY (EventName) REFERENCES Events (EventName) ON DELETE NO ACTION)''')

			if debug:
				print("Created MatchData table")

			# MapData table
			c.execute('''CREATE TABLE IF NOT EXISTS MapData (
				MapID integer primary key unique,
				MapName varchar,
				Team1 varchar,
				Team2 varchar,
				T1firsthalf integer,
				T2firsthalf integer,
				T1secondhalf integer,
				T2secondhalf integer,
				T1overtime integer,
				T2overtime integer,
				T1startside varchar)''')

			if debug:
				print("Created MapData table")

			# Create player table query
			# And then execute it
			query = "CREATE TABLE IF NOT EXISTS PlayerStats (MapID integer unique,"
			for i in range(0,10):
				mprefix = "P" + str(i)
				pname    = mprefix + "Name varchar,"
				pkills   = mprefix + "Kills integer,"
				passists = mprefix + "Assists integer,"
				pdeaths  = mprefix + "Deaths integer,"
				padr     = mprefix + "ADR real,"
				phs      = mprefix + "HeadShots integer,"
				pfa      = mprefix + "FlashAssists integer,"
				pfkd     = mprefix + "FirstKillDifference integer,"
				prating  = mprefix + "Rating real," if i < 9 else "Rating real)"
				query = query + pname + pkills + passists + pdeaths + padr + phs + pfa + pfkd + prating
			c.execute(query)

			if debug:
				print("PlayerStats table query:", query)
				print("Created PlayerStats table!")

			# Event table
			c.execute('''CREATE TABLE IF NOT EXISTS Events (
				EventName varchar primary key unique,
				EventTeams varchar,
				EventPrize varchar,
				EventType varchar)''')

			if debug:
				print("Created Event table")

			self.dbconn.commit()
			#conn.close()

			if debug:
				print("Finished CSGO database initialization!")
			return True

		except sqlite3.Error as e:
			print("Error in csgoDB initialization:", e)
			return False

	#####################################################
	#           Match related stuff in csgoDB           #
	#####################################################

	# Insert player stats from 1 map to csgoDB
	# Parameters:
	# mapID: mapID, (int)
	# stats: list containing all player stats from 1 map
	#
	# Returns: True if success, False in case of Error
	def InsertPlayerStatsToDB(self, mapID, stats, debug=False):
		try:
			if debug:
				print("Inserting player stats to csgoDB, mapID:", mapID)

			#conn = sqlite3.connect(csgoDBname)
			c = self.dbconn.cursor()
			combined = [mapID] + stats
			# total 10 players
			c.execute(self.ps_query, combined)
			self.dbconn.commit()
			#conn.close()
			if debug:
				print("Finished inserting player stats")
			return True

		except sqlite3.Error as e:
			print("Error in inserting player stats to csgoDB:", e)
			return False

	# Insert data from single map to csgoDB
	# Parameters:
	# mapData: list containing all map data
	#
	# Returns: True if success, False in case of Error
	def InsertMap(self, mapData, debug=False):
		try:
			if debug:
				print("Inserting map to csgoDB, mapID:", mapData[0])

			#conn = sqlite3.connect(csgoDBname)
			c = self.dbconn.cursor()

			mMapData = mapData[:11]        # MAPSTATOFFSET
			mPlayerStatData = mapData[11:] # MAPSTATOFFSET
			# Insert map data
			if debug:
				print("MapData: ", mMapData)
				print("PlayerStats:", mPlayerStatData)

			query = "INSERT OR IGNORE INTO MapData VALUES(?,?,?,?,?,?,?,?,?,?,?)"

			c.execute(query, mMapData)
			self.dbconn.commit()
			#conn.close()

			# Insert player stats
			self.InsertPlayerStatsToDB(mMapData[0], mPlayerStatData, debug)

			if debug:
				print("Finished inserting map stats")
			return True

		except sqlite3.Error as e:
			print("Error in inserting map to csgoDB:", e)
			return False

	# Insert match data to csgoDB
	# Parameters:
	# matchData: list of matchdata, [matchData]
	#
	# Returns: True if success, False in case of Error
	def InsertMatch(self, matchData, debug=False):
		try:
			if debug:
				print("Inserting match to csgoDB:", matchData[0])

			#conn = sqlite3.connect(csgoDBname)
			c = self.dbconn.cursor()

			if len(matchData) != 4:
				print("Length of matchdata does not match!")
				return False

			c.execute('''INSERT OR IGNORE INTO MatchData VALUES (?, ?, ? ,?)''', matchData)

			self.dbconn.commit()
			#conn.close()

			if debug:
				print("Finished inserting match to csgoDB!")
			return True

		except sqlite3.Error as e:
			print("Error in inserting match to csgoDB:", e)
			return False

	# Queries map by its ID
	# Parameters:
	# mID: mapID, integer
	#
	# Returns: mapdata list or None
	def GetMapByID(self, mID, debug=False):
		try:
			if debug:
				print("Querying map by ID:", mID)

			#conn = sqlite3.connect(csgoDBname)
			c = self.dbconn.cursor()
			res = c.execute('''SELECT * FROM MapData WHERE mapID = ?''', (mID,)).fetchone()
			#conn.close()
			if debug:
				print("Finished querying map, result:", res)
			return res

		except sqlite3.Error as e:
			print("Error in querying map by ID:", mID, " error:", e)
			return None

	# Queries maps by matchID
	# Parameters:
	# mID: matchID, (int)
	#
	# Returns: list of mapdata lists, [[mapdata]]
	def GetMapsByMatchID(self, mID, debug=False):
		try:
			if debug:
				print("Querying maps by matchID:", mID)

			mmatch = GetMatchByID(mID, debug)
			mapids = MapIDsToList(mmatch[3])

			#conn = sqlite3.connect(csgoDBname)
			c = self.dbconn.cursor()
			res = []
			for mid in mapids:
				res.append(c.execute('''SELECT * FROM MapData WHERE MapID = ?''', (mid,)).fetchone())
			#conn.close()
			if debug:
				print("Finished querying maps by matchID, result:", res)
			return res

		except sqlite3.Error as e:
			print("Error in querying maps by matchID:", mID, " error:", e)
			return None

	# Queries match by its ID
	# Parameters:
	# mID: matchID, (int)
	#
	# Returns: matchdata list, [matchdata]
	def GetMatchByID(self, mID, debug=False):
		try:
			if debug:
				print("Querying match by ID:", mID)

			#conn = sqlite3.connect(csgoDBname)
			c = self.dbconn.cursor()
			res = c.execute('''SELECT * FROM MatchData WHERE MatchID = ?''', (mID,)).fetchone()
			#conn.close()
			if debug:
				print("Finished querying match, result:", res)
			return res

		except sqlite3.Error as e:
			print("Error in querying match by ID:", mID, " error:", e)
			return None

	# Queries player stats by mapID
	# Parameters:
	# mID: mapID, (int)
	#
	# Returns: list of player stats
	def GetPlayerStatsByMapID(self, mID, debug=False):
		try:
			if debug:
				print("Querying player stats by mapID:", mID)

			#conn = sqlite3.connect(csgoDBname)
			c = self.dbconn.cursor()
			res = c.execute('''SELECT * FROM PlayerStats WHERE MapID = ?''', (mID,)).fetchone()
			#conn.close()
			if debug:
				print("Finished querying player stats, result:", res)
			return res

		except sqlite3.Error as e:
			print("Error in querying player stats by mapID:", mID, "error:", e)
			return None

	# Queries player stats by matchID
	# Parameters:
	# mID: mapID, (int)
	#
	# Returns: list of player stats
	def GetPlayerStatsByMatchID(self, mID, debug=False):
		try:
			if debug:
				print("Querying player stats by matchID", mID)

			#conn = sqlite3.connect(csgoDBname)
			c = self.dbconn.cursor()

			match = GetMatchByID(mID)
			mapids = MapIDsToList(match[3])
			playerstats = []
			for mid in mapids:
				playerstats.append(GetPlayerStatsByMapID(mid))

			if debug:
				print("Finished querying player stats by matchID, result:", )
			return res

		except sqlite3.Error as e:
			print("Error in querying player stats by matchID:", mID, " error:", e)


	def GetTesting(self):
		try:
			#conn = sqlite3.connect(csgoDBname)
			c = self.dbconn.cursor()
			res = c.execute('''SELECT * FROM MatchData''').fetchall()
			res2 = c.execute('''SELECT * FROM MatchData WHERE EventName = ?''',("StarLadder Major Berlin 2019",)).fetchall()
			#print(res)
			#print("SELECT * FROM MatchData")
			print("SELECT * FROM MatchData WHERE EventName = StarLadder Major Berlin 2019", res2)
			#conn.close()
			return res2
		except sqlite3.Error as e:
			print("Error:", e)

	#####################################################
	#           Event related stuff in csgoDB           #
	#####################################################

	# Insert 1 event to csgodb
	# Parameters:
	# event: (4) length list containing event data
	#
	# Returns: True is success, False in case of Error
	def InsertEventToDB(self, event, debug=False):
		try:
			if debug:
				print("Inserting event:", event[0], "Teams:", event[1], "Prize:", event[2], "Type:", event[3])

			#conn = sqlite3.connect(csgoDBname)
			c = self.dbconn.cursor()

			c.execute('''INSERT OR IGNORE INTO Events VALUES (?, ?, ?, ?)''', event)
			self.dbconn.commit()
			#conn.close()

			if debug:
				print("Finished inserting event", event[0], "to csgoDB")
			return True

		except sqlite3.Error as e:
			print("Error in inserting single event to csgoDB:", e)
			return False

	# Insert multiple events to csgoDB
	# Parameters:
	# events: list of (4) length lists containing event data
	#
	# Returns: True if success, False in case of Error
	def InsertEventsToDB(self, events, debug=False):
		try:
			if debug:
				print("Inserting multiple events to csgoDB")
				for e in events:
					print(e)

			#conn = sqlite3.connect(csgoDBname)
			c = self.dbconn.cursor()

			c.executemany('''INSERT OR IGNORE INTO Events VALUES (?, ?, ?, ?)''', events)
			self.dbconn.commit()
			#conn.close()

			if debug:
				print("Finished inserting {} events to csgoDB".format(len(events)))
			return True

		except sqlite3.Error as e:
			print("Error in inserting event to csgoDB:", e)
			return False

	# Queries event by its name
	# Parameters:
	# ename: event name, (str)
	#
	# Returns: matching event or None
	def GetEventByName(self, ename, debug=False):
		try:
			if debug:
				print("Querying event by name", ename, "from csgoDB")

			#conn = sqlite3.connect(csgoDBname)
			c = self.dbconn.cursor()

			res = c.execute('''SELECT * FROM Events WHERE EventName = ?''', (ename,)).fetchall()
			#conn.close()

			if debug:
				print("Finished querying event", ename, "from csgoDB:", res)
			return res

		except sqlite3.Error as e:
			print("Error in querying event by name:", e)
			return None

	# Queries all events from csgoDB
	# Returns: list of all events in database
	def GetAllEventsFromDB(self, debug=False):
		try:
			if debug:
				print("Querying all events from csgoDB")
			#conn = sqlite3.connect(csgoDBname)
			c = self.dbconn.cursor()

			res = c.execute('''SELECT * FROM Events''').fetchall()
			#conn.close()

			if debug:
				print("Finished querying all events from csgoDB:")
				for r in res:
					print(r)
			return res

		except sqlite3.Error as e:
			print("Error in querying all events from DB:", e)
			return None


#####################################################
#              Some utility functions               #
#####################################################

# Convert list of mapIDs to string
# Parameters:
# mapIDs: list of integers
#
# Returns: string of mapIDs separated by '_'
def MapIDsToStr(mapIDs):
	res = ""
	for i in range(0, len(mapIDs)):
		if i != len(mapIDs) - 1:
			res = res + str(mapIDs[i]) + "_"
		else:
			res = res + str(mapIDs[i])
	return res

# Convert string of mapIDs to list
# Parameters:
# mapIDs: string of mapIDs separated by '_'
#
# Returns: list of integer mapIDs
def MapIDsToList(mapIDs):
	parts = mapIDs.split('_')
	for i in range(0, len(parts)):
		parts[i] = SToI(parts[i])
	return parts

# Rate event based on its features
# Feature1: amount of teams
# Feature2: prize
# Feature3: event type
def RateEventFromData(eventData, debug=False):
	featureWeights = [0.4, 0.2, 0.4]

	# Teams 4-32
	# Normalize teams to [0,1]
	# If addition -> teams+ (over n teams), normalized += 0.2
	# Feature1
	minTeams = 4
	maxTeams = 32.0
	teamAddition = False
	if not eventData[1][-1].isdigit():
		teamAddition = True
	teamAmount = SToI(eventData[1].strip('+'))
	teamRating = featureWeights[0] * (teamAmount / maxTeams)
	if teamAddition:
		teamRating = teamRating + 0.2
	if debug:
		print("Teams in event:", teamAmount, " addition:", teamAddition, " teamRating:", teamRating)

	# Prize scale from 0 to 1'500'000
	# Normalize prize money to [0, 1]
	# Other prize = 0.5
	# Feature2
	minPrize = 0
	maxPrize = 1500000.0
	moneyPrize = False
	prize = eventData[2]
	if prize[-1].isdigit():
		prize = SToI(prize.strip("$,"))
		moneyPrize = True
	prizeRating = 0.0
	if moneyPrize:
		prizeRating = featureWeights[1] * (prize / maxPrize)  # Money prize
	else:
		prizeRating = 0.5  # Other prize

	if debug:
		print("Prize in event:", prize, " moneyPrize:", moneyPrize, " prizeRating:", prizeRating)

	# Feature3
	# EventTypes from lowest to highest
	typeRatings = {"Online" : 0.3, "Local LAN" : 0.5, "Reg. LAN" : 0.7, "Intl. LAN" : 0.9}
	eventType = evenData[3]
	eventRating = featureWeights[2] * typeRatings[eventType]
	if debug:
		print("Event type:", eventType, " eventRating:", eventRating, " eventRating:", eventRating)

	# Rate event based on ratings and weights
	finalRating = teamRating + prizeRating + eventRating

	if debug:
		print("Event:", eventData[0], "final rating:", finalRating)

	return finalRating