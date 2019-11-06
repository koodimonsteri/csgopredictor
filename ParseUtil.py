'''
Helper functions to parse Strings
'''

# StringToInt
# Converts string to int
def SToI(mstr):
	res = 0
	try:
		res = int(mstr.strip("\n"))
	except ValueError:
		print("Failed to convert string '{}' to int!".format(mstr))
		return 0
	return res

# StringToFloat
# Converts string to int, strips few most common special characters
def SToF(mstr):
	res = 0.0
	try:
		res = float(mstr.strip("\n"))
	except ValueError:
		print("Failed to convert string '{}' to float!".format(mstr))
		return 0.0
	return res

# StringToIntTuple
# Converts string to integer tuple
def SToIT(mstr, sep):
	parts = mstr.split(sep)
	res = (0, 0)
	try:
		p1 = parts[0].strip(" ()\n")
		p2 = 0
		if len(parts) > 1:
			p2 = parts[1].strip(" ()\n")
		res = (int(p1), int(p2))

	except ValueError:
		print("Failed to convert string '{}' to int tuple!".format(mstr))
		return (0, 0)
	return res

# StringToFloatTuple
# Converts string to float tuple
def SToFT(mstr, sep):
	parts = mstr.split(sep)
	res = (0.0, 0.0)
	try:
		p1 = parts[0].strip(" ()\n")
		p2 = 0.0
		if len(parts) > 1:
			p2 = parts[1].strip(" ()\n")
		res = (float(p1), float(p2))
	except ValueError:
		print("Failed to convert string '{}' to float tuple!".format(mstr))
		return (0.0, 0.0)
	return res

# Helper function to parse start side
def parseStartSide(mstr):
	if mstr == "ct-color":
		return "ct"
	else:
		return "t"

# Function to ease start side printing
def ssNeg(ss):
	if ss == "ct":
		return "t"
	else:
		return "ct"