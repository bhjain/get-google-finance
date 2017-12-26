from subprocess import call
import MySQLdb as mysql
import sys
import re
import os

db = mysql.connect("localhost", "", "", "")
cursor = db.cursor()

query = "SELECT DISTINCT company_name FROM nse_companies WHERE company_id > 216;"

try:
	cursor.execute(query)
	db.commit()
except:
	print query
	print "Error in query"
	db.rollback()
	sys.exit(0)


db.close()
results = cursor.fetchall()

for company in results:

	try:
		company = re.search('\([\"\'](.*)[\'\"],\)', str(company)).group(1)
		cmd = 'python /home/path/google-finance-scraper.py --company "' + company + '"'
		os.system(cmd)
	except:
		print company
