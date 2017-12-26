from bs4 import BeautifulSoup
import MySQLdb as mysql
import requests
import re
import pprint
import argparse

pp = pprint.PrettyPrinter(indent=4)

db = mysql.connect("localhost", "", "", "")
cursor = db.cursor()

# url = "https://www.moneyworks4me.com/best-index/top-stocks/top-small-cap-companies-list/orderby/industry/sort/asc/page/"
# totalPages = 472

def requestHandler(url, page):
	r = requests.get(url + str(page))
	if r.status_code == 200:
		print url + str(page)
		return r.content
	else:
		return False

def make_it_beautiful(content, data):
	mappings = {
		0: 'company_name',
		1: 'sector',
		2: 'market_cap',
		3: 'price',
		4: 'pe',
		5: 'pbv'
	}

	soup = BeautifulSoup(content, 'html.parser')
	soup.prettify()

	for idx, tr in enumerate(soup.find_all('tr')):
		if idx > 0:
			obj = {}
			for j, td in enumerate(tr.find_all('td')):
				if j < 6:
					var = re.sub('\\n|\(.*\)', '', td.text).strip()
					if j >= 3 and j < 6:
						if var == '-':
							var = 0
						else:
							var = float(var)

					obj[mappings[j]] = var

			data.append(obj)

	return data

def add_to_db(data):
	success = 0
	error = 0
	t = []

	for obj in data:
		query = 'INSERT INTO nse_companies (company_name, sector, market_cap, price, pe, pbv) VALUES ("' + obj['company_name'] + '", "' + obj['sector'] + '", "' + obj['market_cap'] + '", ' + str(obj['price']) + ', ' + str(obj['pe']) + ', ' + str(obj['pbv']) + ');'
		
		try:
			cursor.execute(query)
			db.commit()

			print query
			success = success + 1
			print "Successfully executed query \n"
		except:
			t.append(query)

			error = error + 1
			print "Error in query"
			db.rollback()

	string = ''
	for q in t:
		string += q + "\n"

	logfile = open('error_log.log', 'w')
	logfile.write(string)
	logfile.close()

	print "Success: " + str(success)
	print "Error: " + str(error)

def main():
	parser = argparse.ArgumentParser()
	parser.add_argument('-u', '--url', required=True)
	parser.add_argument('-t', '--totalPages', required=True)
	args = parser.parse_args()

	data = []

	for i in xrange(1, int(args.totalPages)+1):
		content = requestHandler(args.url, i)
		if content:
			data = make_it_beautiful(content, data)
		else:
			print "Page not found - " + url + str(i)

	# pp.pprint(len(data))

	add_to_db(data)

	db.close()

if __name__ == "__main__":
	main()
