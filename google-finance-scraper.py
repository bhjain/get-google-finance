from threading import Thread
from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from guppy import hpy
import MySQLdb as mysql
import re
import argparse
import sys
import multiprocessing as mp
import os
import gc
import psutil
import resource
import subprocess
import signal


def getHistoricalPrices(driver):
	print "Fetching historical prices"
	try:
		driver.find_elements_by_css_selector('.fjfe-nav a')[2].click()

		records = re.search('of\ ([0-9]+)\ rows', driver.find_element_by_css_selector('.tpsd').text).group(1)

		print "Number of records: " + str(records)
		historical_prices = []
		for num in xrange(0, int(int(records)/30)+1):
			print "Page: " + str(num+1) 
			for idx,row in enumerate(driver.find_elements_by_css_selector('.historical_price tbody tr')):
				if (idx != 31):
					string = ''
					for col in row.find_elements_by_css_selector('td'):
						string += col.text + ","
					historical_prices.append(string[:-1])

			try:
				driver.find_element_by_css_selector('.SP_arrow_next').click()
			except:
				print "Last Page"

		prices = ''
		for idx,row in enumerate(historical_prices):
			if (idx < len(historical_prices)-3):
				prices = prices + row + ";"

		return prices[1:]
	except:
		return ''

def getMgmtBody(driver):
	print "Fetching management details"
	try:
		string = ''
		for row in driver.find_elements_by_css_selector('.id-mgmt-table tbody tr'):
			key = row.find_elements_by_css_selector('td')[0].text
			val = row.find_elements_by_css_selector('td')[2].text

			string = key + ":" + val + "\n"

		return string
	except:
		return ''

def getDescription(driver):
	print "Fetching description"
	try:
		desc = driver.find_element_by_css_selector('.companySummary')
		return desc.text
	except:
		return ''

def getNewsLinks(driver, company):
	print "Fetching news links"
	try:
		driver.find_element_by_css_selector(".goog-tab #news_by_date_tab_title").click()

		news = ''
		for row in driver.find_elements_by_css_selector('#news_by_date_div_cont table tbody tr a'):
			news = news + row.get_attribute("href") + ";"

		return news
	except:
		return ''

def getStats(driver):
	print "Fetching Stats"
	try:
		stats = ''
		for row in driver.find_elements_by_css_selector('table.snap-data > tbody > tr'):
			key = row.find_element_by_css_selector('td.key').text
			val = row.find_element_by_css_selector('td.val').text
			stats = stats + key + ":" + val.strip() + ";"

		for row in driver.find_elements_by_css_selector('table.quotes tbody tr'):
			key = row.find_element_by_css_selector('.lft').text
			latest_quarter = row.find_elements_by_css_selector('.period')[0].text
			previous_quarter = row.find_elements_by_css_selector('.period')[1].text

			stats = stats + key + ":" + latest_quarter + "," + previous_quarter + ";"

		return stats
	except:
		return ''

def executeQuery(company, colname, data):
	db = mysql.connect("localhost", "root", "password", "stockmarket")
	cursor = db.cursor()
	query = 'UPDATE nse_companies SET ' + colname + '="' + data + '" WHERE company_name="' + company + '";'

	try:
		cursor.execute(query)
		db.commit()

		# print query
		print "Successfully executed query \n"
	except:
		# print query

		print "Error in query"
		db.rollback()

	db.close()

def init(driver, company):
	print "Searching " + company + "\n"
	search_bar = driver.find_element_by_css_selector('input#gbqfq')
	search_bar.clear()
	search_bar.send_keys("NSE: " + company)
	search_bar.send_keys(Keys.RETURN)

	try:
		driver.find_element_by_css_selector(".goog-tab #news_by_date_tab_title")

		stats = getStats(driver)
		newsUrls = getNewsLinks(driver, company)
		desc = getDescription(driver)
		mgnt = getMgmtBody(driver)
		prices = getHistoricalPrices(driver)

		print "\n\n"
		print "Connecting to DB"

		executeQuery(company, "stats", stats)
		executeQuery(company, "newsUrls", newsUrls)
		executeQuery(company, "description", desc)
		executeQuery(company, "management", mgnt)
		executeQuery(company, "historical_prices", prices)
	except:
		try:
			print "Searching " + company + "in BSE\n"
			search_bar = driver.find_element_by_css_selector('input#gbqfq')
			search_bar.clear()
			search_bar.send_keys("BOM: " + company)
			search_bar.send_keys(Keys.RETURN)
			driver.find_element_by_css_selector(".goog-tab #news_by_date_tab_title")

			stats = getStats(driver)
			newsUrls = getNewsLinks(driver, company)
			desc = getDescription(driver)
			mgnt = getMgmtBody(driver)
			prices = getHistoricalPrices(driver)

			print "\n\n"
			print "Connecting to DB"

			executeQuery(company, "stats", stats)
			executeQuery(company, "newsUrls", newsUrls)
			executeQuery(company, "description", desc)
			executeQuery(company, "management", mgnt)
			executeQuery(company, "historical_prices", prices)
		except:
			logfile = open('error_company.log', 'a')
			logfile.write(company + "\n")
			logfile.close()
			sys.exit(0)

	finally:
		h = hpy()
		driver.service.process.send_signal(signal.SIGTERM)
		driver.quit()
		cmd = "sync; echo 3 | sudo tee /proc/sys/vm/drop_caches"
		os.system(cmd)
		os.system('pgrep phantomjs | xargs kill -9')
		print subprocess.call(['ps'])
		print subprocess.call(['free', '-m'])

def start(company):
	driver = webdriver.PhantomJS('./phantomjs')
	
	print "Opening Google Finance"
	driver.get("https://www.google.com/finance?hl=en&tab=ee")
	init(driver, company)

if __name__ == "__main__":
	db = mysql.connect("localhost", "", "", "")
	cursor = db.cursor()

	query = "SELECT DISTINCT company_name FROM nse_companies WHERE company_id > 1700;"

	try:
		cursor.execute(query)
		db.commit()
	except:
		print query
		print "Error in query"
		db.rollback()
		sys.exit(0)

	results = cursor.fetchall()
	db.close()

	for idx in xrange(0, len(results), 2):
		thread1, thread2, thread3, thread4 = [''] * 4
		for i in xrange(1,5):
                        if eval(idx+i) < len(results):
                                company = re.search('\([\"\'](.*)[\'\"],\)', str(results[eval(idx+i)])).group(1)
                                eval("thread"+str(i)) = Thread(target=start, args=(company, ))

                for i in xrange(1,5):
                        if not isinstance(eval("thread" + str(i)), str):
                                eval("thread" + str(i)).start()

                        if not isinstance(eval("thread" + str(i)), str):
                                eval("thread" + str(i)).join()
                                print "thread " + str(i) + " finished...exiting"
