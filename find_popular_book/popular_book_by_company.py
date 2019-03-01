"""
Script written by Tom Slattery 
This is a script that loads a csv file into a sqlite3 database, finds book title using https://www.googleapis.com/books/v1/volumes?maxResults=1&q=, then finds the most popular books by time used for each company and prints data to console 

Running Script:
    $ python popular_book_by_company.py -f usage_sessions_20160930.csv

csv format:
    company,isbn,start_time,end_time
    Nguyen-Scott,9781491931653,2016-09-01 01:54:32+00:00,2016-09-01 01:58:32+00:00
    Nguyen-Scott,9781491956250,2016-09-01 04:01:13+00:00,2016-09-01 04:12:13+00:00

Returns:
    Company:Zhang Group
      Reactive Programming with RxJava  Minutes:424
      Fundamentals of Deep Learning  Minutes:404
      iOS 10 Programming Fundamentals with Swift  Minutes:401
      Microservice Architecture  Minutes:356
      Big Data Analytics with Spark  Minutes:233
 
Raises:
    Raises an exception if "User Rate Limit Exceeded" when calling https://www.googleapis.com/books/v1/volumes?maxResults=1&q= or if there is no internet connection
"""

import csv, sqlite3
import datetime
from datetime import timedelta
import urllib2
import json
import argparse
import sys
conn = sqlite3.connect("buzzard.sl3")
curs = conn.cursor()
curs2 = conn.cursor()

def book_flow(FILENAME):
    load_csv(FILENAME)
    find_book_titles()
    find_most_popular_title()

def load_csv(filename):
    """Drop and Create sqlite3 books table and load csv data into table. Add a column time_diff for the time difference in seconds between end_time and start_time"""
    print "Loading CSV File"
    curs.execute("DROP TABLE IF EXISTS books") 
    curs.execute("CREATE TABLE IF NOT EXISTS books (id INTEGER PRIMARY KEY, company TEXT, isbn TEXT, start_time TEXT, end_time TEXT, time_diff TEXT);")
    datetimeFormat = '%Y-%m-%d %H:%M:%S+00:00'
    with open(filename,'rb') as fin: 
        dr = csv.DictReader(fin) 
        to_db = [(i['company'], i['isbn'], i['start_time'], i['end_time'], (datetime.datetime.strptime(i['end_time'], datetimeFormat) - datetime.datetime.strptime(i['start_time'],datetimeFormat)).seconds ) for i in dr]
    curs.executemany("INSERT INTO books (company, isbn, start_time, end_time, time_diff) VALUES (?, ?, ?, ?, ?);", to_db)
    conn.commit()

def find_book_titles():
    '''Drop and Create book_titles table. 
    Find distinct isbn numbers in books table, get title from www.googleapis.com/books, and insert isbn / title into books_title table.   
    If there is no internet connection, a raise URLError will occur and script will exit.  
    If response is 403 which means User Rate Limit Exceeded, then script will exit''' 
    print "Finding book titles using www.googleapis.com/books"
    curs.execute("DROP TABLE IF EXISTS book_titles") 
    curs.execute("CREATE TABLE IF NOT EXISTS book_titles (id INTEGER PRIMARY KEY, isbn TEXT, title TEXT);")
    db = curs.execute("select distinct(isbn) from books");
    for row in db:
        print row[0]
        google_books = 'https://www.googleapis.com/books/v1/volumes?maxResults=1&q=' + row[0]
        try:
            response = urllib2.urlopen(google_books)
        except urllib2.URLError, e:
            print e.args
            sys.exit("unable to get a valid response from www.googleapis.com/books - stopping script")
        except urllib2.HTTPError, e:
            if e.code == 403:
              sys.exit("unable to get a valid response from www.googleapis.com/books User Rate Limit Exceeded - stopping script")
        else:
          data = json.load(response)
          if data["totalItems"] > 0:
            book_title = data["items"][0]["volumeInfo"]["title"]
          else:
            book_title = "Book title not available for isbn:" + row[0]
          curs2.execute("INSERT INTO book_titles (isbn, title) VALUES (?, ?);", (row[0], book_title))
    conn.commit()

def find_most_popular_title():
    '''Run query to find most popular book by company based on time and print out to console'''
    print "Running query to find most popular books"
    db = curs.execute("select company, book_titles.title, sum(time_diff) pop, books.isbn  from books, book_titles where books.isbn = book_titles.isbn group by books.isbn, company order by company, pop desc");
    title = ''
    for row in db:
        if title == row[0]: 
          print " " + row[1] + "  Minutes:" + str(row[2]/60)
        else:
          print "\n"
          print "Company:" + row[0]
          print " " + row[1] + "  Minutes:" + str(row[2]/60)
        title = row[0]

    conn.close()

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-f', help="csv file to be parsed in format company,isbn,start_time,end_time", required=True)
    args = parser.parse_args()  
    
    FILENAME = args.f

    book_flow(FILENAME)
