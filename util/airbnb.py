import requests
import json
import pandas as pd
import time
import requests
from bs4 import BeautifulSoup

def getAirbnbListingFromGoogle(query):
    # given a google search url, e.g.:https://www.google.com/search?q=site:https://www.airbnb.com sonder&start=390
    # extract a list of listing_id
    r = requests.get(query)
    html_doc = r.text

    soup = BeautifulSoup(html_doc, 'html.parser')

    s = soup.text

    start = s.find('https://www.airbnb.com/rooms/')


    list_listing_id = []
    for segment in s[start:].split('https://www.airbnb.com/rooms/'):
        if len(segment)>0:
            listing_id = ''
            for char in segment:
                if char>='0' and char<='9':
                    listing_id += char
                else:
                    #print listing_id
                    list_listing_id.append(listing_id)
                    break
    return list_listing_id

def iterateGoogleSearch(q):
    # iterate over pages of google search results
    # INPUT q: e.g. site:https://www.airbnb.com sonder "san francisco"
    print 'Start iterating over %s'%q
    start = 0
    master_list = []
    while start<=400:
        query = 'https://www.google.com/search?q=%s&start=%d'%(q,start)
        current_list = getAirbnbListingFromGoogle(query)
        print start,len(current_list)
        if len(current_list[0])==0:
            break
        time.sleep(15)
        master_list += current_list
        start+=10
    return master_list