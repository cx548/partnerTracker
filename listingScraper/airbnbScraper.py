import datetime
import pandas as pd
import yaml
import os
import sys
sys.path.append("../")

from util.airbnb import *

list_q = ['site:https://www.airbnb.com sonder "san francisco"',
              'site:https://www.airbnb.com sonder "new york"',
              'site:https://www.airbnb.com sonder "los angeles"',
              'site:https://www.airbnb.com sonder "chicago"',
                'site:https://www.airbnb.com "Hosted by Zeus"',
                'site:https://www.airbnb.com "The Urban Flat experience"']

# get listing id
master_list = []
for q in list_q:
    current_master_list = iterateGoogleSearch(q)
    master_list += current_master_list

master_list = list(set(master_list))

# look up listing info
list_listing_info = []
for listing_id in master_list:
    print listing_id
    try:
        url_listing = 'https://api.airbnb.com/v2/listings/%s'%listing_id
        payload = {'key': 'd306zoyjsyarp7ifhu67rjxn52tv0t20','_format':'v1_legacy_for_p3'}

        r = requests.get(url_listing, params=payload)

        json_response = r.json()['listing']
        listing_info_map = {'host_name':json_response['primary_host']['first_name'],
                           'host_id':json_response['primary_host']['id'],
                            'address':json_response['address'],
                            'amenities':json_response['amenities'],
                            'bathrooms':json_response['bathrooms'],
                            'city':json_response['city'],
                            'cleaning_fee_native':json_response['cleaning_fee_native'],
                            'description':json_response['description'],
                            'listing_id':json_response['id'],
                            'lat':json_response['lat'],
                            'lng':json_response['lng'],
                            'min_nights':json_response['min_nights'],
                            'name':json_response['name'],
                            'price':json_response['price'],
                            'reviewee_count':json_response['primary_host']['reviewee_count'],
                            'property_type':json_response['property_type'],
                            'space':json_response['space'],
                            'summary':json_response['summary'],
                            'monthly_price_factor':json_response['monthly_price_factor'],
                            'min_nights':json_response['min_nights']
                           }
        list_listing_info += [listing_info_map]
    except Exception as e:
        print '%s failed'%listing_id
df_listing = pd.DataFrame(list_listing_info)
df_listing['ts'] = datetime.datetime.today()
df_listing.to_csv('airbnb_listing.csv',encoding='utf-8',index=False)

# look up calendar for each listing
today = datetime.datetime.today().date()
starting_month = today.month - 1 if today.month <> 1 else 12
n_month = 6
starting_year = today.year if today.month <> 1 else today.year - 1
# look up from previous month


df_calendar_all = pd.DataFrame()
list_listing_info = []
for listing_id in master_list:
    print listing_id
    try:
        url_calendar = 'https://www.airbnb.com/api/v2/calendar_months'

        payload = {'key': 'd306zoyjsyarp7ifhu67rjxn52tv0t20', 'listing_id': listing_id,
                   'year': starting_year, 'month': starting_month, 'count': n_month, '_format': 'with_conditions'}

        r = requests.get(url_calendar, params=payload)

        list_month = []
        for item_month in r.json()['calendar_months']:
            list_month += item_month['days']

        df_calendar = pd.DataFrame(list_month)

        df_calendar['listing_id'] = item_month['listing_id']
        df_calendar['local_price'] = df_calendar.apply(lambda row: row['price']['local_price'], axis=1)
        df_calendar['local_currency'] = df_calendar.apply(lambda row: row['price']['local_currency'], axis=1)
        df_calendar = df_calendar.drop('price', 1)

        df_calendar_all = df_calendar_all.append(df_calendar.drop_duplicates())
    except Exception as e:
        print '%s failed' % listing_id

df_calendar_all['month'] = df_calendar_all.date.apply(lambda x: x[0:7])
df_calendar_all['ts'] = datetime.datetime.today()
df_calendar_all.to_csv('airbnb_calendar.csv',encoding='utf-8',index=False)

# monthly
df_monthly_agg = df_calendar_all.groupby(['month', 'listing_id']).local_price.agg(
    ['mean', 'sum', 'count']).reset_index()
df_monthly_agg.rename(columns={'mean': 'price_nightly', 'sum': 'price_monthly', 'count': 'days_included'}, inplace=True)
df_monthly_agg['ts'] = datetime.datetime.today()
df_monthly_agg = pd.merge(df_monthly_agg, df_listing[['listing_id', 'monthly_price_factor']], on='listing_id',
                          how='left')
df_monthly_agg.to_csv('airbnb_monthly.csv',encoding='utf-8',index=False)
