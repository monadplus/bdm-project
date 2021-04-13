#!/usr/bin/env python3

import os
import json
import happybase
from datetime import datetime
from statistics import mean

def kpi1(conn: happybase.Connection):
    """Average number of new listings per day."""
    table = conn.table('housing')
    def getDate(x):
        k, data = x
        return datetime.strptime(data[b'cf2:date'].decode('utf-8'), '%Y-%m-%d')

    dates = list(map(getDate, table.scan(columns=[b'cf2:date'])))
    nDays = (max(dates) - min(dates)).days
    nListings = len(dates)
    print('Average number of new listings per day {}'.format(nListings/nDays))

def getKey(district, neighborhood):
    return f'{district}-{neighborhood}'

def kpi2(conn: happybase.Connection):
    """Correlation of rent price and family income per neighborhood."""

    table = conn.table('housing')
    table2 = conn.table('opendatabcn')
    table3 = conn.table('idealista-to-open')

    # key = district-neighborhood
    rfdByZone = dict() # value = RFD
    pricesByZone = dict() # value = [price]

    for _k, v in table.scan():
      # Union by hand
      district = v[b'cf1:district'].decode('utf-8')
      neighborhood = v[b'cf1:neighborhood'].decode('utf-8')
      k = getKey(district, neighborhood)
      row = table3.row(k, columns=['cf1:district', 'cf1:neighborhood'])
      k = getKey(row[b'cf1:district'].decode('utf-8'), row[b'cf1:neighborhood'].decode('utf-8'))
      row = table2.row(k, columns=['cf1:year-rfd'])
      yearRfd = json.loads(row[b'cf1:year-rfd'].decode('utf-8'))

      # Update data
      k = getKey(district.replace('-', ' '), neighborhood.replace('-', ' '))
      rfdByZone[k] = yearRfd['2017'] # arbitrary
      price = float(v[b'cf1:price'].decode('utf-8'))
      if k in pricesByZone:
          pricesByZone[k].append(price)
      else:
          pricesByZone[k] = [price]

    for k, rfd in rfdByZone.items():
        (district, neighborhood) = k.split('-')
        price = mean(pricesByZone[k])
        # Not the actual correlation formula but to simplify things
        correlation = price/rfd
        print(f'{neighborhood} has a correlation price/rfd = {correlation}')


if __name__ == "__main__":
    host = os.getenv('THRIFT_HOST') or 'hbase-docker' # localhost
    port = int(os.getenv('THRIFT_PORT') or '9090') # 49167
    conn = happybase.Connection(host, port)
    kpi1(conn)
    kpi2(conn)
