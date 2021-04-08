#!/usr/bin/env python3

import os
import happybase
from datetime import datetime

def kpi1(conn: happybase.Connection):
    """Average number of new listings per day."""
    table = conn.table('housing')
    list(table.scan(columns=['cf2:date']))
    def getDate(x):
        k, data = x
        return datetime.strptime(data[b'cf2:date'].decode('utf-8'), '%Y-%m-%d')

    dates = list(map(getDate, table.scan(columns=[b'cf2:date'])))
    nDays = (max(dates) - min(dates)).days
    nListings = len(dates)
    print('Average number of new listings per day {}'.format(nListings/nDays))

def kpi2(conn: happybase.Connection):
    """Correlation of rent price and family income per neighborhood."""
    print('Not implemented')

if __name__ == "__main__":
    host = os.getenv('THRIFT_HOST') or 'hbase-docker' # localhost
    port = int(os.getenv('THRIFT_PORT') or '9090') # 49167
    conn = happybase.Connection(host, port)
    kpi1(conn)
    kpi2(conn)
