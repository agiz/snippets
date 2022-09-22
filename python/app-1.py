#
# This script downloads a tar/gzip file from google storage,
# expends the archive to a set of binary files,
# unpacks the data in those files using struct
# and stores the output into bigquery.
#

import datetime
import os
import struct
import sys
import tarfile

from collections import namedtuple
from gcloud import storage
from google.cloud import bigquery
from math import ceil, log

rig_diff_max = float('-inf')

TABLE_ID = 'REDACTED'
BUCKET_NAME = 'REDACTED'

def ceil_power_of_10(n):
    exp = log(n, 10)
    exp = ceil(exp)
    return 10 ** exp

def parse(f):
    Share = namedtuple('Share',
        'timestamp share_diff rig_diff pool_diff pool_port pool_host_len'
    )

    while True:
        chunk = f.read(37)
        if len(chunk) == 0:
            break

        out = struct.unpack('QdddIB', chunk)

        (timestamp, share_diff, rig_diff, pool_diff, pool_port, pool_host_len,) = out
        out = Share(timestamp, share_diff, rig_diff, pool_diff, pool_port, pool_host_len)
        # 34 bytes is next batch

        chunk = f.read(pool_host_len)

        chunk = f.read(1)
        (pool_job_id_len,) = struct.unpack('B', chunk)

        chunk = f.read(pool_job_id_len)

        chunk = f.read(1)
        (miner_version_len,) = struct.unpack('B', chunk)

        chunk = f.read(miner_version_len)

        chunk = f.read(1)
        (miner_ip_len,) = struct.unpack('B', chunk)

        chunk = f.read(miner_ip_len)

        yield out

def readTar(fileName):
    global rig_diff_max

    t = tarfile.open(fileName, 'r:gz',)

    for m in t.getmembers():
        f = t.extractfile(m)

        if f is not None:
            data = parse(f)
            for chunk in data:
                r = chunk.rig_diff
                if r > rig_diff_max:
                    rig_diff_max = r

def main(algorithm_order, algorithm_title, year, month):
    global rig_diff_max

    client = bigquery.Client()
    table_id = TABLE_ID
    table = client.get_table(table_id)

    locations = ('stratum-eu-01', 'stratum-us-01')

    storage_client = storage.Client.from_service_account_json('./keys.json')

    bucket = storage_client.bucket(BUCKET_NAME)

    for location in locations:
        prefix = '%s/%s/%s/%s' % (location, algorithm_order, year, month)
        blobs = bucket.list_blobs(prefix=prefix)

        for blob in blobs:
            print(blob.name)
            fileName = os.path.basename(blob.name)
            blob.download_to_filename(fileName)
            readTar(fileName)
            os.remove(fileName)

    rig_diff_ceil_10 = ceil_power_of_10(rig_diff_max)
    print('rig diff max 10:', rig_diff_ceil_10)
    date = '%s-%s-01' % (year, month)

    rows_to_insert = [(date, algorithm_title, algorithm_order, rig_diff_ceil_10, 0.0,),]

    errors = client.insert_rows(table, rows_to_insert)
    if errors == []:
        pass
    else:
        print(errors)

if __name__ == '__main__':
    algorithm_order = os.environ.get('ALGORITHM_ORDER') or 1
    algorithm_title = os.environ.get('ALGORITHM_TITLE') or 'SHA256'

    if algorithm_order == None:
        print('Please specify ALGORITHM_ORDER as environment variable.')
        sys.exit(1)

    set_day = datetime.datetime.today()

    year = os.environ.get('YEAR') or set_date.year
    month = os.environ.get('MONTH') or '{:02d}'.format(set_date.month)

    main(algorithm_order, algorithm_title, year, month)
