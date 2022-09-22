#
# This script opens a protobuf formatted file
# and parses the data.
#

import glob
import os
import importlib.util

spec = importlib.util.spec_from_file_location("rpc_pb2", "./rpc_pb2.py")
stratum_proto = importlib.util.module_from_spec(spec)
spec.loader.exec_module(stratum_proto)

data_folder = "./data"

def prepare_data_source(path, algorithm_id):
    files_map = {}
    for file in glob.iglob(path + '**/*.proto', recursive=True):
        basename = os.path.basename(file)
        timestamp = basename[:-6]
        if timestamp not in files_map:
            files_map[timestamp] = set()
        files_map[timestamp].add(file)
    return files_map

def prepare_data(source):
    data = []
    for timestamp, files in source:
        for file in files:
            proto_data = stratum_proto.RigOrderStatistic()
            with open(file, "rb") as f:
                proto_data.ParseFromString(f.read())
                for rig in proto_data.rigs:
                    data.append({
                        'id': rig.orderId,
                        'address': rig.btcAddress,
                        'ip': rig.ipAddress,
                    })
    return data

def analyze_data(data):
    print(data)

if __name__ == '__main__':
    files_map = prepare_data_source(data_folder, 20)
    sorted_files_map = sorted(files_map.items())

    data = prepare_data(sorted_files_map)
    analyze_data(data)
