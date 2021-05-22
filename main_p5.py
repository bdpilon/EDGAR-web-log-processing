import sys, json
import csv
from zipfile import ZipFile
from io import TextIOWrapper
import time
import netaddr
import re
import pandas as pd
import geopandas


def csv_reader(name):
    with open(name, 'r') as file:
        reader = csv.reader(file)
        all_rows = []
        for row in reader:
            all_rows.append(row)
        return(all_rows)

def node_search(depth, curr_node, target, collection):
    start = time.time()
    if target < int(collection[curr_node][0]):
        depth+=1
        new_node = curr_node - (round((len(collection)/(2**depth))))
        return node_search(depth, new_node, target, collection)
    if target> int(collection[curr_node][1]):
        depth+=1
        new_node = curr_node + (round((len(collection)/(2**depth))))
        return node_search(depth, new_node, target, collection)
    else:
        end = time.time()
        runtime = (end-start)*1000
        return collection[curr_node]
        
def binary_search(target, collection):
    root = round(len(collection)/2)
    depth = 1
    return node_search(depth, root, target, collection)
    
def check_ip(ip, collection):
    info = []
    previous_location = None
    format_ip = int(netaddr.IPAddress(ip))
    info.append(format_ip)
    start = time.time()
    if previous_location != None:
        if format_ip <= previous_location[1]:
            if format_ip >= previous_location[2]:
                end = time.time()
                info.append(previous_location)
                runtime = (end-start)*1000
                return info
    else:
        previous_location = binary_search(format_ip, collection)
        end = time.time()
        info.append(previous_location)
        runtime = (end-start)*1000
        info.append(runtime)
        return info

def zip_csv_iter(name):
    with ZipFile(name) as zf:
        with zf.open(name.replace(".zip", ".csv")) as f:
            reader = csv.reader(TextIOWrapper(f))
            for row in reader:
                yield row
                
def ip_to_add(ip):
    ip_nums = re.sub(r'[a-z]{1,3}', '000', ip)
    ip_add = netaddr.IPAddress(ip_nums)
    return ip_add

def update(old_row, collection):
    new_row = old_row
    ip_convert = re.sub(r'[a-z]{1,3}', '000', old_row[0])
    location = [check_ip(ip_convert, collection)[1][3]]
    return new_row + location
                
def sample(inputzip, outputzip, stride, collection):
    zip1 = inputzip
    zip2 = outputzip
    reader = zip_csv_iter(zip1)
    header = next(reader)
    new_header = header +['region']
    stride = int(stride)
    with ZipFile(zip2, "w") as zf:
        with zf.open(zip2.replace(".zip", ".csv"), "w") as raw:
            with TextIOWrapper(raw) as f:
                writer = csv.writer(f, lineterminator='\n')
                writer.writerow(new_header) # write the row of column names to zip2
                stride_count =0
                storage = []
                for row in reader:
                    stride_count+=1
                    if stride_count % stride == 1:
                        new_row = update(row, collection)
                        storage.append(new_row)
                storage.sort(key = lambda x:ip_to_add(x[0]))
                for row in storage:
                    writer.writerow(row) # write a row to zip2

def world_counter(zip_file):               
    reader = zip_csv_iter(zip_file)
    header = next(reader)
    header
    region_count = {}
    region_list = []
    for row in reader:
        region_list.append(row[15])
    for region in region_list:
        if region in region_count.keys():
            next
        else:
            region_count[region]= region_list.count(region)
    return region_count

def new_col(zip_file):
    world = geopandas.read_file(geopandas.datasets.get_path('naturalearth_lowres'))
    world = world[(world.pop_est>0) & (world.name!="Antarctica")]
    world['name'] 
    counts_col = []
    counts = world_counter(zip_file)
    for name in world['name']:
        if name in counts.keys():
            for region in counts.keys():
                if name == region:
                    counts_col.append(counts[region])
        else:
            counts_col.append(0)
    return counts_col

def phone_num_finder(string):
    regex = r"\(?[0-9]{3}\)?-?\s?[0-9]{3}-[0-9]{4}"
    return re.findall(regex, string)

def phone_reader_zip(zip_file):
    unzipped = []
    nums = []
    with ZipFile(zip_file) as zf:
        for info in zf.infolist():
            name = info.filename
            unzipped.append(name)
            paths = sorted(unzipped)
            zipper = zip_file
        for file in paths:
            with zf.open(file, "r") as f:
                tio = TextIOWrapper(f)
                string = tio.read()
                for item in phone_num_finder(string):
                     nums.append(item)
        return set(nums)


def main():
    if len(sys.argv) < 2:
        print("usage: main.py <command> args...")
    elif sys.argv[1] == "ip_check":
        ips = sys.argv[2:]
        csv_as_list = csv_reader( 'ip2location.csv')
        #ip_list = list(ips)
        multi_data = []
        for ip in ips:
            info=check_ip(ip,csv_as_list)
            ip_data = {}
            ip_data['ip'] = ip
            ip_data["int_ip"] = info[0]
            ip_data["region"] = info[1][3]
            ip_data["ms"] = info[2]
            multi_data.append(ip_data)
        print(json.dumps(multi_data, sort_keys=False, indent=2))
    elif sys.argv[1] == "sample":
        csv_as_list = csv_reader( 'ip2location.csv')
        sample(sys.argv[2], sys.argv[3], sys.argv[4], csv_as_list)
    elif sys.argv[1] == "world":
        world = geopandas.read_file(geopandas.datasets.get_path('naturalearth_lowres'))
        world = world[(world.pop_est>0) & (world.name!="Antarctica")]
        world['counts'] = new_col(sys.argv[2])
        world.plot(column='counts', cmap='OrRd').get_figure().savefig(sys.argv[3],format="svg")   
    elif sys.argv[1] == "phone":
        all_nums = phone_reader_zip(sys.argv[2])
        for num in all_nums:
            print(num)
    else:
        print("unknown command: "+sys.argv[1])

if __name__ == '__main__':
     main()
