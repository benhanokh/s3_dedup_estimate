#!/usr/bin/python3 -tt
import sys
import subprocess
import json
import argparse

#-------------------------------------------------------------------------------
def print_stats(name, size_kb):
    if size_kb > (1024 * 1024 * 1024):
        print("%s %.2f GiB, %.2f TiB" %
          (name, float(size_kb)/(1024*1024), float(size_kb)/(1024*1024*1024)) )
    elif size_kb > (1024 * 1024):
        print("%s %.2f MiB, %.2f GiB" %
          (name, float(size_kb)/1024, float(size_kb)/(1024*1024)) )
    elif size_kb > (1024 * 1024):
        print("%s %d KiB, %.2f MiB" %
              (name, size_kb, float(size_kb)/1024) )
    else:
        print("%s %d KiB" % (name, size_kb) )

#-------------------------------------------------------------------------------
def display_buckets_stats(bucket_list):
    try:
        stats = subprocess.check_output("radosgw-admin bucket stats 2> /dev/null", shell=True)
    except subprocess.CalledProcessError as e:
        print(f"'radosgw-admin bucket stats' failed with return code {e.returncode}")
        sys.exit(e.returncode)

    jstats = json.loads(stats)
    buckets_count    = 0
    size_kb_utilized = 0
    size_kb_actual   = 0

    for entry in jstats:
        if len(bucket_list) > 0 and entry['bucket'] not in bucket_list:
            continue

        try:
            #print("Call GC to commit all pending deletes for ", entry['bucket'])
            radosgw_command = "radosgw-admin gc process --include-all --bucket=" + entry['bucket'] + " 2> /dev/null"
            #subprocess.check_output(radosgw_command, shell=True)
        except subprocess.CalledProcessError as e:
            print(f"'radosgw-admin gc process' failed with return code {e.returncode}")
            sys.exit(e.returncode)

        buckets_count += 1
        usage = entry['usage']
        if usage:
            #rgwmain = usage["rgw.main"]
            rgwmain = usage.get('rgw.main')
            if rgwmain:
                size_kb_actual   += rgwmain["size_kb_actual"]
                size_kb_utilized += rgwmain["size_kb_utilized"]

    if len(bucket_list) == 0:
        print("We processed all %d buckets" % buckets_count)
    else:
        print("We processed %d buckets out of %d total" % (buckets_count, len(bucket_list)))
    print_stats("Buckets size actual:  ", size_kb_actual)
    print_stats("Buckets size utilized:", size_kb_utilized)

#-----------------------------------------------
def display_pool_stats_rados_df(pool_list):
    try:
        stats = subprocess.check_output("rados df -f json 2> /dev/null", shell=True)
    except subprocess.CalledProcessError as e:
        print(f"'rados df -f json' failed with return code {e.returncode}")
        sys.exit(e.returncode)
    jstats = json.loads(stats)
    pools = jstats['pools']
    pools_count = 0
    size_kb_total = 0
    for pool in pools:
        if len(pool_list) == 0 or pool['name'] in pool_list:
            pools_count += 1
            replica = 1
            if pool['num_objects'] > 0 and pool['num_object_copies'] > pool['num_objects']:
                replica = pool['num_object_copies'] / pool['num_objects']
            size_kb = pool['size_kb']
            size_kb /= replica
            size_kb_total += size_kb

    if len(pool_list) == 0:
        print("We processed all %d pools" % pools_count)
    else:
        print("We processed %d pools out of %d total" % (pools_count, len(pool_list)))
    print_stats("rados df::Pools size utilized:  ", size_kb_total)

#-----------------------------------------------
def display_pool_stats_ceph_df(pool_list):
    try:
        stats = subprocess.check_output("ceph df -f json 2> /dev/null", shell=True)
    except subprocess.CalledProcessError as e:
        print(f"'ceph df -f json' failed with return code {e.returncode}")
        sys.exit(e.returncode)
    jstats = json.loads(stats)
    pools = jstats['pools']
    pools_count = 0
    size_bytes_total = 0
    for pool in pools:
        if len(pool_list) == 0 or pool['name'] in pool_list:
            pools_count += 1
            stats = pool['stats']
            if stats:
                size_bytes_total += stats['stored']

    if len(pool_list) == 0:
        print("We processed all %d pools" % pools_count)
    else:
        print("We processed %d pools out of %d total" % (pools_count, len(pool_list)))
    print_stats("ceph df::Pools size utilized:  ", (size_bytes_total/1024))

#-----------------------------------------------
def load_list_from_file(filename, entries_list):
    try:
        file = open(filename, "r")
    except FileNotFoundError as err:
        print("load_list_from_file: Failed to open file %s" % filename);
        sys.exit(err)

    for line in file:
        entries_list.append(line.rstrip())
    file.close()

#---------------------------------------------------------------
def process_args(bucket_list, pool_list):
    parser = argparse.ArgumentParser(description='Display bucket and pools utilized space to help detect server-side copy dedup')
    parser.add_argument('--bucket_filename', dest='bucket_filename', required=False,
                        help='Filename with a list of buckets names to process (default: process all buckets)')
    parser.add_argument('--pool_filename', dest="pool_filename", required=False,
                        help='Filename with a list of pool names to process (default: process all pools)')
    args = parser.parse_args()
    if args.bucket_filename:
        load_list_from_file(args.bucket_filename, bucket_list)
    if args.pool_filename:
        load_list_from_file(args.pool_filename, pool_list)

#--------------------------------------
def main():
    bucket_list = []
    pool_list = []
    process_args(bucket_list, pool_list)
    display_buckets_stats(bucket_list)
    display_pool_stats_rados_df(pool_list)
    display_pool_stats_ceph_df(pool_list)

#--------------------------------------
if __name__ == '__main__':
    main()
