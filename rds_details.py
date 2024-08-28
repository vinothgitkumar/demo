'''
This script pulls details of all RDS Databases
python3 rds_details.py -p/--profile <sso profile name> -a/--account-name <Account Name>
python3 rds_details.py -p cn-digital-non-prod -a cn-digital-non-prod

'''

import boto3
import datetime
import csv
import argparse
import sys
from datetime import datetime, timedelta


def rds_cpu(profile,db_identifier,region):
    days_to_check = 90
    now = datetime.utcnow()
    start_time = now - timedelta(days=days_to_check)
    end_time = now + timedelta(minutes=5)
    db_id = db_identifier

    #Configure client session for given region
    sess = boto3.Session(profile_name=profile, region_name=region)
    cw_client = sess.client('cloudwatch')

    cpu_points = []

    cpu_avg = cw_client.get_metric_statistics(
                    Namespace='AWS/RDS',
                    MetricName='CPUUtilization',
                    Dimensions=[{'Name': 'DBInstanceIdentifier', 'Value': db_id}],
                    StartTime=start_time,
                    EndTime=end_time,
                    Period=10800, #every 3H, max data points 1440
                    #Statistics=['Maximum'])
                    #Statistics=['Minimum'])
                    Statistics=['Average'])

    if cpu_avg['Datapoints']:
        for datapoints in cpu_avg['Datapoints']:
            cpu_points_float = float(datapoints['Average'])
            cpu_points_cropped = round(cpu_points_float, 2)
            cpu_points.append(cpu_points_cropped)

        avg_cpu = ''
        max_cpu = ''
        min_cpu = ''

        if len(cpu_points) > 0:
            avg_cpu = round((sum(cpu_points)/len(cpu_points)), 2)
            max_cpu = max(cpu_points)
            min_cpu = min(cpu_points)
        else:
            avg_cpu = 0
            max_cpu = 0
            min_cpu = 0

        #print(f"{region_name},{db_id},{avg_cpu}%,{min_cpu}%,{max_cpu}%")
        return avg_cpu, min_cpu, max_cpu


def rds_db_connection(profile,db_identifier,region):
    days_to_check = 90
    now = datetime.utcnow()
    start_time = now - timedelta(days=days_to_check)
    end_time = now + timedelta(minutes=5)
    db_id = db_identifier

    #Configure client session for given region
    sess = boto3.Session(profile_name=profile, region_name=region)
    cw_client = sess.client('cloudwatch')

    connection_points = []

    connection_avg = cw_client.get_metric_statistics(
                    Namespace='AWS/RDS',
                    MetricName='DatabaseConnections',
                    Dimensions=[{'Name': 'DBInstanceIdentifier', 'Value': db_id}],
                    StartTime=start_time,
                    EndTime=end_time,
                    Period=10800, #every 3H, max data points 1440
                    #Statistics=['Maximum'])
                    #Statistics=['Minimum'])
                    Statistics=['Average'])

    if connection_avg['Datapoints']:
        for datapoints in connection_avg['Datapoints']:
            connection_points.append(datapoints['Average'])

        avg_connection = ''
        max_connection = ''
        min_connection = ''

        if len(connection_points) > 0:
            avg_connection = int(sum(connection_points)/len(connection_points))
            max_connection = int(max(connection_points))
            min_connection = int(min(connection_points))
        else:
            avg_connection = 0
            max_connection = 0
            min_connection = 0

        #print(f"{region_name},{db_id},{avg_cpu}%,{min_cpu}%,{max_cpu}%")
        return avg_connection, min_connection, max_connection

def rds_read_iops(profile,db_identifier,region):
    days_to_check = 90
    now = datetime.utcnow()
    start_time = now - timedelta(days=days_to_check)
    end_time = now + timedelta(minutes=5)
    db_id = db_identifier

    #Configure client session for given region
    sess = boto3.Session(profile_name=profile, region_name=region)
    cw_client = sess.client('cloudwatch')

    read_iops_points = []

    read_iops_avg = cw_client.get_metric_statistics(
                    Namespace='AWS/RDS',
                    MetricName='ReadIOPS',
                    Dimensions=[{'Name': 'DBInstanceIdentifier', 'Value': db_id}],
                    StartTime=start_time,
                    EndTime=end_time,
                    Period=10800, #every 3H, max data points 1440
                    Statistics=['Maximum'])
                    #Statistics=['Minimum'])
                    #Statistics=['Average'])

    if read_iops_avg['Datapoints']:
        for datapoints in read_iops_avg['Datapoints']:
            read_iops_points.append(datapoints['Maximum'])

        avg_read_iops = ''
        max_read_iops = ''
        min_read_iops = ''

        if len(read_iops_points) > 0:
            avg_read_iops = int(sum(read_iops_points)/len(read_iops_points))
            max_read_iops = int(max(read_iops_points))
            min_read_iops = int(min(read_iops_points))
        else:
            avg_read_iops = 0
            max_read_iops = 0
            min_read_iops = 0

        #print(f"{region_name},{db_id},{avg_cpu}%,{min_cpu}%,{max_cpu}%")
        return avg_read_iops, min_read_iops, max_read_iops


def rds_write_iops(profile,db_identifier,region):
    days_to_check = 90
    now = datetime.utcnow()
    start_time = now - timedelta(days=days_to_check)
    end_time = now + timedelta(minutes=5)
    db_id = db_identifier

    #Configure client session for given region
    sess = boto3.Session(profile_name=profile, region_name=region)
    cw_client = sess.client('cloudwatch')

    write_iops_points = []

    write_iops_avg = cw_client.get_metric_statistics(
                    Namespace='AWS/RDS',
                    MetricName='WriteIOPS',
                    Dimensions=[{'Name': 'DBInstanceIdentifier', 'Value': db_id}],
                    StartTime=start_time,
                    EndTime=end_time,
                    Period=10800, #every 3H, max data points 1440
                    Statistics=['Maximum'])
                    #Statistics=['Minimum'])
                    #Statistics=['Average'])

    if write_iops_avg['Datapoints']:
        for datapoints in write_iops_avg['Datapoints']:
            write_iops_points.append(datapoints['Maximum'])

        avg_write_iops = ''
        max_write_iops = ''
        min_write_iops = ''

        if len(write_iops_points) > 0:
            avg_write_iops = int(sum(write_iops_points)/len(write_iops_points))
            max_write_iops = int(max(write_iops_points))
            min_write_iops = int(min(write_iops_points))
        else:
            avg_write_iops = 0
            max_write_iops = 0
            min_write_iops = 0

        #print(f"{region_name},{db_id},{avg_cpu}%,{min_cpu}%,{max_cpu}%")
        return avg_write_iops, min_write_iops, max_write_iops


def rds_details(profile,csv_writer):
    ec2_region_client=boto3.client('ec2')

    #First row for the output csv file
    csv_writer.writerow(['Region','Cluster Name','DB ID','DB Name','DB Instance Type','DB Instance Status','DB Engine','DB_Storage_Type','Size_GB','DB Backup Window','DB Backup Retention Days','CPU_Avg','CPU_Min','CPU_Max','DB_Connection_avg','DB_Connection_Min','DB_Connection_Max','Read_iops_avg','Read_iops_min','Read_iops_max','Write_iops_avg','Write_iops_min','Write_iops_max','Creation_Date','Tags'])

    #Get all regions
    ec2_region_response = ec2_region_client.describe_regions()
    all_regions = ec2_region_response['Regions']
    for region in all_regions:
        all_region_name=region['RegionName']

        #Configure client session for given region
        sess = boto3.Session(profile_name=profile, region_name=all_region_name)
        rds_client = sess.client('rds')

        #Calling boto3 client function
        rds_response = rds_client.describe_db_instances()
        for rds_instances in rds_response['DBInstances']:
            #Exception handling
            try:
                rds_db_cluster_id = rds_instances['DBClusterIdentifier']
            except:
                rds_db_cluster_id = "No Cluster"
            
            rds_db_id = rds_instances['DBInstanceIdentifier']

            try:
                rds_db_name = rds_instances['DBName']
            except:
                rds_db_name = 'No DB Name'

            rds_db_instance_type = rds_instances['DBInstanceClass']
            rds_db_status = rds_instances['DBInstanceStatus']
            rds_db_engine = rds_instances['Engine']
            rds_storage_db_size_in_GB = rds_instances['AllocatedStorage']
            try:
                rds_db_bkp_window = rds_instances['PreferredBackupWindow']
            except:
                rds_db_bkp_window = 'No Automated Backup Configured'

            try:
                rds_db_bkp_retention = rds_instances['BackupRetentionPeriod']
            except:
                rds_db_bkp_retention = '0'

            rds_db_storage_type = rds_instances['StorageType']
            try:
                rds_db_tags_with_comma = str(rds_instances['TagList'])
                rds_db_tags = rds_db_tags_with_comma.replace(',', ' & ')
            except:
                rds_db_tags = '-'

            rds_db_creation_date = rds_instances['InstanceCreateTime']

            cpu_avg, cpu_min, cpu_max = rds_cpu(profile,rds_db_id,all_region_name)
            db_conn_avg, db_conn_min, db_conn_max = rds_db_connection(profile,rds_db_id,all_region_name)
            riops_avg, riops_min, riops_max = rds_read_iops(profile,rds_db_id,all_region_name)
            wiops_avg, wiops_min, wiops_max = rds_write_iops(profile,rds_db_id,all_region_name)

            #Print values in the desired order
            csv_writer.writerow([all_region_name,rds_db_cluster_id,rds_db_id,rds_db_name,rds_db_instance_type,rds_db_status,rds_db_engine,rds_db_storage_type,rds_storage_db_size_in_GB,rds_db_bkp_window,rds_db_bkp_retention,cpu_avg,cpu_min,cpu_max,db_conn_avg,db_conn_min,db_conn_max,riops_avg,riops_min,riops_max,wiops_avg,wiops_min,wiops_max,rds_db_creation_date,rds_db_tags])

def main():
    a = argparse.ArgumentParser()
    a.add_argument('-p', '--profile', help='AWS SSO profile name to use', required=True)
    a.add_argument('-a', '--account-name', help='account name will be used for output file name', required=True)

    #Parse the given commandline arguments
    args = a.parse_args()

    profile = args.profile

    # Create a output csv file writer object
    account = args.account_name
    output_file = f"{account}-rds-details.csv"
    output_file = open(output_file, 'w')
    csv_writer = csv.writer(output_file, delimiter=',')

    rds_details(profile,csv_writer)

    #Close outfut file connection   
    output_file.close()

if __name__ == "__main__":
    main()