'''
This script will pull the details of all stopped Instances in all region.

python3 list_all_stopped_instances.py -p/--profile <aws sso profile name> -a/--account-name <account name> 
python3 list_all_stopped_instances.py -p/--profile cn-digital-nonprod -a/--account-name cn-digital-non-prod
'''

import boto3
import csv
import argparse
from botocore.config import Config

def stopped_instances(region,profile,csv_writer):
    #Configure client session for given region
    sess = boto3.Session(profile_name=profile, region_name=region)
    ec2_client = sess.client('ec2')

    #Boto3 function call
    ec2_response = ec2_client.describe_instances()
    for ec2_instance_list in ec2_response['Reservations']:
        for ec2_ins in ec2_instance_list['Instances']:
            instate = ec2_ins['State']
            for state_key, state_value in instate.items():
                if state_key == 'Name':
                    ins_state = state_value

            if ins_state == 'stopped':
                ins_id = ec2_ins['InstanceId']
                ins_type = ec2_ins['InstanceType']
                ins_created_time = ec2_ins['LaunchTime']

                #Exception handling
                try:
                    ins_key = ec2_ins['KeyName']
                except KeyError:
                    ins_key = 'None'


                for ec2_sec_group_list in ec2_ins['SecurityGroups']:
                    ins_sec_group_name = ec2_sec_group_list['GroupName']
                    ins_sec_group_id = ec2_sec_group_list['GroupId']

                ins_state_transition = ec2_ins['StateTransitionReason']
                ins_ebs_root_device_name = ec2_ins['RootDeviceName']

                #Variable initialization
                ins_vol_id_comma = []
                ins_vol_id = ''
                ins_root_vol_id = ''
                ins_vol_size = 0
                ins_name = '-'
                tags = '-'

                for ins_block_device in ec2_ins['BlockDeviceMappings']:
                    ins_ebs_device_name = ins_block_device['DeviceName']
                    insebs = ins_block_device['Ebs']
                    for ebs_key, ebs_value in insebs.items():                
                        if ebs_key == 'VolumeId':
                            ins_vol_id_comma.append(ebs_value)
                            try:
                                vol_resp = ec2_client.describe_volumes(VolumeIds=[ebs_value])
                                for volumes_list in vol_resp['Volumes']:
                                    ins_vol_size = ins_vol_size + volumes_list['Size']
                            except:
                                continue

                            if ins_ebs_root_device_name == ins_ebs_device_name:
                                ins_root_vol_id = ebs_value

                if not ins_vol_id_comma:
                    ins_vol_id = 'No volume'
                elif len(ins_vol_id_comma) == 1:
                    ins_vol_id = ins_vol_id_comma
                else:
                    ins_vol_id_string = str(ins_vol_id_comma)
                    ins_vol_id = ins_vol_id_string.replace(',', ' & ')

                try:
                    tags_with_comma = str(ec2_ins['Tags'])
                    tags = tags_with_comma.replace(',', ' & ')

                    for tag in ec2_ins['Tags']:
                        if tag['Key'] == 'Name':
                                ins_name = tag['Value']
                except:
                    ins_name = '-'
                    tags = '-'

                #To know termination protection boolean value
                ec2_check_attribute_response = ec2_client.describe_instance_attribute(Attribute='disableApiTermination',InstanceId=ins_id)
                api_termination_check = ec2_check_attribute_response['DisableApiTermination']
                api_termination_status = api_termination_check['Value']

                #Print values in the desired order
                csv_writer.writerow([f"{region},{ins_id},{ins_name},{ins_type},{ins_state},{ins_state_transition},{tags},{ins_created_time},{ins_key},{ins_sec_group_name},{ins_sec_group_id},{ins_vol_id},{ins_vol_size},{ins_root_vol_id},{api_termination_status}"])

                #Variable value reset
                ins_vol_id_comma = []
                ins_vol_id = ''
                ins_root_vol_id = ''
                ins_vol_size = 0
                ins_name = '-'
                tags = '-'

def main():
    a = argparse.ArgumentParser()
    a.add_argument('-p', '--profile', help='AWS SSO profile name to use', required=True)
    a.add_argument('-a', '--account-name', help='account name will be used for output file name', required=True)

    #Parse the given commandline arguments
    args = a.parse_args()

    profile = args.profile

    # Create a output csv file writer object
    account = args.account_name
    output_file = f"{account}-stopped-instances-list.csv"
    output_file = open(output_file, 'w')
    csv_writer = csv.writer(output_file, delimiter=',')

    # Write the header row
    csv_writer.writerow([f"Region,Instance_Id,Instance Name,Instance_Type,Instance_State,State Transition,Tags,Instance_Created_Time,Instance_Key,Instance_Security_Group_Name,Instance_Security_Group_Id,All_Volumes,Volumes_Total_Size_GB,Root_Volume,Disable Api Termination Status"])

    #Boto3 client
    region_config = Config(region_name='us-east-1')
    ec2_region_client = boto3.client('ec2', config=region_config)

    #Get all regions
    region_response = ec2_region_client.describe_regions()
    regions = region_response['Regions']
    for all_region_name in regions:
        region=all_region_name['RegionName']
        
        #Function call
        stopped_instances(region,profile,csv_writer)

    #Close outfut file connection   
    output_file.close()

if __name__ == '__main__':
    main()