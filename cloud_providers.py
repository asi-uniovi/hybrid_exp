import requests
from io import StringIO
import pandas as pd
import collections

from malloovia import LimitingSet, InstanceClass

# Amazon

def get_amazon_service_url(service):
    '''Given the name of an Amazon web service (for instance, "EC2" or "S3"),
    return the url to download the bulk data in CSV format
    '''
    # Get all of Amazon services
    services = requests.get("https://pricing.us-east-1.amazonaws.com/offers/v1.0/aws/index.json")

    # Retrieve the URL for service
    url = services.json()["offers"]["Amazon" + service]["currentVersionUrl"]

    # Return the CSV version
    return "https://pricing.us-east-1.amazonaws.com" + url[:-4] + "csv"

def save_amazon_s3_data(path = ""):
    '''Gets the data about amazon S3 and saves it as a CSV file
    in the provided path (a string)
    '''
    s3_info = requests.get(get_amazon_service_url("S3"))

    # Convert the data to Pandas. use StringIO to convert the data from
    # string to something read_csv can parse
    csv = StringIO(s3_info.content.decode("utf-8"))
    all_s3_data = pd.read_csv(csv,
                              header=5) # Skip the first 5 lines: they are not data

    # Save only some columns and rename them
    interesting_columns = ["PriceDescription", "Location", 
                           "StartingRange", "EndingRange",
                           "Unit", "PricePerUnit" ]
    s3_data = all_s3_data[interesting_columns].copy()
    s3_data.columns = ["Pdesc", "Region", 
                       "StartingRange", "EndingRange",
                       "Unit", "Price"]

    # Save to CSV
    if path == "":
        filepath = "amazon_s3_data.csv"
    else:
        filepath = path + "/amazon_s3_data.csv"
    s3_data.to_csv(filepath)

def read_amazon_s3_data(path):
    '''Returns the data read from a csv file given as a path (string)
    '''
    amazon_s3_data = pd.read_csv(path)
    amazon_s3_data.Region = amazon_s3_data.Region.fillna('N/A')
    return amazon_s3_data

def save_amazon_ec2_data(path = ""):
    '''Gets the data about amazon EC2 and saves it as a CSV file
    in the provided path (a string)
    '''
    instances_info = requests.get(get_amazon_service_url("EC2"))

    # Convert the data to Pandas. use StringIO to convert the data from
    # string to something read_csv can parse
    csv = StringIO(instances_info.content.decode("utf-8"))
    all_amazon_data = pd.read_csv(csv,
                                  header=5) # Skip the first 5 lines: they are not data

    # Save only some columns and rename them
    interesting_columns = ["Instance Type", "Location", "PricePerUnit", 
                "vCPU", "Memory", "Storage",  "Operating System",
                "Unit", "LeaseContractLength", "PurchaseOption", "Tenancy",
                "OfferingClass", "PriceDescription", "Pre Installed S/W", "ECU" ]
    amazon_data = all_amazon_data[interesting_columns].copy()
    amazon_data.columns = ["Type", "Region", "Price", 
                "Cores", "Mem", "Disk",  "OS", 
                 "Unit", "Rsv", "Popt", "Tenancy", "OfferingClass", "Pdesc",
                 "Software", "ECU"]

    # Clean up data changing some NaN with more significant values
    amazon_data.Rsv = amazon_data.Rsv.fillna("No")
    amazon_data.Popt = amazon_data.Popt.fillna("N/A")

    # Save to CSV
    if path == "":
        filepath = "amazon_ec2_data.csv"
    else:
        filepath = path + "/amazon_ec2_data.csv"
    amazon_data.to_csv(filepath)

def read_amazon_ec2_data(path):
    '''Returns the data read from a csv file given as a path (string).
    NaN values in Rsv are changed to "No" and "N/A" in Popt to "N/A"
    '''
    amazon_ec2_data = pd.read_csv(path)
    amazon_ec2_data.Rsv = amazon_ec2_data.Rsv.fillna("No")
    amazon_ec2_data.Popt = amazon_ec2_data.Popt.fillna("N/A")
    amazon_ec2_data.Software = amazon_ec2_data.Software.fillna("N/A")
    return amazon_ec2_data

def get_amazon_ec2_prices(amazon_ec2_data, instance, os):
    """Receives a pandas dataframe with all the required data (columns
    "Type", "Region", "Price", "Cores", "Mem", "Disk",  "OS", "Unit",
    "Rsv", "Popt", "Tenancy", "Pdesc")  and the name of a instance type and OS
    and returns pricing information per hour as a pandas dataframe with multi-indexed
    rows by region, tenancy, leasing, and payment opts"""
    filtered = amazon_ec2_data[(amazon_ec2_data.Type == instance) & (amazon_ec2_data.OS.str.contains(os))]
    
    # Remove instances with SQL
    filtered = filtered[~filtered.Software.str.contains('SQL')]

    # Remove rows with no upfront price or partial price
    filtered = filtered[~(filtered.Popt.str.contains('No Upfront'))]
    filtered = filtered[(~filtered.Popt.str.contains('Partial Upfront'))]

    # Remove rows that are part of the "reserved" on-demand instances,
    # which have a price even when they are not used
    filtered = filtered[~filtered.Pdesc.str.contains('Unused')]

    # Remove rows with no price
    filtered = filtered[~(filtered.Price == 0)]

    # Get only Shared instances
    filtered = filtered[filtered.Tenancy == "Shared"]

    # Remove convertible instances
    filtered = filtered[filtered.OfferingClass != 'convertible']

    index_keys = ["Region", "Tenancy", "Rsv", "Popt", "Unit"]
    
    # Group using the previous keys and keep only data about price
    grouped = filtered.sort_values(by=index_keys).set_index(index_keys)[["Price"]]
    
    # Change into columns the rows with have the price per hour and
    # per unit (last element of the index, and thus the use of -1).
    # In addition, fill NaN with zeros
    grouped = grouped[["Price"]].unstack(-1).fillna(0).reset_index()
    
    # Compute the price per hour for the instances reserved for one year.
    # In the column Price/Hrs we have the upfront price divided into the
    # hours of one year plus the price per hour after the upfront, so
    # we get the total price per hour
    yr1 = grouped[grouped.Rsv=="1yr"].copy()
    yr1["Price/h"] = yr1[("Price", "Hrs")] + yr1[("Price", "Quantity")] /365 / 24

    # Idem for instances reserved for three years
    yr3 = grouped[grouped.Rsv=="3yr"].copy()
    yr3["Price/h"] = yr3[("Price", "Hrs")] + yr3[("Price", "Quantity")] /365 /24 /3

    # For on-demand instances, the price per hour is the one we already had in Price
    on_demand = grouped[grouped.Rsv=="No"].copy()
    on_demand["Price/h"] = on_demand[("Price", "Hrs")]
    
    # Join together the data from the previous computations
    prices = yr1.merge(yr3, how="outer").merge(on_demand, how="outer")
    
    # Reorder and reindex the table using the same keys as before,
    # except the last one, that has been removed in the process. Now
    # all instance types have the price per hour
    index_keys = index_keys[:-1]
    prices = prices.sort_values(by=index_keys).set_index(index_keys)

    return prices[["Price/h"]]

def get_simplified_amazon_prices(amazon_ec2_data, instance, os):
    '''Receives a pandas dataframe with all the required data (columns
    "Type", "Region", "Price", "Cores", "Mem", "Disk",  "OS", "Unit",
    "Rsv", "Popt", "Tenancy", "Pdesc")  and the name of a instance type and OS
    and returns pricing information per hour as a dictionary with two
    rows: "on_demand" and "reserved"
    '''
    prices = get_amazon_ec2_prices(amazon_ec2_data, instance, os)
    on_demand = prices.xs(("Shared", "No", "N/A"), level=(1,2,3))["Price/h"]
    reserved = prices.xs(("Shared", "1yr", "All Upfront"), level=(1,2,3))["Price/h"]
    return dict(on_demand = on_demand.to_dict(), reserved = reserved.to_dict())

def generate_amazon_region_instances(amazon_ec2_data, region_name,
                                     max_inst_per_type, max_inst_per_group,
                                     availability_zones, instance_names):
    '''Arguments:
    - amazon_ec2_data: dataframe with information about price. 
    - region_name: name of the region where we are generating the instances
    - max_inst_per_type: maximum number of on-demand instances per type
    - max_inst_per_group: maximum number of on-demand instances per region and
                and of reserved instances per availability zone
    - availability_zones: number of availability zones inside the region
    - instance_names: a list of instance names, such as 'c5.xlarge'

    Returns:
    - A list of InstaceClasses with the given region, limits and availability
      zones and
    '''
    region_data = collections.OrderedDict()
    
    for instance_name in sorted(instance_names):
        r = get_simplified_amazon_prices(amazon_ec2_data, instance_name, "Linux")
        region_data[instance_name] = dict(on_demand = r["on_demand"].get(region_name, None),
                                          reserved = r["reserved"].get(region_name, None),
                                          max_vms = max_inst_per_type)
    
    ins = []
    # The limiting set "region" sets a limit for on-demand instances
    region = LimitingSet(id=region_name, name=region_name,
                         max_vms = max_inst_per_group)
    for i,dat in region_data.items():
        if dat["on_demand"] == None:
            continue # Not all regions have all instance types
        ins.append(InstanceClass(id=f'{i}_{region_name}', name=i,
                       limiting_sets=(region,),
                       price=dat["on_demand"],
                       max_vms=dat["max_vms"], 
                       is_reserved = False,
                       time_unit="h"))
        
    # The limiting set "availability zone" sets a limit for reserved instances
    availability_zones = [ 
        LimitingSet(id=f"{region_name}_AZ{z}", name=f"{region_name}_AZ{z}",
                    max_vms = max_inst_per_group)
        for z in range(1,availability_zones + 1) 
    ]
    for i, dat in region_data.items():
        for zone in availability_zones:
            if dat["reserved"] == None:
                continue # Not all regions have all instance types
            ins.append(InstanceClass(id=f'{i}_{region_name}_{zone.name}', name=i,
                       limiting_sets=(zone,),
                       price=dat["reserved"], 
                       max_vms=0, # Reserved instances don't have limit per type
                       is_reserved = True,
                       time_unit="h"))
    return ins

# Azure

def read_azure_data(path):
    '''Returns the data read from a csv file given as a path (string).
    '''
    return pd.read_csv(path)

def generate_azure_instances(azure_data, region_name, max_cores, perf):
    '''Arguments:
    - azure_data: dataframe with information about price and VM characteristics. 
    - region_name: name of the region where we are generating the instances.
      Can be 'eu' or 'us-east-2'.
    - max_cores: maximum number of cores per region
    - perf: a dictionary where the key is the name of an instance type and the value,
            its performance

    Returns:
    - A list of InstaceClasses with the given region, limits and performance'''
    ins = []
    ls = LimitingSet(region_name, max_cores = max_cores)
    for i,dat in azure_data.iterrows():
        ins.append(InstanceClass(dat["Type"], ls,
                               performance=perf[dat["Type"]],
                               price=dat["price-"+region_name],
                               reserved = False,
                               provides={"cpus": dat["Cores"]}))

    return ins
