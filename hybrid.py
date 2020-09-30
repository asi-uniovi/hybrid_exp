from typing import List
from collections import namedtuple
from functools import reduce
import pickle
import math
import csv

from datetime import datetime
import pandas as pd
import pulp
import numpy as np
import click

import malloovia
import cloud_providers

N_AVAILABLE_WLS = 4
WL_LEN = 24*365 # Number of hours of the workload

def create_ics(amazon_ec2_data, amazon_s3_data, instance_names,
        region_names=None):
    '''Creates the instance classes. It uses the data from Amazon. If no regions
    are given it tries to create instance classes in all the regions available
    in S3, except for the goverment regions.

    Args:
    - amazon_ec2_data: Pandas data frame with the EC2 data
    - amazon_s3_data: Pandas data frame with the S3 data
    - instance_names: list of instance names. Example: ['c5.large', 'c5.xlarge']
    - region_names: list of region names.

    Returns:
    - The list of instance classes
    - The number of regions
    '''
    if not region_names:
        region_names = list(amazon_s3_data.Region.unique())
        region_names.remove('N/A') # It is not a region
        region_names.remove('AWS GovCloud (US-East)')
        region_names.remove('AWS GovCloud (US-West)')

    print(f'Using {len(region_names)} regions')

    ics = []
    for region_name in region_names:
        region_ics = cloud_providers.generate_amazon_region_instances(
            amazon_ec2_data, region_name,
            max_inst_per_type=20, max_inst_per_group=20,
            availability_zones=3, instance_names=instance_names)
        ics.extend(region_ics)

    print(f'There are {len(ics)} instance classes in {len(region_names)} regions')

    return ics, len(region_names)

def get_perfs(
    amazon_ec2_data: pd.DataFrame,
    ics: List[malloovia.InstanceClass],
    apps: List[malloovia.App],
    perf_factor: int,
    perfs_per_ecu: List[int],
    priv_ecus: int,
    ) -> malloovia.PerformanceSet:

    perf_dict = {}
    for ic in ics:
        try:
            ecus = int(amazon_ec2_data[amazon_ec2_data.Type == ic.name]['ECU'].iloc[0])
        except:
            ecus = priv_ecus
        perf_dict[ic] = {
            app: perf * ecus * perf_factor
            for app, perf in zip(apps, perfs_per_ecu)
        }

    performances = malloovia.PerformanceSet(
        id="performances",
        time_unit="h",
        values=malloovia.PerformanceValues(perf_dict)
    )
    return performances

ExpResult = namedtuple('ExpResult', [
    'comp_cost_malloovia',
    'creation_time_malloovia',
    'solving_time_malloovia'])

def remove_unneded_instances_ec2(amazona_ec2_data):
     # Remove instances with SQL
    filtered = amazona_ec2_data[~amazona_ec2_data.Software.str.contains('SQL')]

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

    return filtered.copy()

def get_quanta(perf_list, quant_factor):
    '''Get the quantum for each app as the GCD multiplied by quant_factor

    Args:
    - perf_list: Receives a list of performances for each app, i.e., each element
        in the list is the list of performances for every instance for that app
    - quant_factor: factor to multiplicate the GCD
    '''
    # Get the GCD
    quanta = [] # One element per app
    for i in range(len(perf_list)):
        l = [p for p in perf_list[i]]
        quanta.append(reduce(math.gcd, l))

    return [q*quant_factor for q in quanta]

def discretize_levels(workloads, quanta):
    assert len(workloads) == len(quanta),\
       "The number of quanta (%d) is not equal to the number of apps (%d)" % (len(quanta), len(workloads))
    quantized = []
    for workload, quantum in zip(workloads, quanta):
        levels = list(range(0, max(workload)+quantum, quantum))
        load_q = np.take(levels, np.digitize(workload, levels, right=True))
        quantized.append(load_q)
    return np.array(quantized)

def reordered_list(l, first_element):
    '''Returns a list that starts with the element at index first_element and
    puts the elements before that of the original list at the end of the new
    list'''
    return l[first_element:] + l[:first_element]

def solve_problem(perf_factor, instance_names, n_apps,
                  first_app, quant_factor, exp_n, priv_n, priv_prev_n,
                  priv_ecus, priv_cost, region_names=None,
                  verbose=False):

    amazon_s3_data = cloud_providers.read_amazon_s3_data('amazon_s3_data.csv')
    amazon_ec2_data = cloud_providers.read_amazon_ec2_data('amazon_ec2_data.csv')

    # This data is also filtered later, but it is done per instance, so if we
    # remove it here, the computation is faster
    amazon_ec2_data = remove_unneded_instances_ec2(amazon_ec2_data)

    ics, _ = create_ics(amazon_ec2_data, amazon_s3_data,
        instance_names, region_names)

    # Add instance classes for the private cloud
    private_vms_count = priv_n
    ics.append(malloovia.InstanceClass(id=f'priv', name='priv',
                    limiting_sets=(),
                    price=priv_cost,
                    max_vms=private_vms_count,
                    is_reserved=True,
                    time_unit="h"))

    # Previously bought private instances
    if priv_prev_n > 0:
        ics.append(malloovia.InstanceClass(id=f'priv_prev', name='priv_prev',
                        limiting_sets=(),
                        price=0.000001, # Insignificant
                        max_vms=priv_prev_n,
                        is_reserved=True,
                        time_unit="h"))

    # Apps
    apps = [malloovia.App(f'a{i}', name=f'{i}') for i in range(n_apps)]

    # This is used for having more apps than the available workloads
    factor_repeat = math.ceil(n_apps / N_AVAILABLE_WLS)

    # Performances
    perfs_per_ecu = [1000, 500, 2000, 300]*factor_repeat # Performance for a machine with 1 ECU
    perfs_per_ecu = reordered_list(perfs_per_ecu, first_app)
    perfs = get_perfs(amazon_ec2_data, ics, apps, perf_factor, perfs_per_ecu,
                priv_ecus)

    perf_list = []
    for a in apps:
        l = list(perfs.values[i, a] for i in ics)
        perf_list.append(l)

    quanta = get_quanta(perf_list, quant_factor)

    # Workloads
    wls = []
    for i in range(N_AVAILABLE_WLS):
        with open(f'workloads/hours/wl{i}.csv') as f:
            reader = csv.reader(f)
            for row in reader:
                wls.append(tuple(int(x) for x in row))
            wls[i] = wls[i][:WL_LEN]

    wls = reordered_list(wls, first_app)
    wls = wls * factor_repeat
    wls = wls[:n_apps]

    if quant_factor != 0:
        wls = discretize_levels(wls, quanta)
        wls = wls.tolist()*factor_repeat
    else:
        wls = wls*factor_repeat

    ltwp = []
    for app, wl in zip(apps, wls):
        ltwp.append(
            malloovia.Workload(
                "ltwp_{}".format(app.id),
                description="rph for {}".format(app.name),
                app=app,
                time_unit="h",
                values=wl,
            )
        )

    problem = malloovia.Problem(
        id="Hybrid cloud",
        name="Hybrid cloud",
        workloads=ltwp,
        instance_classes=ics,
        performances=perfs,
    )

    print('Solving', datetime.now().strftime("%H:%M:%S"))
    phase_i = malloovia.PhaseI(problem)
    solver = pulp.COIN(maxSeconds=2200*60, msg=1, fracGap=0.05, threads=8)
    phase_i_solution = phase_i.solve(solver=solver)

    filename = f'sols/{exp_n:03}_sol.p'
    pickle.dump(phase_i_solution, open(filename, 'wb'))

    status = phase_i_solution.solving_stats.algorithm.status
    if status != malloovia.Status.optimal:
        print(f"No optimal solution. Status: {status.name} ({status})")
        print('Time', datetime.now().strftime("%H:%M:%S"))

        raise Exception()

    comp_cost_malloovia = phase_i_solution.solving_stats.optimal_cost
    creation_time_malloovia = phase_i_solution.solving_stats.creation_time
    solving_time_malloovia = phase_i_solution.solving_stats.solving_time

    if verbose:
        print("="*80)
        print(f"Computation cost = {comp_cost_malloovia}")
        print("="*80)

    return ExpResult(
        comp_cost_malloovia,
        creation_time_malloovia,
        solving_time_malloovia)

@click.command()
@click.option('--n-apps', help='Number of apps', required=True, type=click.INT)
@click.option('--first-app', help='Index (0, 1 or 2) of the first app to use', required=True, type=click.INT)
@click.option('--perf-factor', help='Performance factor', required=True, type=click.INT)
@click.option('--quant-factor', help='Quantization factor', required=True, type=click.INT)
@click.option('--output-prefix', help='Prefix to the output csv file name', required=True, type=click.STRING)
@click.option('--priv-n', help='Number of VMs in the private cloud', required=True, type=click.INT)
@click.option('--priv-prev-n', help='Number of VMs in the private cloud already bought', required=False, type=click.INT, default=0)
@click.option('--priv-ecus', help='ECS of VMs in the private cloud', required=True, type=click.INT)
@click.option('--priv-cost', help='Dolars per hour of VMs in the private cloud', required=True, type=click.FLOAT)
@click.option('--exp-n', help='Number of the experiment used as prefix in files', required=True, type=click.INT)
def main(n_apps, first_app, perf_factor, quant_factor, output_prefix, priv_n,
        priv_prev_n, priv_ecus, priv_cost, exp_n):
    # Uncomment to save data
    # cloud_providers.save_amazon_ec2_data()
    # cloud_providers.save_amazon_s3_data()

    instance_names = ['c5.large', 'c5.xlarge', 'c5.2xlarge', 'c5.4xlarge']
    print(f'Exp {exp_n:03} Solving for {n_apps} apps first_app {first_app}'
          f' perf_factor: {perf_factor}'
          f' quant factor: {quant_factor} instances: {len(instance_names)}'
          f' priv instances: {priv_n} priv_ecus: {priv_ecus}'
          f' priv_cost: {priv_cost}')

    # Use only two regions
    region_names = ['EU (Ireland)', 'EU (London)']
    print(f'Using only {region_names}')

    results = []
    exp_result = solve_problem(perf_factor=perf_factor,
                            instance_names=instance_names,
                            n_apps=n_apps,
                            first_app=first_app,
                            quant_factor=quant_factor,
                            exp_n=exp_n,
                            priv_n=priv_n,
                            priv_prev_n=priv_prev_n,
                            priv_ecus=priv_ecus,
                            priv_cost=priv_cost,
                            region_names=region_names,
                            verbose=True)
    results.append([exp_n, n_apps, perf_factor, first_app, quant_factor,
                priv_n, priv_ecus, priv_cost,
                *exp_result])

    df = pd.DataFrame(results)
    df.columns = ["exp", "n_apps", "perf_factor", "first_app", "quant_factor",
                "priv_n", "priv_ecus", "priv_cost",
                *ExpResult._fields]

    print(df)
    df.to_csv(f'res_malloovia/{exp_n:03}_{output_prefix}.csv')

if __name__ == "__main__":
    main()
