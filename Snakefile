'''Using Malloovia, generates the optimal solution of different scenarios in a
hybrid cloud. Then it simulates them with simlloovia.

The file hybrid.py with different parameters is used for generating the
Malloovia solutions, which are saved in the directory "sol" as pickle files. A
summary in CSV format is saved in directory "res_malloovia".

Simlloovia leaves the results in directory "res_sim".
'''
import pandas as pd

HOURS_TO_SIMULATE=168

SCENARIOS = {
    '001': {
        'priv_new': 100,
        'priv_prev': 0,
    },
    '002': {
        'priv_new': 25,
        'priv_prev': 0,
    },
    '003': {
        'priv_new': 100,
        'priv_prev': 10,
    },
    '004': {
        'priv_new': 15,
        'priv_prev': 10,
    }
}

WORKLOADS = [
    'smooth', 'uniform'
]

rule all:
    input:
        'res_malloovia/summary.csv',
        'res_sim/summary.csv'

rule summarize_simulations:
    input:
        expand('res_sim/{exp}_{workload}.csv', exp=SCENARIOS.keys(), workload=WORKLOADS),
        expand('res_sim/{exp}_{workload}_out.txt', exp=SCENARIOS.keys(), workload=WORKLOADS),
    output:
        'res_sim/summary.csv'
    run:
        dfs = [pd.read_csv(f, names=['param', f], header=0).set_index('param') for f in input if f[-4:] == '.csv']
        df = pd.concat(dfs, axis=1).T
        df.to_csv(output[0])

rule simulate_exp:
    input:
        'sols/{exp}_sol.p',
        expand('workloads/sec_{workload}/wl{app}.csv', workload=WORKLOADS, app=range(4))
    output:
        'res_sim/{exp}_{workload}.csv',
        'res_sim/{exp}_{workload}_out.txt',
    shell:
        'simlloovia --sol-file=sols/{wildcards.exp}_sol.p'\
        ' --workload=workloads/sec_{wildcards.workload}/wl'\
        ' --workload-period=1'\
        ' --output-prefix={wildcards.exp}_{wildcards.workload} --output-dir=res_sim'\
        f' --workload-length={HOURS_TO_SIMULATE*3600}'\
        ' --save-evs=true'\
        ' --save-utils=true'

rule summarize_experiments_malloovia:
    input:
        expand('res_malloovia/{n_exp}_sol.csv', n_exp=SCENARIOS.keys())
    output:
        'res_malloovia/summary.csv'
    run:
        dfs = [pd.read_csv(f) for f in input]
        df = pd.concat(dfs)
        df.to_csv(output[0])

rule one_experiment_malloovia:
    input:
        'hybrid.py',
        expand('workloads/hours/wl{app}.csv', app=range(4)),
        'amazon_s3_data.csv',
        'amazon_ec2_data.csv'
    output:
         'res_malloovia/{n_exp}_sol.csv',
         'sols/{n_exp}_sol.p',
    params:
        priv_n = lambda wildcards: SCENARIOS[wildcards.n_exp]['priv_new'],
        priv_prev_n = lambda wildcards: SCENARIOS[wildcards.n_exp]['priv_prev']
    shell:
        'python hybrid.py --n-apps=4 '\
            '--first-app=0 '\
            '--perf-factor=1 '\
            '--quant-factor=1 '\
            '--output-prefix="sol" '\
            '--priv-n={params.priv_n} '\
            '--priv-prev-n={params.priv_prev_n} '\
            '--priv-ecus=10 '\
            '--priv-cost=0.01 '\
            '--exp-n={wildcards.n_exp}'

rule unzip_input_files:
    input:
        'workloads.tgz',
        'amazon_ec2_data.csv.gz',
        'amazon_s3_data.csv.gz'
    output:
        expand('workloads/hours/wl{app}.csv', app=range(4)),
        expand('workloads/sec_{wl}/wl{app}.csv', wl=WORKLOADS, app=range(4)),
        'amazon_s3_data.csv',
        'amazon_ec2_data.csv'
    shell:
        '''
        tar zxvf workloads.tgz
        gunzip amazon_ec2_data.csv.gz
        gunzip amazon_s3_data.csv.gz
        '''

rule clean:
    shell:
        '''rm -rf res_malloovia/* res_sim/* sols/*'''
