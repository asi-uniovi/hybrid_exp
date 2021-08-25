'''Create rph from rps using different resampling methods, use malloovia to find
the optimal allocation for each case and simulate it with simlloovia.

This script produces new "scenarios" numbered from 10 onwards. Each scenario
generates a workload in rph using a different method (stored in
workloads/hours<N_SCENARIO>) and uses malloovia to solve it, which generates
pickle solutions in "sols/<N_SCENARIO>_sol.p" and solution summaries in
"res_malloovia/<N_SCENARIO>_sol.csv". Then it uses Simlloovia to simulate it
and leaves the results in "res_sim/<N_SCENARIO>".

These are the generated scenarios:

* 10: The rph are computed as 3600 times the rps with percentile 0.9 within the
  hour
* 11: The rph are computed as 3600 times the rps with percentile 0.95 within the
  hour
* 12: The rph are computed as 3600 times the rps with percentile 0.99 within the
  hour
* 12: The rph are computed as 3600 times the rps with percentile 1.0 within the
  hour, i.e., the maximum number of requests seing in the hour
'''

apps = range(4)
percentiles = [.9, .95, .99, 1]
first_sol = 10
sols = range(first_sol, first_sol+len(percentiles))
HOURS_TO_SIMULATE = 168

WORKLOADS = [
    'smooth'
]

rule all:
    input: expand("res_sim/0{sol}.csv", sol=sols)

rule create_one_rph_file:
    input: 'workloads/sec_smooth/wl{app}.csv'
    output: 'workloads/hours{sol}/wl{app}.csv'
    params:
        percentile = lambda wildcards: percentiles[int(wildcards.sol)-first_sol]

    shell:
        'python  resample_load.py {input} {output} --percentile={params.percentile}'

rule create_malloovia_solution:
    input: 
        'hybrid.py',
        expand('workloads/hours{{sol}}/wl{app}.csv', app=range(4)),
        'amazon_s3_data.csv',
        'amazon_ec2_data.csv'
    output:
        'res_malloovia/0{sol}_sol.csv',
        "sols/0{sol}_sol.p"
    shell:
        '''
        python hybrid.py --n-apps=4 --first-app=0 --perf-factor=1 --quant-factor=1 \
        --output-prefix=sol --max_priv_new=15 --priv-prev-n=10 --priv-prev-ecus=10 \
        --priv-new-ecus=20 --priv-new-cost=0.01 --exp-n={wildcards.sol} \
        --workload-dir=workloads/hours{wildcards.sol}
        '''

rule simulate_scenario:
    input:
        'sols/01{exp}_sol.p',
        expand('workloads/sec_smooth/wl{app}.csv', app=apps)
    output:
        'res_sim/01{exp}.csv',
        'res_sim/01{exp}_out.txt',
    shell:
        'simlloovia --sol-file=sols/01{wildcards.exp}_sol.p'\
        ' --workload=workloads/sec_smooth/wl'\
        ' --workload-period=1'\
        ' --output-prefix=01{wildcards.exp} --output-dir=res_sim'\
        f' --workload-length={HOURS_TO_SIMULATE*3600}'\
        ' --save-evs=true'\
        ' --save-utils=true'

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
