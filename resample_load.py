import pandas as pd
import click

def load_dataframe(filename):
    return pd.Series(map(int, open(filename)
            .read()
            .split(",")))

def resample_dataframe(df, percentile, totalsum, nseconds=3600) -> pd.DataFrame:
    groups = df.groupby(by=lambda x: x//nseconds)
    total = groups.sum()
    if percentile and percentile>0.0:
        print(f"Using percentile {percentile}")
        result = groups.quantile(q=percentile)*nseconds
        if (result<total).any():
            print(f"WARNING: Using percentile {percentile} produces less rph than the input")
    elif totalsum:
        print(f"Using totalsum: {totalsum}")
        result = total
    else:
        raise ValueError("Either percentile or totalsum must be specified")
    return result.astype(int)

@click.command()
@click.option('--percentile', help='Use a percentile as guaranteed workload (default=1.0, meaning max workload in the hour)', 
    required=False, type=click.FLOAT, default=None)
@click.option('--totalsum', help='Use sum of rps as rph (default TRUE)', 
    required=False, type=click.BOOL, default=True)
@click.argument("filename")
@click.argument("output")
def main(filename, output, percentile, totalsum):
    if not totalsum and not percentile:
        print("Either --totalsum or --percentile=<value> should be used")
        exit()

    df = load_dataframe(filename)
    rph = resample_dataframe(df, percentile=percentile, totalsum=totalsum)
    with open(output, "w") as f:
        f.write(",".join(map(str, rph)))

if __name__ == "__main__":
    main()