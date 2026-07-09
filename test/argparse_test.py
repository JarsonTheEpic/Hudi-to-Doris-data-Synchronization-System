import argparse

parser = argparse.ArgumentParser()
parser.add_argument("--mode", default="full")
parser.add_argument("--table", required=True)
args = parser.parse_args()

print(f"mode={args.mode}, table={args.table}")