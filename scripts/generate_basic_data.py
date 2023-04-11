import argparse
import os
import string
from os.path import exists
from random import random, choices

from tqdm import tqdm


def generate_data(multiplier):
    print("Generating data files for simplified model")

    random.seed = 2023

    # create dir if not there yet
    if not exists(f"{os.path.abspath(os.getcwd())}/sql/basic/data"):
        os.mkdir(f"{os.path.abspath(os.getcwd())}/sql/basic/data")

    create_data_for_50kx_table('t1', 16, multiplier)
    create_data_for_50kx_table('t2', 128, multiplier)
    create_data_for_50kx_table('t3', 512, multiplier)

    create_table_with_1k_nulls('ts2', 20000, multiplier)
    create_table_with_1k_nulls('ts3', 5000, multiplier)


def create_data_for_50kx_table(table_name: str, str_length: int, multiplier: int):
    if exists(f"{os.path.abspath(os.getcwd())}/sql/basic/data/{table_name}.csv"):
        print(f"Model files already presented, skipping {table_name}.csv")
    else:
        with open(
                f"{os.path.abspath(os.getcwd())}/sql/basic/data/{table_name}.csv",
                "w") as csv_file:
            for i in tqdm(range(50_000 * multiplier)):
                ng_string = ''.join(
                    choices(string.ascii_uppercase + string.digits, k=str_length))
                csv_file.write(f"{i},k2-{i},{i},{ng_string}\n")


def create_table_with_1k_nulls(table_name: str, table_size: int, multiplier: int):
    if exists(f"{os.path.abspath(os.getcwd())}/sql/basic/data/{table_name}.csv"):
        print(f"Model files already presented, skipping {table_name}.csv")
    else:
        with open(
                f"{os.path.abspath(os.getcwd())}/sql/basic/data/{table_name}.csv",
                "w") as table_file:
            for i in tqdm(range((table_size - 3000) * multiplier)):
                ng_string = ''.join(
                    choices(string.ascii_uppercase + string.digits, k=16))
                table_file.write(f"{i},k2-{i},{i},{ng_string}\n")

            for i in tqdm(range((table_size - 3000) * multiplier,
                                (table_size - 2000) * multiplier)):
                ng_string = ''.join(
                    choices(string.ascii_uppercase + string.digits, k=16))
                table_file.write(f"{i},k2-{i},NULL,{ng_string}\n")

            for i in tqdm(range((table_size - 2000) * multiplier,
                                (table_size - 1000) * multiplier)):
                table_file.write(f"{i},k2-{i},{i},NULL\n")

            for i in tqdm(range((table_size - 1000) * multiplier,
                                table_size * multiplier)):
                table_file.write(f"{i},k2-{i},NULL,NULL\n")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        prog='TAQO: basic model data generator',
        description='Generates random data for basic model')
    parser.add_argument('-m', '--multiplier', default=10)
    args = parser.parse_args()

    generate_data(args.multiplier)
