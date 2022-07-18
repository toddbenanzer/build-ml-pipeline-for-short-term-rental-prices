#!/usr/bin/env python
"""
Perform basic cleaning then save results to W and B
"""
import argparse
import logging
import wandb
import pandas as pd
import os


logging.basicConfig(level=logging.INFO, format="%(asctime)-15s %(message)s")
logger = logging.getLogger()


def go(args):

    run = wandb.init(job_type="basic_cleaning")
    run.config.update(args)

    # Download input artifact. This will also log that this script is using this
    logger.info("Downloading artifact")
    local_path = run.use_artifact(args.input_artifact).file()
    df = pd.read_csv(local_path)

    logger.info("Cleaning data")
    min_price = args.min_price
    max_price = args.max_price
    idx = df['price'].between(min_price, max_price)
    df = df[idx].copy()
    # Convert last_review to datetime
    df['last_review'] = pd.to_datetime(df['last_review'])
    # drop out-of-scope geocodes
    idx = df['longitude'].between(-74.25, -73.50) & df['latitude'].between(40.5, 41.2)
    df = df[idx].copy()
    # save file to W&B
    save_filename = "clean_sample.csv"
    df.to_csv(save_filename, index=False)

    logger.info("Saving data")
    artifact = wandb.Artifact(
        args.output_artifact,
        type=args.output_type,
        description=args.output_description,
    )
    artifact.add_file(save_filename)
    logger.info("Logging artifact")
    run.log_artifact(artifact)
    os.remove(save_filename)


if __name__ == "__main__":

    parser = argparse.ArgumentParser(description="Clean the data")


    parser.add_argument(
        "--input_artifact", 
        type=str,
        help="Input artifact to clean",
        required=True
    )

    parser.add_argument(
        "--output_artifact", 
        type=str,
        help="Output artifact to save cleaned data",
        required=True
    )

    parser.add_argument(
        "--output_type", 
        type=str,
        help="Type of output artifact",
        required=True
    )

    parser.add_argument(
        "--output_description", 
        type=str,
        help="Description of output artifact",
        required=True
    )

    parser.add_argument(
        "--min_price", 
        type=float,
        help="Minimum price to keep",
        required=True
    )

    parser.add_argument(
        "--max_price", 
        type=float,
        help="Maximum price to keep",
        required=True
    )

    args = parser.parse_args()

    go(args)
