#!/usr/bin/env python
"""
Download from W&B the raw dataset and apply some basic data cleaning, exporting the results to a new artifact
"""
import argparse
import logging
import wandb
import pandas as pd


logging.basicConfig(level=logging.INFO, format="%(asctime)-15s %(message)s")
logger = logging.getLogger()


def go(args):

    run = wandb.init(job_type="basic_cleaning")
    run.config.update(args)

    # Download input artifact. This will also log that this script is using this
    # particular version of the artifact
    # artifact_local_path = run.use_artifact(args.input_artifact).file()

    logger.info("Getting artifact")
    artifact = run.use_artifact(args.input_artifact)
    local_path = artifact.file()
    logger.info(f"Artifact was downloaded to {local_path}.")

    logger.info("Transform input artifact to pandas dataframe.")
    df = pd.read_csv(local_path)

    # Dropping outliers in 'price' column
    logger.info('Dropping outliers in price column.')
    idx = df['price'].between(args.min_price, args.max_price)
    df = df[idx].copy()

    # Convert column type of 'last_review' from string to datetime
    logger.info('Change type to datetime of last_review column.')
    df['last_review'] = pd.to_datetime(df['last_review'])

    # Save dataframe to csv
    logger.info('Save dataframe.')
    filename = 'clean_sample.csv'
    df.to_csv(filename, index=False)

    # Upload artifact to W&B
    logger.info('Upload artifact to W&B.')
    artifact = wandb.Artifact(
        args.output_artifact, 
        type=args.output_type, 
        description=args.output_description,
    )
    artifact.add_file("clean_sample.csv")
    run.log_artifact(artifact)



if __name__ == "__main__":

    parser = argparse.ArgumentParser(description="A very basic data cleaning")


    parser.add_argument(
        "--input_artifact", 
        type=str,
        help="Fully-qualified name for the input artifact",
        required=True
    )

    parser.add_argument(
        "--output_artifact", 
        type=str,
        help="Name for the artifact",
        required=True
    )

    parser.add_argument(
        "--output_type", 
        type=str,
        help="Type for the artifact",
        required=True
    )

    parser.add_argument(
        "--output_description", 
        type=str,
        help="Description of the artifact",
        required=True
    )

    parser.add_argument(
        "--min_price", 
        type=float,
        help="Minimum renting price for short-term rental",
        required=True
    )

    parser.add_argument(
        "--max_price", 
        type=float,
        help="Maximum renting price for short-term rental",
        required=True
    )


    args = parser.parse_args()

    go(args)
