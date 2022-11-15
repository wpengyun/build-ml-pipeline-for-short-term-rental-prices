#!/usr/bin/env python
"""
Download from W&B the raw dataset and apply some basic data cleaning, exporting the result to a new artifact
"""
import argparse
import logging
import wandb
import pandas as pd
import tempfile


logging.basicConfig(level=logging.INFO, format="%(asctime)-15s %(message)s")
logger = logging.getLogger()


def go(args):

    run = wandb.init(job_type="basic_cleaning")
    run.config.update(args)

    # Download input artifact. This will also log that this script is using this
    # particular version of the artifact
    # artifact_local_path = run.use_artifact(args.input_artifact).file()
    logger.info("downloading input artifact")
    artifact_local_path = run.use_artifact(args.input_artifact).file()
    df=pd.read_csv(artifact_local_path)
    ######################
    # YOUR CODE HERE     #
    ######################
    logger.info("removing outliers")
    df=df[df["price"].between(args.min_price, args.max_price)]
    idx = df['longitude'].between(-74.25, -73.50) & df['latitude'].between(40.5, 41.2)
    df = df[idx].copy()

    logger.info("saving and uploading output artifact")
    with tempfile.NamedTemporaryFile() as fp:
        df.to_csv(fp, index=False)
        artifact=wandb.Artifact(name=args.output_artifact,type=args.output_type,description=args.output_description)
        artifact.add_file(fp.name)
        run.log_artifact(artifact)

        artifact.wait() # wait to finish uploading

if __name__ == "__main__":

    parser = argparse.ArgumentParser(description="A very basic data cleaning")


    parser.add_argument(
        "--input_artifact", 
        type=str,
        help="name of input_artifact",
        required=True
    )

    parser.add_argument(
        "--output_artifact", 
        type=str,
        help="name of output_artifact",
        required=True
    )


    parser.add_argument(
        "--output_type", 
        type=str,
        help="type of output_artifact",
        required=True
    )

    parser.add_argument(
        "--output_description", 
        type=str,
        help="description of output",
        required=True
    )

    parser.add_argument(
        "--min_price", 
        type=float,
        help="min price",
        required=True
    )

    parser.add_argument(
        "--max_price", 
        type=float,
        help="max price",
        required=True
    )


    args = parser.parse_args()

    go(args)
