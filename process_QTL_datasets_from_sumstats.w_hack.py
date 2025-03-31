#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Ed Mountjoy
#

import pyspark.sql
from pyspark.sql.types import *
from pyspark.sql.functions import *
from datetime import date
import argparse
import os

def list_all_files(directory, batch_prefix=""):
    file_paths = []
    for root, _, files in os.walk(directory):
        for file in files:
            file_path = os.path.join(root, file)
            if file_path.endswith('.parquet') and "/output/sumstats" in file_path:
                if batch_prefix in file_path:
                    file_paths.append(file_path)
    return file_paths

def main(in_path, out_path, batch_prefix):

    # Make spark session
    global spark
    spark = (
        pyspark.sql.SparkSession.builder
        .config("spark.driver.memory", "8g")
        .getOrCreate()
    )
    print('Spark version: ', spark.version)

    # Get list of all parquet files
    parquet_list = list_all_files(in_path, batch_prefix)
    assert len(parquet_list) > 0, f"No parquet files found in {in_path}"

    # Load datasets
    df = spark.read.parquet(*parquet_list)

    # Filter based on bonferonni correction of number of tests per gene
    df = df.filter(col('pval') <= (0.05 / col('num_tests')))

    # Only keep runs where gene_id is not null
    df = df.filter(col('gene_id').isNotNull())

    # Hack the columns to fit the current structure
    hack = (
        df
        .withColumn('source', col('type'))
        .withColumn('feature', concat_ws('-', col('study_id'), col('bio_feature')))
        .withColumnRenamed('gene_id', 'ensembl_id')
        .withColumnRenamed('ref', 'other_allele')
        .withColumnRenamed('alt', 'effect_allele')
        .select('type', 'source', 'study_id', 'feature',
                'chrom', 'pos', 'other_allele', 'effect_allele',
                'ensembl_id', 'beta', 'se', 'pval'
        )
    )

    # Repartition
    hack = (
        hack.repartitionByRange('chrom', 'pos')
        .sortWithinPartitions('chrom', 'pos')
    ).persist()

    # Save data
    (
        hack
        .write
        .parquet(
            f"{out_path}/qtl.parquet",
            mode='overwrite'
        )
    )

    # Save a list of all (source, features)
    (
        hack
        .select('source', 'feature')
        .drop_duplicates()
        .toPandas()
        .to_json(f"{out_path}/qtl.feature_list.json", orient='records', lines=True)
    )


    
    return 0

if __name__ == '__main__':

    # Parsing command line arguments
    parser = argparse.ArgumentParser(
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
        description='This script reformats QTL studies for ingestion in V2G.')
    parser.add_argument('--input', help='QTLs from sumstats to parquet step',
                        type=str, required=True)
    parser.add_argument('--output', help='Output parquet QTLs for V2G',
                        type=str, required=True)
    parser.add_argument('--prefix', help='Batch prefix with QTLs',
                        type=str, required=True)

    args = parser.parse_args()

    # Args
    in_path = args.input
    out_path = args.output
    batch_prefix = args.prefix

    main(in_path, out_path, batch_prefix)
