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

def main(in_path, out_path):

    # Make spark session
    global spark
    spark = (
        pyspark.sql.SparkSession.builder
        .config("spark.driver.memory", "8g")
        .getOrCreate()
    )
    print('Spark version: ', spark.version)

    # Load datasets
    df = spark.read.parquet(in_path)

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

    args = parser.parse_args()

    # Args
    in_path = args.input
    out_path = args.output

    main(in_path, out_path)
