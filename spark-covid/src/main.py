import argparse
import importlib
import os
import sys
import time

from shared.session import create_spark_session

if os.path.exists('libs.zip'):
    sys.path.insert(0, 'libs.zip')
else:
    sys.path.insert(0, './libs')

if os.path.exists('jobs.zip'):
    sys.path.insert(0, 'jobs.zip')
else:
    sys.path.insert(0, './jobs')


def main():
    parser = argparse.ArgumentParser(description='Run a PySpark job')
    parser.add_argument('--job', type=str, required=True, dest='job_name', help="The name of the job module you want "
                                                                                "to run. (ex: poc will run job on "
                                                                                "jobs.poc package)")
    parser.add_argument('--input-data', type=str, required=True, dest='input_name',
                        help="The path to the directory that contains the input data")
    parser.add_argument('--output-data', type=str, required=True, dest='output_name',
                        help="The path to the directory that contains the output data")

    args = parser.parse_args()
    print("Called with arguments: %s" % args)

    print('\nRunning job %s...\ninput  is %s\noutput is %s\n' % (args.job_name, args.input_name, args.output_name))

    job_module = importlib.import_module('jobs.%s' % args.job_name)

    start = time.time()
    spark = create_spark_session()

    # Call the job provided in the arguments
    job_module.analyze(spark, args.input_name, args.output_name)
    end = time.time()

    print("\nExecution of job %s took %s seconds" % (args.job_name, end - start))


if __name__ == "__main__":
    main()