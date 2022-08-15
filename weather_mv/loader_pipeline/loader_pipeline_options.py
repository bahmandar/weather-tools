from argparse import ArgumentError
import datetime
import argparse
from .bq import ToBigQuery
from .regrid import Regrid
from .ee import ToEarthEngine
from apache_beam.options.pipeline_options import PipelineOptions

base = argparse.ArgumentParser(add_help=False)
base.add_argument('-i', '--uris', type=str, required=True,
                  help="URI glob pattern matching input weather data, e.g. 'gs://ecmwf/era5/era5-2015-*.gb'.")
base.add_argument('--topic', type=str,
                  help="A Pub/Sub topic for GCS OBJECT_FINALIZE events, or equivalent, of a cloud bucket. "
                       "E.g. 'projects/<PROJECT_ID>/topics/<TOPIC_ID>'.")
base.add_argument("--window_size", type=float, default=1.0,
                  help="Output file's window size in minutes. Only used with the `topic` flag. Default: 1.0 "
                       "minute.")
base.add_argument('--num_shards', type=int, default=5,
                  help='Number of shards to use when writing windowed elements to cloud storage. Only used with '
                       'the `topic` flag. Default: 5 shards.')
base.add_argument('-d', '--dry-run', action='store_true', default=False,
                  help='Preview the load into BigQuery. Default: off')


class LoaderPipelineOptions(PipelineOptions):


  @classmethod
  def _add_argparse_args(cls, parser):
    parser.prog = 'weather-mv'
    parser.description='Weather Mover loads weather data from cloud storage into analytics engines.'

    subparsers = parser.add_subparsers(help='help for subcommand', dest='subcommand')
    bq_parser = subparsers.add_parser('bigquery', aliases=['bq'], parents=[base],
                                      help='Move data into Google BigQuery')
    ToBigQuery.add_parser_arguments(bq_parser)

    # Regrid command registration
    rg_parser = subparsers.add_parser('regrid', aliases=['rg'], parents=[base],
                                      help='Copy and regrid grib data with MetView.')
    Regrid.add_parser_arguments(rg_parser)

    # EarthEngine command registration
    ee_parser = subparsers.add_parser('earthengine', aliases=['ee'], parents=[base],
                                      help='Move data into Google EarthEngine')
    ToEarthEngine.add_parser_arguments(ee_parser)
