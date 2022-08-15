# Copyright 2021 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""Pipeline for loading weather data into analysis-ready mediums, like Google BigQuery."""

# pylint: disable=bad-indentation
import argparse
import logging
import os
import time
import typing as t

import apache_beam as beam
from apache_beam.io.filesystems import FileSystems
from apache_beam.pipeline import PipelineOptions
from apache_beam.options.pipeline_options import WorkerOptions
from apache_beam.options.pipeline_options import StandardOptions
from .bq import ToBigQuery
from .ee import ToEarthEngine
from .regrid import Regrid
from .streaming import GroupMessagesByFixedWindows, ParsePaths
from .loader_pipeline_options import LoaderPipelineOptions


# NOTE(bahmandar): the below portion is cause I have a hard time seeing the logs on mac
# terminal when running. This will color each line (more) distinctly
try:
    import colored_traceback
    import colorlog
    from pygments.styles import get_style_by_name
    material_style = get_style_by_name('material')
    formatter = colorlog.ColoredFormatter(
        "%(log_color)s%(levelname)-8s%(reset)s %(message_log_color)s%(message)s",
        datefmt=None,
        reset=True,
        log_colors={
            'DEBUG':    'cyan',
            'INFO':     'green',
            'WARNING':  'yellow',
            'ERROR':    'red',
            'CRITICAL': 'red,bg_white',
        },
        secondary_log_colors={
            'message': {
                'DEBUG':    'cyan',
                'INFO':     'green',
                'WARNING':  'yellow',
                'ERROR':    'red',
                'CRITICAL': 'red,bg_white',
            }
        },
        style='%'
    )

    handler = colorlog.StreamHandler()
    handler.setFormatter(formatter)
    handler.setLevel(logging.INFO)
    logger = colorlog.getLogger(__name__)
    logger.addHandler(handler)
    logging.getLogger('apache_beam').addHandler(handler)
    colored_traceback.add_hook(always=True, style='material')
except ImportError:
    logger = logging.getLogger(__name__)


def configure_logger(verbosity: int) -> None:
    """Configures logging from verbosity. Default verbosity will show errors."""
    level = (40 - verbosity * 10)
    logging.getLogger(__package__).setLevel(level)
    logger.setLevel(level)


def pattern_to_uris(match_pattern: str) -> t.Iterable[str]:
    for match in FileSystems().match([match_pattern]):
        yield from [x.path for x in match.metadata_list]


def pipeline(known_args: LoaderPipelineOptions,
             pipeline_options: PipelineOptions) -> None:
    all_uris = list(pattern_to_uris(known_args.uris))
    if not all_uris:
        raise FileNotFoundError(
            f'File prefix "{known_args.uris}" matched no objects')
    known_args_dict = known_args.get_all_options()

    with beam.Pipeline(options=pipeline_options) as p:
        # Validate subcommand
        if known_args.subcommand == 'bigquery' or known_args.subcommand == 'bq':
            ToBigQuery.validate_arguments(known_args, pipeline_options)
        elif known_args.subcommand == 'earthengine' or known_args.subcommand == 'ee':
            ToEarthEngine.validate_arguments(known_args, pipeline_options)
        if known_args.topic:
            paths = (
                p
                # Windowing is based on this code sample:
                # https://cloud.google.com/pubsub/docs/pubsub-dataflow#code_sample
                | 'ReadUploadEvent' >> beam.io.ReadFromPubSub(known_args.topic)
                | 'WindowInto' >> GroupMessagesByFixedWindows(known_args.window_size, known_args.num_shards)
                | 'ParsePaths' >> beam.ParDo(ParsePaths(known_args.uris))
            )
        else:
            # NOTE(bahmandar): utilizing the pipeline to matchfiles and read matches ended
            # up being slow cause it was continually check sizes or something
            # paths = p | 'Create' >> beam.Create(known_args.uris)
            paths = (p
                     | 'Create' >> beam.Create(all_uris)
                     # | 'Match Files' >> fileio.MatchFiles(known_args.uris)
                     # | 'Read Matches' >> fileio.ReadMatches()
                     | 'Shuffle Paths' >> beam.Reshuffle()
                     )

            # NOTE(bahmandar): will use temp_gcs_location to store a temp zarr when the 
            # data is filtered
            p_options = pipeline_options.get_all_options()
            if temp_gcs_location := p_options.get('temp_location'):
                job_name = p_options.get('job_name', 'beam')
                temp_gcs_location = os.path.join(temp_gcs_location,
                                                 f'{job_name}.{time.time()}')
            logger.info(f'KNOWN ARGS: {known_args}')
            logger.info(f'subcommand: {known_args.subcommand}')
            if (known_args.subcommand == 'bigquery' or known_args.subcommand == 'bq'):
                (paths
                 | "MoveToBigQuery" >> ToBigQuery.from_kwargs(
                        temp_gcs_location=temp_gcs_location,
                        **known_args_dict)
                 )
            elif known_args.subcommand == 'regrid' or known_args.subcommand == 'rg':
                paths | "Regrid" >> Regrid.from_kwargs(**known_args_dict)
            elif known_args.subcommand == 'earthengine' or known_args.subcommand == 'ee':
                paths | "MoveToEarthEngine" >> ToEarthEngine.from_kwargs(**known_args_dict)
            else:
                raise ValueError('invalid subcommand!')

    logger.info('Pipeline is finished.')


def run(argv: t.List[str]) -> t.Tuple[LoaderPipelineOptions, PipelineOptions]:
    """Main entrypoint & pipeline definition."""
    # NOTE(bahmandar): this needs to be added for the flex template
    if argv[1].startswith('--'):
        argv.insert(1, 'bq')

    parser = argparse.ArgumentParser()
    _, pipeline_args = parser.parse_known_args(argv[1:])


    # initializing Pipeline object
    pipeline_options = PipelineOptions(
      pipeline_args
    )

    # NOTE(bahmandar): This is required for flex templates. The flex template
    # seems to have problems with arguments that cannot be explicitly set, i.e.
    # boolean arguements. So the work around is to set our own arguement for public
    # ips. Issue submitted https://github.com/apache/beam/issues/22727
    known_args = pipeline_options.view_as(LoaderPipelineOptions)
    pipeline_options.view_as(WorkerOptions).use_public_ips = known_args.public_ips

    # known_args = argparse.Namespace(**user_options)


    configure_logger(2)  # 0 = error, 1 = warn, 2 = info, 3 = debug



    # If a topic is used, then the pipeline must be a streaming pipeline.
    if known_args.topic:
        pipeline_options.view_as(StandardOptions).streaming = True
        # pipeline_args.extend('--streaming true'.split())

        # make sure we re-compute utcnow() every time rows are extracted from a file.
        known_args.import_time = None

    # We use the save_main_session option because one or more DoFn's in this
    # workflow rely on global context (e.g., a module imported at module level).

    # NOTE(bahmandar): removed this because it was needed when the modules were seperated
    # pipeline_args.extend('--save_main_session true'.split())

    return known_args, pipeline_options
