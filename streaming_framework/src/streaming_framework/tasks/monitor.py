"""monitor.py — Bundle task: print the monitoring dashboard to the job log."""

from streaming_framework.tasks.common import base_parser, get_spark


def main(argv=None):
    from streaming_framework.utils.monitor import PipelineMonitor

    args    = base_parser("Pipeline monitoring").parse_args(argv)
    monitor = PipelineMonitor(get_spark(), args.catalog, args.schema)
    monitor.run_all(render=lambda df: df.show(50, truncate=False))


if __name__ == "__main__":
    main()
