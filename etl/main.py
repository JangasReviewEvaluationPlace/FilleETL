import logging
import argparse

from configs import SOURCES, settings
from utils.sftp import send_outputs_to_sftp

logger = logging.getLogger()
logger.setLevel(logging.getLevelName(settings.LOG_LEVEL))


def parse_cmd_args() -> dict:
    parser = argparse.ArgumentParser(
        description="Define configuration for etl run mode."
    )
    parser.add_argument(
        "mode",
        help=("Mode of ETL: run || dummy "
              "Take a look into documentation for more info."),
    )
    parser.add_argument(
        "--types",
        help=("Commaseparated List of sources which should be included "
              "in etl process. If empty all known sources will be "
              "included.")
    )
    parser.add_argument(
        "--allowed-threads",
        type=int,
        help="Number of allowed threads for execution. Default Value: 1",
        default=1
    )
    parser.add_argument(
        "--chunk-size",
        required=False,
        type=int,
        help=(
            "Optional Chunksize. "
            "If setted the datasource will be splitted into chunks with given size."
        )
    )

    return vars(parser.parse_args())


def dynamic_etl_import(source):
    mod = __import__(source)
    return getattr(mod, 'ETL')


def main():
    cmd_args = parse_cmd_args()
    if not cmd_args["types"]:
        for source in SOURCES:
            ETL = dynamic_etl_import(source=source)
            etl = ETL(allowed_threads=cmd_args["allowed_threads"],
                      chunk_size=cmd_args["chunk_size"],
                      is_dummy=cmd_args["mode"] != "run")
            etl.run()
            send_outputs_to_sftp(source=source)
    else:
        for source in cmd_args["types"].split(","):
            try:
                ETL = dynamic_etl_import(source=source)
            except ModuleNotFoundError:
                logging.error(f"No Source with {source} exists.")
                continue
            if ETL:
                etl = ETL(allowed_threads=cmd_args["allowed_threads"],
                          chunk_size=cmd_args["chunk_size"],
                          is_dummy=cmd_args["mode"] != "run")
                etl.run()
                send_outputs_to_sftp(source=source)


if __name__ == "__main__":
    main()
