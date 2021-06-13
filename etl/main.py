import logging
import argparse
import nltk

from configs import SOURCES, settings

logger = logging.getLogger()
logger.setLevel(logging.getLevelName(settings.LOG_LEVEL))

nltk.download('punkt')


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
        "--sources",
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
    parser.add_argument(
        "--copy-to-sftp",
        required=False,
        type=bool,
        help="should output get copied to sftp server?"
    )

    return vars(parser.parse_args())


def dynamic_etl_import(source):
    mod = __import__(source)
    return getattr(mod, 'ETL')


def process_etl(source: str, cmd_args: dict):
    try:
        ETL = dynamic_etl_import(source=source)
    except ModuleNotFoundError:
        logging.error(f"No Source with {source} exists.")
        return
    etl = ETL(allowed_threads=cmd_args["allowed_threads"],
              chunk_size=cmd_args["chunk_size"],
              is_dummy=cmd_args["mode"] != "run",
              sftp_active=cmd_args["copy_to_sftp"])
    etl.run()


def main():
    cmd_args = parse_cmd_args()
    if not cmd_args["sources"]:
        for source in SOURCES:
            process_etl(source=source, cmd_args=cmd_args)
    else:
        for source in cmd_args["sources"].split(","):
            process_etl(source=source, cmd_args=cmd_args)


if __name__ == "__main__":
    main()
