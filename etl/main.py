import logging
import argparse

from configs import SOURCES, settings

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

    return vars(parser.parse_args())


def main():
    cmd_args = parse_cmd_args()
    if not cmd_args["types"]:
        for _, ETL in SOURCES.items():
            etl = ETL()
            etl.run(is_dummy=cmd_args["mode"] != "run")
    else:
        for etl_type in cmd_args["types"].split(","):
            ETL = SOURCES.get(etl_type)
            if ETL:
                etl = ETL()
                etl.run(is_dummy=cmd_args["mode"] != "run")


if __name__ == "__main__":
    main()
