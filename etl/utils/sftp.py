import os
import pysftp
from datetime import datetime
from configs import settings


def init_sftp(sftp: pysftp.Connection):
    dirs = sftp.listdir()
    if "output" not in dirs:
        sftp.mkdir("output")
    if "outputfinished" not in dirs:
        sftp.mkdir("outputfinished")
    if "outputerror" not in dirs:
        sftp.mkdir("outputerror")


def send_outputs_to_sftp(source: str):
    cnopts = pysftp.CnOpts()
    cnopts.hostkeys = None
    with pysftp.Connection(cnopts=cnopts, **settings.SFTP_CONFIGS) as sftp:
        init_sftp(sftp=sftp)

        output_directory = os.path.join(settings.BASE_DIR, source, "output")
        output_files = [
            filename
            for filename in os.listdir(output_directory)
            if filename.endswith(".csv")
        ]
        for filename in output_files:
            sftp.put(os.path.join(output_directory, filename),
                     remotepath=f"./output/{int(datetime.now().timestamp())}_{filename}")


def send_file_to_sftp(path: str, filename: str):
    cnopts = pysftp.CnOpts()
    cnopts.hostkeys = None
    with pysftp.Connection(cnopts=cnopts, **settings.SFTP_CONFIGS) as sftp:
        init_sftp(sftp=sftp)
        sftp.put(path, remotepath=f"./output/{int(datetime.now().timestamp())}_{filename}")
