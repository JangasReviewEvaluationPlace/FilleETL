import os
import pysftp
from configs import settings


def send_outputs_to_sftp(source: str):
    cnopts = pysftp.CnOpts()
    cnopts.hostkeys = None
    with pysftp.Connection(cnopts=cnopts, **settings.SFTP_CONFIGS) as sftp:
        dirs = sftp.listdir()
        if "output" not in dirs:
            sftp.mkdir("output")
        with sftp.cd("output"):
            output_directory = os.path.join(settings.BASE_DIR, source, "output")
            sftp.put_d(output_directory, remotepath="./")
