import pandas as pd
import os
import paramiko


class Sftp:
    CSV_DELIMITER = '\t'

    def __init__(self, host, username, password):
        self.ssh_client = paramiko.SSHClient()
        self.ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        self.ssh_client.connect(hostname=host, username=username, password=password)
        self.sftp = self.ssh_client.open_sftp()

    def close(self):
        self.sftp.close()
        self.ssh_client.close()

    def send_file(self, file_name, directory):
        file_route = '{directory}/{file_name}'.format(directory=directory, file_name=file_name)
        response = self.sftp.put(localpath=file_name, remotepath=file_route)
        return 1

    def send_file_df(self, df: pd.DataFrame, file_name, directory, delimiter=None):
        local_file_name = os.path.join(file_name)
        if not delimiter:
            delimiter = self.CSV_DELIMITER
        df.to_csv(local_file_name, index=False, sep=delimiter, encoding='utf-8')

        file_route = '{directory}/{file_name}'.format(directory=directory, file_name=file_name)
        response = self.sftp.put(localpath=file_name, remotepath=file_route)
        os.remove(local_file_name)
        return 1
