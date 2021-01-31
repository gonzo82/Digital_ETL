import ftplib
import pandas as pd
import os
from utils import credentials as cred


class Ftp:
    CSV_DELIMITER = '\t'

    def __init__(self):
        self.sftp = ftplib.FTP_TLS(cred.FOXTER_FTP_IP)
        self.sftp.login(cred.FOXTER_FTP_USER, cred.FOXTER_FTP_PASSWORD)
        self.session = ftplib.FTP(cred.FOXTER_FTP_IP, cred.FOXTER_FTP_USER, cred.FOXTER_FTP_PASSWORD)

    def send_file(self, file_name):
        file = open(file_name)
        with open(file_name, "rb") as f:
            self.ftp.storbinary('STOR ' + os.path.basename(file_name), f)
        file.close()

    def send_file_df(self, df: pd.DataFrame, file_name):
        """"""
        # save csv to local file

        local_file_name = os.path.join(file_name)
        df.to_csv(local_file_name, index=False, sep=self.CSV_DELIMITER, encoding='utf-8')

        # upload the file to ftp
        # ftp = ftplib.FTP(host=cred.FOXTER_FTP_IP)
        # , user=cred.FOXTER_FTP_USER, passwd=cred.FOXTER_FTP_PASSWORD)
        ftp = ftplib.FTP()
        ftp.connect(host=cred.FOXTER_FTP_IP, port=cred.FOXTER_FTP_PORT)
        #, timeout=7000)
        ftp.login(user=cred.FOXTER_FTP_USER, passwd=cred.FOXTER_FTP_PASSWORD)
        ftp.set_pasv(True)
        ftp.cwd('ftp_dir')
        response = ftp.storbinary(cmd='APPE {file_name}'.format(file_name=file_name), fp=open(local_file_name, 'rb'))
        ftp.quit()
        os.remove(local_file_name)
        return 1

    def send_file_2(self):
        ftp = ftplib.FTP()
        ftp.connect(host=cred.FOXTER_FTP_IP, port=cred.FOXTER_FTP_PORT)
        ftp.login(user=cred.FOXTER_FTP_USER, passwd=cred.FOXTER_FTP_PASSWORD)
        ftp.set_pasv(True)
        ftp.cwd('ftp_dir')
        hola = ftp.getwelcome()
        print(hola)
        file_name = 'users_data.csv'
        file_open = open(file_name, 'rb')
        print('Pre send')
        command = 'STOR {file_name}'.format(file_name=file_name)
        response = ftp.storbinary(cmd=command, fp=file_open)
        print('Sended')
        ftp.quit()
        return 1

if __name__ == '__main__':
    ftp = Ftp()
    ftp.send_file_2()