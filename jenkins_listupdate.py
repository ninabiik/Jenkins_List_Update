# -*- coding: utf-8 -*-

import re
import datetime
import pytz
import pandas as pd
import numpy as np
import logging
import luigi
import csv
from jenkinsapi.jenkins import Jenkins
from luigi.s3 import S3Client
from conf_jenkins_listupdate import Const


today = datetime.datetime.now(pytz.utc).astimezone(pytz.timezone(Const.TIMEZONE))
today_formatted = today.strftime('%Y%m%d')
logger = logging.getLogger('analytics-pf')
client = S3Client()

def get_server_instance():
    server = Jenkins(Const.JENKINS_URL, username=Const.JENKINS_USER, password=Const.JENKINS_API_TOKEN)
    return server


def extract_job_name(pattern, string):
    m = re.search(pattern, string)
    if m:
        extract = m.group(1)
        return extract


def get_job_details():
    server = get_server_instance()
    d = []
    for job_name, job_instance in server.get_jobs():

        # Extract job name
        job_name = job_instance.name
        job_name_extract = extract_job_name(Const.JOB_PREFIX + '(.+?$)', job_name)
        d.append(job_name_extract)

        # Job description
        job_desc = job_instance.get_description().replace('\r\n', ' ')
        d.append(job_desc)

        # Extract environment
        env_extract = extract_job_name('(.+?)' + Const.JOB_PREFIX, job_name)
        d.append(env_extract)

        # Is job enabled
        enable = job_instance.is_enabled()
        if enable:
            enable = 'Enabled'
        else:
            enable = 'Disabled'
        d.append(enable)

        # Get timezone
        up = ''.join(job_instance.get_upstream_job_names())
        if up:
            up = extract_job_name(Const.JOB_PREFIX + '(.+?$)', up)
            d.append('N/A')
        else:
            conf = job_instance.get_config()
            m = re.search('<spec>(.+?)\n(.+?)</spec>', conf)
            if m:
                timezone = m.group(1)
                d.append(str(timezone))
            else:
                up = extract_job_name(Const.JOB_PREFIX + '(.+?$)', up)
                d.append('N/A')

        # Get build trigger
        up = ''.join(job_instance.get_upstream_job_names())
        if up:
            up = extract_job_name(Const.JOB_PREFIX + '(.+?$)', up)
            d.append(up)
        else:
            conf = job_instance.get_config()
            m = re.search('<spec>(.+?)\n(.+?)</spec>', conf)
            if m:
                time = m.group(2)
                d.append(str(time))
            else:
                up = extract_job_name(Const.JOB_PREFIX + '(.+?$)', up)
                d.append(up)

        # Get downstream job
        down = job_instance.get_downstream_job_names()
        down_list = []
        for job in down:
            job_2 = ''.join(job)
            down_list.append(extract_job_name(Const.JOB_PREFIX + '(.+?$)', job_2))
        down_str = ', '.join(down_list)
        d.append(down_str)

        # Get URL
        d.append(''.join(job_instance.url))

    cols = len(Const.CSV_HEADERS)
    print len(d)
    print len(d)/cols
    df = pd.DataFrame(np.array(d).reshape(len(d)/cols, cols))

    return df


class GetJenkinsJobList(luigi.Task):
    if_date = luigi.Parameter(default=today_formatted)

    def output(self):
        return luigi.s3.S3Target('{s3_location}/{file_prefix}_{if_date}.csv'.format(
            s3_location=Const.MDWH_S3_LOCATION,
            file_prefix=Const.CSV_FILENAME,
            if_date=self.if_date
        ), client=client)

    def run(self):
        df_jobs = get_job_details()

        with self.output().open('w') as output:
            df_jobs.to_csv(output, header=Const.CSV_HEADERS, index=False, quoting=csv.QUOTE_ALL, escapechar=Const.ESCAPECHAR)


if __name__ == '__main__':
    luigi.run() 