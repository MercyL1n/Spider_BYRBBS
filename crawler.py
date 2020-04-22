# coding=utf-8
import re
import numpy as np
import pandas as pd
from time import sleep
from datetime import datetime
from byr_util import INSTANCE, with_login, with_log, with_heartbeat
from file_set_db import scan_database, insert_batch
from html import load_html_template, save_result, load_html_head


@with_login
def get_job_list(session, page_start=1, page_end=3, step_time=3, day_last=8):
    """
    :return: jobs info dataframe: columns("job_id","job_type","company","job_name","start_time")
    """

    def parse_one_page(page_idx):
        # get and parse one job list page
        target_url = r"https://bbs.byr.cn/board/ParttimeJob/"
        params = {"_uid": INSTANCE["username"], "p": page_idx}
        html = session.get(target_url, params=params).text
        html = html.replace("&amp;", "")
        pattern = re.compile(r'href\=\"\/article\/ParttimeJob\/(\d+)\"\>【(.*?)】.*?【(.*?)】(.*?)\<\/a\>.*?'
                             r'\<\/td\>\<td.*?\>(\d+\-\d+\-\d+)\<\/td\>',
                             re.UNICODE)
        jobs = re.findall(pattern, str(html))
        return jobs

    job_list = []
    for page_i in range(page_start, page_end + 1):
        trylimit = 3
        while 1:
            try:
                jobs = with_log(parse_one_page)(page_i)
                job_list.extend(jobs)
                sleep(step_time)
                break
            except:
                print("we should wait")
                sleep(100)
                trylimit -= 1
                if not trylimit:
                    print("unable to fetch this page %s" % page_i)
                    break

    # 实现增量爬取
    history_jobs = scan_database()
    new_job_list = [item for item in job_list if item[0] not in history_jobs]
    if not new_job_list:
        return pd.DataFrame({})
    insert_batch([item[0] for item in new_job_list])

    job_df = pd.DataFrame(new_job_list,
                          columns=["job_id", "job_type", "company", "job_name", "start_time"],
                          dtype="str")
    job_df = job_df.sort_values(["start_time"], ascending=False)
    now = datetime.now()

    def filter_day(day):
        day2 = datetime.strptime(day, '%Y-%m-%d')
        last = abs(int((day2 - now).days))
        if last > day_last:
            return np.nan
        else:
            return day

    job_df["start_time"] = job_df["start_time"].apply(filter_day)
    job_df = job_df.dropna(axis=0)
    job_df["job_link"] = job_df["job_id"].apply(lambda id: "https://bbs.byr.cn/article/ParttimeJob/{0}".format(id))
    job_df.to_csv("jobs.csv", encoding="utf-8", index=False)

    return job_df


def save_job_list(df):
    # df :job_id,job_type,company,job_name,start_time,job_link
    html = load_html_template()
    head = load_html_head()
    row_head = "<tr><th>{0}</th><th>{1}</th> <th>{2}</th><th>{3}</th><th>{4}</th></tr>"
    row = "<tr><td>{0}</td><td>{1}</td> <td>{2}</td><td>{3}</td><td>{4}</td></tr>"
    trs = [row_head.format("#", "类型", "公司", "发布时间", "具体工作")]
    for i, line in enumerate(df.itertuples()):
        tmp_row = row.format(str(i), line[2], line[3], line[5], r'<a href="{0}">{1}<a>'.format(line[6], line[4]))
        trs.append(tmp_row)
    inner_trs = " ".join(trs)
    html = html.format(inner_trs)
    save_result(head + html)


@with_heartbeat
def run_batch():
    try:
        job_list = get_job_list()
        if job_list.shape[0] == 0:
            print(" no new jobs")
        else:
            print("%s new job" % job_list.shape[0])
            save_job_list(job_list)
    except:
        print("[ERROR] running batch", datetime.now().__str__())
        pass


if __name__ == '__main__':
    run_batch()
