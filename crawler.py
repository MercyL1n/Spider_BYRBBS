# coding=utf-8
import re
import numpy as np
import pandas as pd
from time import sleep
from datetime import datetime
from byr_util import INSTANCE, with_login, with_log, with_heartbeat
from file_set_db import scan_database, insert_batch
from html import load_html_template, save_result, load_html_head, save_comment_result
import requests
import json
import time
access_token = '24.f100b826c1bac64bc3b82feb01aa7e38.2592000.1592751820.282335-20016262'
requests.packages.urllib3.disable_warnings()


@with_login
def get_job_list(session, page_start=1, page_end=1, step_time=3, day_last=8):
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
    comment_list = []
    for link in job_df["job_link"]:
        comment_list.extend(get_comments(link))
    comment_df = pd.DataFrame(comment_list, columns=["comment", "sentiment", "positive_prob"], dtype="str")
    save_comment_list(comment_df)
    job_df.to_csv("jobs.csv", encoding="utf-8", index=False)

    return job_df


@with_login
def get_comments(session, url, page_start=1, page_end=1, step_time=3):
    def parse_one_page_comments(page_idx):
        # get and parse one page comments
        params = {"_uid": INSTANCE["username"], "p": page_idx}
        html = session.get(url, params=params).text
        html = html.replace("&amp;", "")
        pattern = re.compile(r'发信站.*?\<br \/\>.*?\<br \/\>(.*?)--',
                             re.UNICODE)
        comments = re.findall(pattern, str(html))
        comments = [comment.replace("<br />", "").replace("</b>", "").replace("<b>", "") for comment in comments]
        return comments

    comment_list = []
    for page_i in range(page_start, page_end + 1):
        trylimit = 2
        while 1:
            try:
                comments = with_log(parse_one_page_comments)(page_i)
                emotion_analysis(comments)
                comment_list.extend(comments)
                sleep(step_time)
                break
            except:
                print("we should wait")
                sleep(1)
                trylimit -= 1
                if not trylimit:
                    print("unable to fetch this page %s" % page_i)
                    break
    return comment_list


def emotion_analysis(comments):
    labels = []
    label_prediction = []
    url = 'https://aip.baidubce.com/rpc/2.0/nlp/v1/sentiment_classify?access_token=' + access_token
    for i in range(len(comments)):
        if (i + 1) % 5 == 0:
            time.sleep(1)
        params = {'text': comments[i]}
        encoded_data = json.dumps(params).encode('GBK')
        try:
            request = requests.post(url, data=encoded_data, headers={'Content-Type': 'application/json'}, verify=False)
        except Exception as e:
            print(e)
        a = json.loads(request.text)
        a1 = a['items'][0]
        comments[i] = (comments[i], a1['sentiment'], a1['positive_prob'])


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


def save_comment_list(df):
    # df :comment, sentiment, positive_prob
    html = load_html_template()
    head = load_html_head()
    row_head = "<tr><th>{0}</th><th>{1}</th><th>{2}</th><th>{3}</th></tr>"
    row = "<tr><td>{0}</td><td>{1}</td><td>{2}</td><td>{3}</td></tr>"
    trs = [row_head.format("#", "评论", "情绪等级", "积极性")]
    for i, line in enumerate(df.itertuples()):
        tmp_row = row.format(str(i), line[1], line[2], line[3])
        trs.append(tmp_row)
    inner_trs = " ".join(trs)
    html = html.format(inner_trs)
    save_comment_result(head + html)


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
