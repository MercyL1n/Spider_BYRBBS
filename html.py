# coding=utf-8
import pandas as pd
import configparser


configure = "byr.conf"
CONF = configparser.ConfigParser()
CONF.read(configure)


def load_html_template(tempfile=CONF.get("filepath", "joblist_template_html")):
    with open(tempfile, "rt") as f:
        lines = f.readlines()
        html = " ".join([line.strip() for line in lines])  # must be space; in case of \n
    return html


def load_html_head(tempfile=CONF.get("filepath", "joblist_template_head")):
    with open(tempfile, "rt") as f:
        lines = f.readlines()
        head = " ".join([line.strip() for line in lines])  # must be space; in case of \n
    return head


def save_comment_result(html, file=CONF.get("filepath", "comment_result_html")):
    with open(file, "w") as f:
        f.write(html)
        f.close()


def save_result(html, file=CONF.get("filepath", "joblist_result_html")):
    with open(file, "w") as f:
        f.write(html)
        f.close()




