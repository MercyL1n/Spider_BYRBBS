# coding=utf-8
# python 3.7
from crawler import run_batch as run_batch_job
from file_set_db import empty_database
import sys

if __name__ == '__main__':
    if len(sys.argv) > 1:
        if sys.argv[1] == "empty":
            empty_database()
        else:
            print("wrong argv")
    empty_database()
    run_batch_job()
