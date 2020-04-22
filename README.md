# 北邮人论坛爬虫

## 功能

* 增量爬取实习招聘板块近七天的工作信息

## 使用

```shell
初次使用：
在byr.conf中设置北邮人论坛账号密码，爬取周期默认是60s，修改heartbeat可修改爬取周期
python main.py
清空数据：
python main.py empty

爬虫结果在./html/result.html
```

## 运行截图

![run](.\screenshot\1.png)

![result](.\screenshot\2.png)