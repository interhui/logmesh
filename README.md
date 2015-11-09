#logmesh#

Logmesh是一个开源消息采集/过滤/路由应用

- 支持多种类型的消息采集方式, 包括TCP/UDP/JMS和自定义方式
- 支持级联消息过滤器, 包括IP过滤、关键字过滤、正则表达式过滤、消息分类和消息编码
- 支持根据IP地址和消息内容进行消息路由
- 提供消息映射和Syslog解析工具
- 提供正则规则处理和表达式规则处理
- 提供多种消息存储，包括数据库、文本文件和Solr消息存储
- 提供消息转发、消息统计和原始记录存储
- 提供GUI界面和Web界面

## 安装与运行 ##

最新的更新时间 (2015/11/09)

更新版本和下载: logmesh-1.0.tar.gz (更新内容)

安装与运行:
	
	wget https://github.com/interhui/logmesh/releases/logmesh-1.0.tar.gz
	tar -xvf logmesh-1.0.tar.gz
	cd logmesh-1.0
	./startup.sh server.xml

## 授权信息 ##

logmesh 版权采用  Apache License, Version 2.0 更详细的版权信息参考LICENSE
