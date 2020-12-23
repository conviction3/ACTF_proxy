ACTF_proxy for Aggregate collective TCP flows proxy 

## 环境说明
1. 开发系统：win10
1. 开发工具：Pycharm 2020.1
1. 开发语言：Python3.6

## 环境配置
```shell script
make init
```

## milestone
### 2. 2020-12-24
- 描述：完成一个服务端、两个客户端、一个代理端的分布式加法计算
- run: 
```shell script
# 先启动服务端，进入服务端目录下，执行:
make run
# 再启动代理端，进入代理端目录下，执行:
make run
# 最后启动客户端，进入客户端目录下，执行:
make run
```
