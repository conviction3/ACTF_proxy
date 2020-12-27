ACTF_proxy for Aggregate collective TCP flows proxy 

## Develop Environment
1. Develop Operation System: win10
1. Develop IDE: Pycharm 2020.1
1. Develop Language: Python3.6

## Running Environment Configuration
```shell script
make init
````

## Milestone
### 2. 2020-12-24
- Description: Distributed addition with two clients one server and one proxy
- Commit: `9e615e18`
- Run: 
```shell script
# Firstly start server. Goto the directory of server code, then run:
make run
# Secondly start proxy. Goto the directory of proxy code, then run:
make run
# Lastly start clients. Goto the directory of client code, then run:
make run
````
