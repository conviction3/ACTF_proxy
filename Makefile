init:
	pip install virtualenv==20.0.35 -i https://mirrors.aliyun.com/pypi/simple/
	virtualenv venv

run:
	.\venv\Scripts\python main.py

unittest:
	.\venv\Scripts\python -m unittest test/test_utils.py

