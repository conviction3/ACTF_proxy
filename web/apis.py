from flask import Flask, jsonify
import core
import datetime


class Api:
    def __init__(self, app):
        @app.route('/')
        def hello_world():
            return 'Hello World!'

        @app.route('/x')
        def get_bytes_count():
            x = datetime.datetime.now().strftime("%H:%M:%S")
            y = str(core.get_bytes_count())

            return jsonify({
                "code": 20000,
                "data": {
                    'x': x,
                    'y': y
                }
            })
