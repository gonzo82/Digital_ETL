from utils import credentials as cred
from slack_webhook import Slack
# URL del paquete de python https://pypi.org/project/slack-webhook/
from datetime import datetime
import sys
import traceback2 as traceback


def format_message(app, text):
    return '{app}: {hora} --> {text}'.format(
        app=app,
        hora=str(datetime.now().strftime("%Y-%m-%d %H:%M:%S")),
        text=text
    )


def slack_message(app, text):
    slack = Slack(url=cred.SLACK_CHANNEL_LOGS)
    message = format_message(app, text)
    slack.post(text=message)
    print(message)


def print_message(app, text):
    message = format_message(app, text)
    print(message)


def error_log(func):
    def inner_function(*args, **kwargs):
        try:
            func(*args, **kwargs)
        except:
            errors = traceback.format_exception(*sys.exc_info())
            pre_salida = ['          {linea}'.format(linea=e.strip().replace('\n', '')) for e in errors]
            salida = '\r\n'.join(pre_salida[2:6])
            slack_message(
                app='ERROR in {function}'.format(function=func.__name__),
                text='\r\n{text}'.format(text=salida)
            )
    return inner_function
