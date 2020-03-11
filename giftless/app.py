"""Main Flask application initialization code
"""
from flask import Flask
from flask_marshmallow import Marshmallow  # type: ignore

from giftless import config, transfer, view

from .authentication import authentication
from .error_handling import ApiErrorHandler
from .jwt import JWT


def init_app(app=None, additional_config=None):
    """Flask app initialization
    """
    if app is None:
        app = Flask(__name__)

    config.configure(app, additional_config=additional_config)

    ApiErrorHandler(app)
    Marshmallow(app)

    authentication.init_app(app)
    JWT(app)

    view.BatchView.register(app)

    # Load configured transfer adapters
    transfer.init_flask_app(app)

    return app
