"""
Exercise flask application
"""

import os

from flask import Flask
from flask_cors import CORS

from config import Config
from eagle import handlers
from extensions import db


def create_app(config_class=Config):
    """
    create and configure flask application
    """
    app = Flask(__name__, instance_relative_config=True)
    app.config.from_object(config_class)
    app.register_blueprint(handlers.bp)
    CORS(app)
    db.init_app(app)

    if not os.path.isdir(app.config["UPLOAD_FOLDER"]):
        os.mkdir(app.config["UPLOAD_FOLDER"])
    return app
