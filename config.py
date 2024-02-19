import os

basedir = os.path.abspath(os.path.dirname(__file__))


class Config(object):
    DEBUG = False
    TESTING = False
    CSRF_ENABLED = True
    SECRET_KEY = "b48d8dc91c7444c58a5813743690902e"
    SQLALCHEMY_DATABASE_URI = os.environ["DATABASE_URL"]
    UPLOAD_FOLDER = "upload_dsp/"


class ProductionConfig(Config):
    DEBUG = False


class StaginConfig(Config):
    DEVELOPMENT = True
    DEBUG = True


class DevelopmentConfig(Config):
    DEVELOPMENT = True
    DEBUG = True


class TestingConfig(Config):
    TESTING = True
