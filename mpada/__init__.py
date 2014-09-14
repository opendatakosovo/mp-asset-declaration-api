import os
import ConfigParser
import logging
import logging.config

from logging.handlers import RotatingFileHandler

from flask import Flask
from flask.ext.pymongo import PyMongo
from utils.utils import Utils


# Create MongoDB database object.
mongo = PyMongo()

# Create the utils object
utils = Utils()


def create_app():
    ''' Create the Flask app.
    '''
    # Create the Flask app.
    app = Flask(__name__)

    # Load application configurations
    load_config(app)

    # Configure logging.
    configure_logging(app)

    # Register URL rules.
    register_url_rules(app)

    # Init app for use with this PyMongo
    # http://flask-pymongo.readthedocs.org/en/latest/#flask_pymongo.PyMongo.init_app
    mongo.init_app(app, config_prefix='MONGO')

    return app


def load_config(app):
    ''' Reads the config file and loads configuration properties into the Flask app.
    :param app: The Flask app object.
    '''

    # Get the path to the application directory, that's where the config file resides.
    par_dir = os.path.join(__file__, os.pardir)
    par_dir_abs_path = os.path.abspath(par_dir)
    app_dir = os.path.dirname(par_dir_abs_path)

    # Read config file
    # FIXME: Use the "common pattern" described in "Configuring from Files": http://flask.pocoo.org/docs/config/
    config = ConfigParser.RawConfigParser()
    config_filepath = app_dir + '/config.cfg'
    config.read(config_filepath)

    # Set up config properties
    app.config['SERVER_PORT'] = config.get('Application', 'SERVER_PORT')
    app.config['BASE_PATH'] = config.get('Application', 'BASE_PATH')

    app.config['MONGO_DBNAME'] = config.get('Mongo', 'DB_NAME')

    # Logging path might be relative or starts from the root.
    # If it's relative then be sure to prepend the path with the application's root directory path.
    log_path = config.get('Logging', 'PATH')
    if log_path.startswith('/'):
        app.config['LOG_PATH'] = log_path
    else:
        app.config['LOG_PATH'] = app_dir + '/' + log_path

    app.config['LOG_LEVEL'] = config.get('Logging', 'LEVEL').upper()


def configure_logging(app):

    # Get the path of the log from the config
    log_path = app.config['LOG_PATH']

    # Get the level of logging from the config
    log_level = app.config['LOG_LEVEL']

    # If path directory doesn't exist, create it.
    log_dir = os.path.dirname(log_path)
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)

    # Create formatter
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )

    # Create Log_Handler
    log_handler = RotatingFileHandler(log_path, maxBytes=250000, backupCount=5)

    # add formatter to log handler
    log_handler.setFormatter(formatter)

    # Get the level of the Debug and set it to the logger
    app.logger.setLevel(log_level)

    # Add the handlers to the logger
    app.logger.addHandler(log_handler)

    # Test if the logging is working by typing this string to a file.
    app.logger.info('Logging to: %s', log_path)


# Index page view.
from views.index import Index

# Party views.
from views.party.all import Party
from views.party.year import PartyYear

# Aggregate Views
from views.aggregate.sum.party import PartySumAggregate
from views.aggregate.sum.mp import MPSumAggregate
from views.aggregate.sum.all import AllSumAggregate

# Aggregate Views to get declaring parties, their MPs, and their declaration years.
from views.aggregate.yearspartiesdeclared import YearsPartiesDeclaredAggregate
from views.aggregate.yearsmpsdeclared import YearsMPsDeclaredAggregate
from views.aggregate.mpsgroupedbyparties import DeclararedPartyMPsAggregate

# Aggregate Views for Median
from views.aggregate.median.all import AllMedian
from views.aggregate.median.year import YearMedian
from views.aggregate.median.party import PartyMedian
from views.aggregate.median.partyyear import PartyYearMedian

# MP views.
from views.mp.all import MP
from views.mp.year import MPYear


def register_url_rules(app):
    ''' Register the URL rules.
    Use pluggable class-based views: http://flask.pocoo.org/docs/views/
    :param app: the Flask application instance.
    '''

    # Show instructional index page.
    app.add_url_rule('/', view_func=Index.as_view('index'))

    register_aggregate_declaration_years_url_rules(app)

    app.add_url_rule(
        '/aggregate/mps-grouped-by-parties',
        view_func=DeclararedPartyMPsAggregate.as_view('mps_grouped_by_parties'))

    # Register the URL rules.
    register_aggregate_median_url_rules(app)
    register_aggregate_sum_url_rules(app)
    register_party_url_rules(app)
    register_mp_url_rules(app)


def register_aggregate_declaration_years_url_rules(app):
    app.add_url_rule(
        '/aggregate/years-mps-declared/<string:party_slug>',
        view_func=YearsMPsDeclaredAggregate.as_view('years-mps-declared'))

    app.add_url_rule(
        '/aggregate/years-mps-declared/<string:party_slug>/<string:mp_slug>',
        view_func=YearsMPsDeclaredAggregate.as_view('years-mp-declared'))

    app.add_url_rule(
        '/aggregate/years-parties-declared',
        view_func=YearsPartiesDeclaredAggregate.as_view('years-parties-declared'))


def register_aggregate_median_url_rules(app):
    '''URL rules to get asset declaration medians.
    :param app: the application instance
    '''
    app.add_url_rule(
        '/aggregate/median',
        view_func=AllMedian.as_view('medians'))

    app.add_url_rule(
        '/aggregate/median/<int:year>',
        view_func=YearMedian.as_view('medians_year'))

    app.add_url_rule(
        '/aggregate/median/<string:party_slug>',
        view_func=PartyMedian.as_view('medians_party'))

    app.add_url_rule(
        '/aggregate/median/<int:year>/<string:party_slug>',
        view_func=PartyYearMedian.as_view('medians_party_year'))


def register_aggregate_sum_url_rules(app):
    '''URL rules to get asset aggregate declaration sums.
    :param app: the application instance
    '''
    app.add_url_rule(
        '/aggregate/sum',
        view_func=AllSumAggregate.as_view('sum_aggregate'))

    app.add_url_rule(
        '/aggregate/sum/<string:party_slug>',
        view_func=PartySumAggregate.as_view('party_aggregate'))

    app.add_url_rule(
        '/aggregate/sum/<string:party_slug>/<string:mp_slug>',
        view_func=MPSumAggregate.as_view('mp_aggregate'))


def register_party_url_rules(app):
    '''URL rules to get asset declarations of a all MPs of a given Party.
    :param app: the application instance
    '''
    app.add_url_rule(
        '/<string:party_slug>',
        view_func=Party.as_view('party'))

    app.add_url_rule(
        '/<int:year>/<string:party_slug>',
        view_func=PartyYear.as_view('party_year'))


def register_mp_url_rules(app):
    '''URL rules to get asset declarations of a given MP.
    :param app: the application instance
    '''
    app.add_url_rule(
        '/<string:party_slug>/<string:mp_slug>',
        view_func=MP.as_view('mp'))

    app.add_url_rule(
        '/<int:year>/<string:party_slug>/<string:mp_slug>',
        view_func=MPYear.as_view('mp_year'))
