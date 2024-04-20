# -*- encoding: utf-8 -*-
from flask import Flask

from dashboard import dummies
from dashboard.models import db


def create_app(config_class="dashboard.config.Config"):
    app = Flask(__name__)
    app.config.from_object(config_class)

    if app.debug:
        import logging

        file_handler = logging.FileHandler("omnivore_dashboard.log")
        app.logger.addHandler(file_handler)

    db.init_app(app)

    with app.app_context():
        db.create_all()

    @app.cli.command("dummies")
    def add_dummy_data():
        for dummy in dummies.generate_dummies():
            db.session.add(dummy)
        db.session.commit()

    from dashboard.views import init_routes

    init_routes(app)

    return app
