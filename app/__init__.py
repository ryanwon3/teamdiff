import os

from flask import Flask

from app.config import Config
from app.routes import bp as main_bp


def create_app() -> Flask:
    root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
    app = Flask(
        __name__,
        template_folder=os.path.join(root, "templates"),
        static_folder=os.path.join(root, "static"),
    )
    app.config.from_object(Config)
    app.register_blueprint(main_bp)
    return app
