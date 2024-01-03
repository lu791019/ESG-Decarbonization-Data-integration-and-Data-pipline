"""WSGI config for decarb_etl project."""

from app import create_app
from app.blueprint import register

app = create_app()
register(app)


@app.route('/')
def hello():
    "root page"
    return "decarb-etl"


if __name__ == '__main__':
    app.run()
