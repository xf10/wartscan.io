import os

import psycopg2
from flask import Flask, render_template, request
from explorer import Explorer
import endpoints_charts
import endpoints_stats
import endpoints_apiv1
import endpoints_base
from flask_limiter import Limiter
from flask_turnstile import Turnstile

s = Explorer()
app = Flask(__name__)

app.config["JSONIFY_PRETTYPRINT_REGULAR"] = True
app.config["SECRET_KEY"] = os.environ.get("SECRET_KEY")
app.config["TURNSTILE_SITE_KEY"] = os.environ.get("TURNSTILE_SITE_KEY")
app.config["TURNSTILE_SECRET_KEY"] = os.environ.get("TURNSTILE_SECRET_KEY")

turnstile = Turnstile(app=app)

# blueprints
app.register_blueprint(endpoints_apiv1.construct_blueprint(s), url_prefix="/api")
app.register_blueprint(endpoints_base.construct_blueprint(s, turnstile))
app.register_blueprint(endpoints_stats.construct_blueprint(s, turnstile))
app.register_blueprint(endpoints_charts.construct_blueprint(s, turnstile))


def get_client_ip():
    return request.headers.get("CF-Connecting-IP")


limiter = Limiter(
    app,
    key_func=get_client_ip,
    default_limits=["3600 per hour", "60 per minute", "5 per second"],
    storage_uri="memcached://memcached:11211"
)


@app.route("/dbg/status")
def dbg_status():
    return "ok"


# Error Pages


@app.errorhandler(400)
def page_not_found(e):
    return render_template("explorer.html", c1="Error 400 Bad Request."), 400


@app.errorhandler(404)
def page_not_found(e):
    return render_template("explorer.html", c1="Error 404 Page Not Found."), 404


@app.errorhandler(403)
def page_forbidden(e):
    return render_template("explorer.html", c1="Error 403 Forbidden."), 403


@app.errorhandler(405)
def page_forbidden(e):
    return render_template("explorer.html", c1="Error 405 Method Not Allowed."), 405


@app.errorhandler(500)
def server_error(e):
    return render_template("explorer.html", c1="Error 500 Internal Server Error."), 500


if __name__ == '__main__':
    app.run(port=50050)
