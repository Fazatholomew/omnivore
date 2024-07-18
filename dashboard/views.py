# -*- encoding: utf-8 -*-

# Flask modules
from datetime import datetime
from flask import render_template, Flask, Response
from jinja2 import TemplateNotFound

from dashboard.models import Telemetry, HPC, Data
from dashboard import db

from sqlalchemy import inspect


def init_routes(app: Flask):
    # App main route + generic routing
    @app.route("/", defaults={"path": "index.html"})
    @app.route("/")
    def index():
        latest_entry = (
            db.session.query(Telemetry)
            .filter(Telemetry.end_date.is_not(None))
            .order_by(Telemetry.created_date.desc())
            .limit(2)
            .all()
        )

        last_run = (datetime.now() - latest_entry[0].created_date).total_seconds()
        compared_stats = [
            {
                "title": "Last Run",
                "value": (
                    "Today"
                    if last_run // 86400 == 0
                    else f"{int(abs(last_run // 86400))} days ago"
                ),
                "icon": "ni-spaceship",
            },
            {
                "title": "Total Runtime",
                "value": f'{(latest_entry[0].total_statistic["total_runtime"] / 3600):.2f} Minutes',
                "percent": (
                    (
                        (
                            latest_entry[0].total_statistic["total_runtime"]
                            - latest_entry[1].total_statistic["total_runtime"]
                        )
                        / latest_entry[0].total_statistic["total_runtime"]
                    )
                    if latest_entry[0].total_statistic["total_runtime"] > 0
                    else 0 * 100
                ),
                "icon": "ni-watch-time",
            },
            {
                "title": "Total Record Processed",
                "value": latest_entry[0].total_statistic["total_records"],
                "percent": (
                    (
                        (
                            latest_entry[0].total_statistic["total_records"]
                            - latest_entry[1].total_statistic["total_records"]
                        )
                        / latest_entry[0].total_statistic["total_records"]
                    )
                    if latest_entry[0].total_statistic["total_records"] > 0
                    else 0 * 100
                ),
                "icon": "ni-delivery-fast",
            },
        ]
        hpcs = [
            {
                **{
                    c.key: getattr(hpc, c.key) for c in inspect(hpc).mapper.column_attrs
                },
                "row_errors": hpc.row_errors,
            }
            for hpc in latest_entry[0].hpcs
        ]
        data_sources = [
            {
                c.key: (
                    getattr(current_data, c.key).strftime("%B %d, %Y")
                    if c.key == "created_date"
                    else getattr(current_data, c.key)
                )
                for c in inspect(current_data).mapper.column_attrs
            }
            for current_data in latest_entry[0].data
        ]
        try:
            return render_template(
                "pages/index.html",
                segment="Telemetry",
                parent="Omnivore",
                compared_stats=compared_stats,
                data=latest_entry[0],
                hpcs=hpcs,
                data_sources=data_sources,
            )
        except TemplateNotFound:
            return render_template("pages/index.html"), 404

    # Pages

    @app.route("/hpc/<string:hpc>/")
    def hpc(hpc: str):
        latest_entry = (
            db.session.query(HPC)
            .filter_by(name=hpc)
            .order_by(HPC.created_date.desc())
            .limit(2)
            .all()
        )

        last_run = (datetime.now() - latest_entry[0].created_date).total_seconds()

        compared_stats = [
            {
                "title": "Last Run",
                "value": (
                    "Today"
                    if last_run // 86400 == 0
                    else f"{int(abs(last_run // 86400))} days ago"
                ),
                "icon": "ni-spaceship",
            },
            {
                "title": "Latest Record",
                "value": latest_entry[0].latest_record,
                "icon": "ni-single-copy-04",
            },
            {
                "title": "Total Runtime",
                "value": f"{(latest_entry[0].runtime / 3600):.2f} Minutes",
                "percent": (
                    (latest_entry[0].runtime - latest_entry[1].runtime)
                    / latest_entry[0].runtime
                )
                * 100,
                "icon": "ni-watch-time",
            },
            {
                "title": "Total Record Processed",
                "value": latest_entry[0].output,
                "percent": (
                    (latest_entry[0].output - latest_entry[1].output)
                    / latest_entry[0].output
                )
                * 100,
                "icon": "ni-delivery-fast",
            },
        ]
        return render_template(
            "pages/hpc.html",
            compared_stats=compared_stats,
            data=latest_entry[0],
            segment=hpc,
            parent="HPC",
        )

    @app.route("/pages/logs/")
    def pages_logs():
        return render_template("pages/logs.html", segment="logs", parent="Omnivore")

    @app.route("/api/logs/")
    def api_logs():
        def generate():
            with open("omnivore.log") as f:
                while True:
                    line = f.readline()
                    if not line:
                        break
                    yield line  # Convert newlines to HTML breaks

        return Response(generate(), mimetype="text/html")

    @app.route("/sankey")
    def sankey():
        return render_template(
            "pages/sankey.html",
            segment="Sankey",
            parent="Sankey",
        )

    @app.route("/sunburst")
    def sunburst():
        return render_template(
            "pages/sunburst.html",
            segment="sunburst",
            parent="sunburst",
        )

    @app.template_filter(name="replace_value")
    def replace_value(value, arg):
        return value.replace(arg, " ").title()
