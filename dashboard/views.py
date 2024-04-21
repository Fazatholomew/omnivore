# -*- encoding: utf-8 -*-

# Flask modules
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
            .order_by(Telemetry.created_date.desc())
            .limit(2)
            .all()
        )

        data = (
            db.session.query(Data)
            .filter_by(telemetry_id=latest_entry[0].id)
            .order_by(Data.created_date.desc())
            .all()
        )

        last_run = (
            latest_entry[0].created_date - latest_entry[1].created_date
        ).total_seconds()
        compared_stats = [
            {
                "title": "Last Run",
                "value": (
                    "Today"
                    if last_run // 86400 == 0
                    else f"{int(abs(last_run // 86400))} days ago"
                ),
                "icon": "ni-world",
            },
            {
                "title": "Total Runtime",
                "value": f'{(latest_entry[0].total_statistic["total_runtime"] / 3600):.2f} Minutes',
                "percent": (
                    (
                        latest_entry[0].total_statistic["total_runtime"]
                        - latest_entry[1].total_statistic["total_runtime"]
                    )
                    / latest_entry[0].total_statistic["total_runtime"]
                )
                * 100,
                "icon": "ni-paper-diploma",
            },
            {
                "title": "Total Record Processed",
                "value": latest_entry[0].total_statistic["total_records"],
                "percent": (
                    (
                        latest_entry[0].total_statistic["total_records"]
                        - latest_entry[1].total_statistic["total_records"]
                    )
                    / latest_entry[0].total_statistic["total_records"]
                )
                * 100,
                "icon": "ni-cart",
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
            for current_data in data
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

        last_run = (
            latest_entry[0].created_date - latest_entry[1].created_date
        ).total_seconds()

        compared_stats = [
            {
                "title": "Last Run",
                "value": (
                    "Today"
                    if last_run // 86400 == 0
                    else f"{int(abs(last_run // 86400))} days ago"
                ),
                "icon": "ni-world",
            },
            {
                "title": "Total Runtime",
                "value": f"{(latest_entry[0].runtime / 3600):.2f} Minutes",
                "percent": (
                    (latest_entry[0].runtime - latest_entry[1].runtime)
                    / latest_entry[0].runtime
                )
                * 100,
                "icon": "ni-paper-diploma",
            },
            {
                "title": "Total Record Processed",
                "value": latest_entry[0].output,
                "percent": (
                    (latest_entry[0].output - latest_entry[1].output)
                    / latest_entry[0].output
                )
                * 100,
                "icon": "ni-cart",
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

    @app.template_filter(name="replace_value")
    def replace_value(value, arg):
        return value.replace(arg, " ").title()
