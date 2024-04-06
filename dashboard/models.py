from datetime import datetime

from flask_sqlalchemy import SQLAlchemy
from sqlalchemy import Integer, String, DateTime, JSON, ForeignKey
from sqlalchemy.orm import (
    Mapped,
    mapped_column,
    relationship,
    attribute_mapped_collection,
)
from dataclasses import dataclass

db = SQLAlchemy()


@dataclass
class Telemetry(db.Model):
    id: Mapped[String] = mapped_column(String, primary_key=True, unique=True)
    created_date: Mapped[DateTime] = mapped_column(DateTime, default=datetime.utcnow)

    hpcs: Mapped[list["HPC"]] = relationship("HPC")
    data: Mapped[list["Data"]] = relationship(
        "Data",
        collection_class=attribute_mapped_collection("hpc_name"),
    )

    @property
    def total_statistic(self) -> JSON:
        result = {
            "total_runtime": 0,
            "total_records": 0,
            "total_acc_created": 0,
            "total_acc_updated": 0,
            "total_opp_created": 0,
            "total_opp_updated": 0,
        }
        for hpc in self.hpcs:
            result["total_runtime"] += hpc.runtime
            result["total_records"] += hpc.output
            result["total_acc_created"] += hpc.acc_created
            result["total_acc_updated"] += hpc.acc_updated
            result["total_opp_created"] += hpc.opp_created
            result["total_opp_updated"] += hpc.opp_updated
        return result


@dataclass
class HPC(db.Model):
    id: Mapped[Integer] = mapped_column(Integer, primary_key=True, autoincrement=True)
    name: Mapped[String] = mapped_column(String)
    created_date: Mapped[DateTime] = mapped_column(DateTime, default=datetime.utcnow)
    start_time: Mapped[DateTime] = mapped_column(DateTime)
    end_time: Mapped[DateTime] = mapped_column(DateTime)
    examples: Mapped[JSON] = mapped_column(JSON)
    input: Mapped[Integer] = mapped_column(Integer)
    output: Mapped[Integer] = mapped_column(Integer)
    acc_created: Mapped[Integer] = mapped_column(Integer)
    acc_updated: Mapped[Integer] = mapped_column(Integer)
    opp_created: Mapped[Integer] = mapped_column(Integer)
    opp_updated: Mapped[Integer] = mapped_column(Integer)

    @property
    def row_errors(self) -> Integer:
        return self.output - (self.opp_created + self.opp_updated)

    @property
    def runtime(self) -> Integer:
        return (self.end_time - self.start_time).total_seconds()

    telemetry_id = mapped_column(ForeignKey("telemetry.id"), nullable=False)


@dataclass
class Data(db.Model):
    id: Mapped[Integer] = mapped_column(Integer, primary_key=True, autoincrement=True)
    hpc_name: Mapped[String] = mapped_column(String)
    created_date: Mapped[DateTime] = mapped_column(DateTime)
    source: Mapped[String] = mapped_column(String)
    row_number: Mapped[Integer] = mapped_column(Integer)

    telemetry_id = mapped_column(ForeignKey("telemetry.id"), nullable=False)
