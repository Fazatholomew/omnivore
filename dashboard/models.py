from datetime import datetime
from uuid import uuid4
from typing import Optional

from flask_sqlalchemy import SQLAlchemy
from sqlalchemy import Integer, String, DateTime, JSON, ForeignKey, desc, text
from sqlalchemy.orm import (
    DeclarativeBase,
    Mapped,
    mapped_column,
    relationship,
)
from dataclasses import dataclass


class Base(DeclarativeBase):
    pass


db = SQLAlchemy(model_class=Base)


@dataclass
class Telemetry(Base):
    __tablename__ = "telemetry"
    id: Mapped[String] = mapped_column(
        String, primary_key=True, unique=True, default=lambda: uuid4().hex
    )
    created_date: Mapped[DateTime] = mapped_column(DateTime, default=datetime.utcnow)

    hpcs: Mapped[list["HPC"]] = relationship("HPC")
    data: Mapped[list["Data"]] = relationship(
        "Data", order_by=desc(text("Data.created_date"))
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
class HPC(Base):
    __tablename__ = "hpc"
    id: Mapped[Integer] = mapped_column(Integer, primary_key=True, autoincrement=True)
    name: Mapped[String] = mapped_column(String)
    start_time: Mapped[DateTime] = mapped_column(DateTime)
    created_date: Mapped[DateTime] = mapped_column(DateTime, default=datetime.utcnow)
    end_time: Mapped[Optional[DateTime]] = mapped_column(DateTime, nullable=True)
    examples: Mapped[Optional[JSON]] = mapped_column(JSON, nullable=True)
    input: Mapped[Optional[Integer]] = mapped_column(Integer, nullable=True)
    output: Mapped[Optional[Integer]] = mapped_column(Integer, nullable=True)
    latest_record: Mapped[DateTime] = mapped_column(DateTime, nullable=True)
    acc_created: Mapped[Integer] = mapped_column(Integer, default=0)
    acc_updated: Mapped[Integer] = mapped_column(Integer, default=0)
    opp_created: Mapped[Integer] = mapped_column(Integer, default=0)
    opp_updated: Mapped[Integer] = mapped_column(Integer, default=0)

    @property
    def row_errors(self) -> Integer:
        return self.output - (self.opp_created + self.opp_updated)

    @property
    def runtime(self) -> Integer:
        return (self.end_time - self.start_time).total_seconds()

    telemetry_id = mapped_column(ForeignKey("telemetry.id"), nullable=False)


@dataclass
class Data(Base):
    __tablename__ = "data"
    id: Mapped[Integer] = mapped_column(Integer, primary_key=True, autoincrement=True)
    hpc_name: Mapped[String] = mapped_column(String)
    created_date: Mapped[DateTime] = mapped_column(DateTime)
    source: Mapped[String] = mapped_column(String)
    row_number: Mapped[Integer] = mapped_column(Integer)

    telemetry_id = mapped_column(ForeignKey("telemetry.id"), nullable=False)
