from typing import List


from fastapi import FastAPI, Query
from metrics import MAIN_METRICS_REGISTRY, METRICS_AND_AGGREGATION_REGISTRY
from dataframes import time_series, filter_metrics

api = FastAPI()


@api.get("/all")
def get_from_all(
    metrics: List[MAIN_METRICS_REGISTRY.get_enum_from_metrics()] = Query(),
):
    _metrics = [
        MAIN_METRICS_REGISTRY.get_metrics_by_full_name(full_name=metric.name)
        for metric in metrics
    ]
    return {
        "response": time_series.pipe(
            filter_metrics, metrics=[metric.name for metric in metrics]
        ).to_dict(orient="records")
    }


PARTIAL_METRICS_REGISTRY = (
    METRICS_AND_AGGREGATION_REGISTRY.generate_registry_by_filtering(
        metrics=["TEMPERATURE_MIN", "TEMPERATURE_MAX"], prefix="Partial"
    )
)


@api.get("/partial")
def get_from_partial(
    metrics: List[PARTIAL_METRICS_REGISTRY.get_enum_from_metrics()] = Query(),
):
    _metrics = [
        PARTIAL_METRICS_REGISTRY.get_metrics_by_full_name(full_name=metric.name)
        for metric in metrics
    ]
    return {"response": _metrics}
