from datetime import datetime
from typing import List

import pandera
import pandas

from metrics import MAIN_METRICS_REGISTRY


class Timeseries(pandera.SchemaModel):
    metrics: pandera.typing.Series[str] = pandera.Field(
        isin=MAIN_METRICS_REGISTRY.list_all_metrics_names()
    )
    timestamp: pandera.typing.Series[datetime]
    value: pandera.typing.Series[float]


@pandera.check_input(schema=Timeseries)
def filter_metrics(data: pandera.typing.DataFrame[Timeseries], metrics: List[str]):
    return (
        data.assign(is_in_metrics=lambda df: df["metrics"].isin(metrics))
        .query("is_in_metrics")
        .drop(columns="is_in_metrics")
    )


time_series = pandas.DataFrame(
    data={
        "metrics": ["TEMPERATURE", "RAIN_FALL"],
        "timestamp": [pandas.Timestamp.now(), pandas.Timestamp.now()],
        "value": [1.0, 1.0],
    }
)

wrong_time_series = pandas.DataFrame(
    data={
        "metrics": ["TEMPERATURE", "RAINFALL"],
        "timestamp": [pandas.Timestamp.now(), pandas.Timestamp.now()],
        "value": [1.0, 1.0],
    }
)

if __name__ == "__main__":
    print(time_series.pipe(filter_metrics, metrics=["TEMPERATURE"]))
    try:
        print(wrong_time_series.pipe(filter_metrics, metrics=["TEMPERATURE"]))
    except pandera.errors.SchemaError as e:
        print(e)
