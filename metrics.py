from enum import Enum, unique
from typing import Any, Dict, List, Literal, Optional, Set

from pydantic import BaseModel, validator


@unique
class Aggregation(str, Enum):
    LAST = "LAST"
    MAX = "MAX"
    MEAN = "MEAN"
    MIN = "MIN"
    SUM = "SUM"


class Metrics(BaseModel):
    """Base class for meteorological metrics.

    Attributes
    ----------
    name
    unit
    is_cumulative
    aggregation
        Store the aggregation to be performed related to the meteorological metrics.
        The aggregation can be empty if the metrics related to "instantaneous"
        quantities.
    full_name
        Built from the combination of the name and the aggregation, _e.g._ TEMPERATURE_MIN for TEMPERATURE and MIN. Fall back to the name if the aggregation is not defined."""

    name: str
    unit: str
    is_cumulative: bool
    aggregation: Optional[Aggregation] = None

    @property
    def full_name(self):
        """Combine the metrics name and the aggregation if it exists. full_name"""
        return (
            self.name + "_" + self.aggregation
            if self.aggregation is not None
            else self.name
        )

    @property
    def table_column(self) -> Literal["SUM", "VALUE"]:
        return "SUM" if self.is_cumulative else "VALUE"

    @validator("name")
    def upper(cls, value: str) -> str:
        return "_".join(value.upper().split())

    def dict(self, *args: Any, **kwargs: Any) -> Dict[str, Any]:
        d = super().dict(*args, **kwargs)
        if d["aggregation"] is None:
            _ = d.pop("aggregation")
        return d

    def __str__(self) -> str:
        return (
            f"Metrics: {self.name}, unit: {self.unit}, "
            + f"aggregation type: {self.aggregation}, full name: {self.full_name}"
        )


class MetricsRegistry:
    """
    Parameters
    ----------
    metrics
    prefix
        This optional parameter is necessary for partial registries as the name of the
        enum needs to be unique.
    """

    # This singleton keeps track of the names used for partial registries enum. It
    # avoids conflicts in case a name is already used.
    _registries_prefixes: Dict[str, str] = {}

    @classmethod
    def _get_prefixes(cls) -> Set[str]:
        return set(cls._registries_prefixes.keys())

    @classmethod
    def _add_prefix(cls, prefix: str) -> None:
        cls._registries_prefixes[prefix] = prefix

    def __init__(self, metrics: List[Metrics], prefix: Optional[str] = None) -> None:
        self._metrics: Dict[str, Metrics] = {
            metrics.full_name: metrics for metrics in metrics
        }

        self._prefix = f"{prefix}" if prefix is not None else ""

        if self._prefix in self._get_prefixes():
            raise ValueError(
                f"Prefix {prefix} is already used. Please use a prefix different from {', '.join(self._registries_prefixes.keys())}"
            )
        else:
            self._add_prefix(prefix=self._prefix)

    def generate_registry_by_filtering(
        self, metrics: List[str], prefix: str
    ) -> "MetricsRegistry":
        """Create a new registry by filtering the metrics of the genuine registry.

        The available metrics can be obtained through the method
        list_all_metrics_names.

        Parameters
        ----------
        metrics
        prefix
            Prefix used in naming the Enum related to the registry. The Enum is named {Prefix}Metrics.

        Raises
        ------
        ValueError
            If the prefix has already been used. Prefixes must be unique as they are
            used to build the name of the Enum class.
        """
        _metrics_included = [name.upper() for name in metrics]
        _filtered_metrics = [
            metrics
            for name, metrics in self._metrics.items()
            if name in _metrics_included
        ]
        return MetricsRegistry(metrics=_filtered_metrics, prefix=prefix)

    def list_all_metrics_names(self) -> List[str]:
        return list(self._metrics)

    def get_metrics_by_full_name(self, full_name: str) -> Metrics:
        """Retrieve a Metrics object from the registry by its full name.

        Attributes
        ----------
        full_name
        """
        return self._metrics[str(full_name).upper()]

    def get_enum_from_metrics(self) -> Enum:
        """Provide an Enum object whose values correspond to the metrics full names,
        _i.e._ the combination of the metrics name and the aggregation.

        The 'name' of the Enum is based on the prefix provided at the initialization of
        the instance."""
        return Enum(
            value=f"{self._prefix}Metrics",
            names=[(name, name) for name in self._metrics.keys()],
            type=str,
        )


TEMPERATURE = Metrics(name="temperature", unit="C", is_cumulative=False)
TEMPERATURE_MIN = Metrics(**TEMPERATURE.dict(), aggregation=Aggregation.MIN)
TEMPERATURE_MAX = Metrics(**TEMPERATURE.dict(), aggregation=Aggregation.MAX)
TEMPERATURE_MEAN = Metrics(**TEMPERATURE.dict(), aggregation=Aggregation.MEAN)
TEMPERATURE_LAST = Metrics(**TEMPERATURE.dict(), aggregation=Aggregation.LAST)

RAIN_FALL = Metrics(name="rain fall", unit="C", is_cumulative=True)
WIND_SPEED_2M = Metrics(name="wind speed at 2m", unit="km/h", is_cumulative=False)

MAIN_METRICS_REGISTRY = MetricsRegistry(
    metrics=[TEMPERATURE, RAIN_FALL, WIND_SPEED_2M]
)

METRICS_AND_AGGREGATION_REGISTRY = MetricsRegistry(
    metrics=[TEMPERATURE_MIN, TEMPERATURE_MAX, TEMPERATURE_MEAN, TEMPERATURE_LAST],
    prefix="Aggregated",
)


if __name__ == "__main__":
    print(TEMPERATURE)
    print(TEMPERATURE_MIN)
    print(MAIN_METRICS_REGISTRY.list_all_metrics_names())
    print(METRICS_AND_AGGREGATION_REGISTRY.list_all_metrics_names())
