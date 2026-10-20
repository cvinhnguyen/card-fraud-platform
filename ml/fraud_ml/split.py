"""Time-based split with embargo gaps, all derived from label_as_of.

  ... train ... | 60 d gap | val (60 d) | 60 d gap | test (60 d) | 60 d immature | label_as_of

Labels are final only 60 days after the payment (disputes arrive up to 60 days late), so the newest usable day is
label_as_of - 60 d. Each gap is also 60 days: a model trained on data up to day T can only be used from T + 60 d,
when the labels of T are known. A random split would leak future fraud patterns into training.
"""
import datetime as dt
from dataclasses import dataclass

from pyspark.sql import Column
from pyspark.sql import functions as F

MATURITY_DAYS = 60
FIRST_DAY = dt.date(2024, 1, 1)


@dataclass(frozen=True)
class TimeSplitter:
    """Date windows of one training run."""

    label_as_of: dt.date
    eval_days: int = 60
    gap_days: int = MATURITY_DAYS

    @property
    def mature_end(self) -> dt.date:
        """First day whose labels are not final yet (exclusive end of all splits)."""
        return self.label_as_of - dt.timedelta(days=MATURITY_DAYS)

    def windows(self) -> dict[str, tuple[dt.date, dt.date]]:
        """split -> [start, end) dates."""
        d = dt.timedelta
        test = (self.mature_end - d(self.eval_days), self.mature_end)
        val = (test[0] - d(self.gap_days + self.eval_days), test[0] - d(self.gap_days))
        train = (FIRST_DAY, val[0] - d(self.gap_days))
        if train[1] <= train[0]:
            raise ValueError(f"label_as_of {self.label_as_of} leaves no training days")
        return {"train": train, "val": val, "test": test}

    def column(self, day: Column) -> Column:
        """Spark column: the split of `day`, NULL inside a gap."""
        out = F.lit(None).cast("string")
        for name, (start, end) in self.windows().items():
            out = F.when((day >= F.lit(start)) & (day < F.lit(end)), F.lit(name)).otherwise(out)
        return out

    def check(self, days_by_split: dict[str, tuple[dt.date, dt.date]]) -> None:
        """Assert observed (min, max) day per split stays inside its window and the gaps are empty."""
        w = self.windows()
        for name, (lo, hi) in days_by_split.items():
            assert w[name][0] <= lo and hi < w[name][1], f"{name} rows {lo}..{hi} outside {w[name]}"
        assert days_by_split["train"][1] + dt.timedelta(days=self.gap_days) <= days_by_split["val"][0]
        assert days_by_split["val"][1] + dt.timedelta(days=self.gap_days) <= days_by_split["test"][0]
