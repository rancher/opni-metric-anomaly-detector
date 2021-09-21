# from statsmodels.tsa.api import ExponentialSmoothing, SimpleExpSmoothing, Holt
# Standard Library
import logging
import os
from abc import abstractmethod

# Third Party
import pandas as pd
from statsmodels.tsa.statespace.sarimax import SARIMAX

LOGGING_LEVEL = os.getenv("LOGGING_LEVEL", "DEBUG")
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(message)s")
logger = logging.getLogger(__file__)
logger.setLevel(LOGGING_LEVEL)

RETRAINING_INTERVAL_MINUTE = 1
confidence_interval_level = 3  ## should be 2(95%) or 3(99.7%) or 2.57(99%)


class MetricModel:
    @abstractmethod
    def train(self):
        pass

    @abstractmethod
    def predict(self):
        pass


class ArimaModel(MetricModel):
    def __init__(self, order=(1, 1, 1), seasonal_order=(0, 0, 0, 0), alpha=None):
        ## paramters for SARIMAX
        self.order = order
        self.seasonal_order = seasonal_order
        if alpha:
            self.alpha = alpha
        else:
            self.alpha = (
                0.003
                if confidence_interval_level == 3
                else 0.05
                if confidence_interval_level == 2
                else 0.01
            )
        self.model = None

    def train(self, data_series: pd.Series):

        self.model = SARIMAX(
            data_series, order=self.order, seasonal_order=self.seasonal_order
        ).fit(disp=False)

    def predict(self):
        fc_series = self.model.forecast(RETRAINING_INTERVAL_MINUTE)
        intervals = self.model.get_forecast(RETRAINING_INTERVAL_MINUTE).conf_int(
            alpha=self.alpha
        )
        lower_series, upper_series = intervals["lower y"], intervals["upper y"]
        return fc_series, lower_series, upper_series
