# Standard Library
import logging
import os
import time
from abc import abstractmethod
from math import sqrt

# Third Party
import numpy as np
import pandas as pd
from statsmodels.tsa.statespace.sarimax import SARIMAX

# from statsmodels.tsa.api import ExponentialSmoothing, SimpleExpSmoothing, Holt

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


def train_test_split(data, n_test):
    # take last n datapoint as test data.
    return data[:-n_test], data[-n_test:]


def measure_rmse(actual, predicted):
    return sqrt(np.square(np.subtract(actual, predicted)).mean())


def walk_forward_evaluation(data_series, config):
    order, seasonal_order = config
    n_test = 10
    predictions = []
    training, testing = train_test_split(data_series, n_test)
    history = [x for x in training]

    for t in testing:
        model = SARIMAX(history, order=order, seasonal_order=seasonal_order).fit(
            disp=False
        )
        yhat = model.forecast(1)[0]  # predict the next value
        predictions.append(yhat)
        history.append(t)
    rmse = measure_rmse(testing, predictions)
    return rmse


class ArimaModel(MetricModel):
    """
    SARIMAX model: https://www.statsmodels.org/dev/generated/statsmodels.tsa.statespace.sarimax.SARIMAX.html
    """

    def __init__(self, order=(1, 1, 1), seasonal_order=(0, 0, 0, 0), alpha=None):
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

    def evaluate(self, data_series: pd.Series):
        # walk-forward evaluation, and parameter selection
        start_time = time.time()
        configs = [
            ((1, 1, 1), (0, 0, 0, 0)),
            ((1, 1, 0), (0, 0, 0, 0)),
        ]  # to be extended
        rmses = [walk_forward_evaluation(data_series, c) for c in configs]
        self.order, self.seasonal_order = configs[np.argmin(rmses)]
        logger.info(
            f"model evaluation and parameter selection - in {time.time() - start_time} seconds."
        )

    def predict(self):
        fc_series = self.model.forecast(RETRAINING_INTERVAL_MINUTE)  # get yhat
        intervals = self.model.get_forecast(
            RETRAINING_INTERVAL_MINUTE
        ).conf_int(  # get yhat_lower and yhat_upper
            alpha=self.alpha
        )
        lower_series, upper_series = intervals["lower y"], intervals["upper y"]
        return fc_series, lower_series, upper_series
