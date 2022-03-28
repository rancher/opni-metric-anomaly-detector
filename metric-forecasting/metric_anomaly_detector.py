# Standard Library
import logging
import os
from datetime import datetime

# Third Party
import numpy as np
import pandas as pd
import ruptures as rpt
from metric_model import ArimaModel

## configurable parameters
RETRAINING_INTERVAL_MINUTE = 1
ROLLING_TRAINING_SIZE = 1440
MIN_TRAINING_SIZE = 30
ALERT_SCORE_THRESHOLD = 5
CPD_TRACE_BACK_TIME = 30  ## minutes
LOOP_TIME_SECOND = float(
    os.getenv("LOOP_TIME_SECOND", 60.0)
)  # 60.0   # unit: second, type: float

LOGGING_LEVEL = os.getenv("LOGGING_LEVEL", "DEBUG")
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(message)s")
logger = logging.getLogger(__file__)
logger.setLevel(LOGGING_LEVEL)


class MetricAnomalyDetector:
    def __init__(self, metric_name, cluster_id):
        self.cluster_id = cluster_id
        self.metric_name = metric_name
        self.metric_xs = []
        self.metric_y = []
        self.metric_rawy = []
        self.pred_history = []
        self.alert_score_counter = 0
        self.metric_model = ArimaModel()
        self.anomaly_history = []
        self.start_index = 0
        self.anomaly_start_time = None
        # start index of training data. update by CPD and rolling time-window. For example if there's 2000 data points in the list, start_index should be 560.

    def load(self, history_data):
        for h in history_data:
            d = h["_source"]
            self.metric_xs.append(datetime.fromisoformat(d["timestamp"]))
            self.metric_y.append(d["y"])
            self.metric_rawy.append(d["y"])
            self.pred_history.append((d["yhat"], d["yhat_lower"], d["yhat_upper"]))
            self.anomaly_history.append(d["is_anomaly"])
        logger.debug(f"history data loaded : {len(self.metric_xs)}")

    def run(self, data):
        xs_raw, y_raw = data[0]["value"]
        xs_raw = (
            int(xs_raw) // LOOP_TIME_SECOND * LOOP_TIME_SECOND
        )  ## convert time to start of minute :00
        xs_new = datetime.fromtimestamp(float(xs_raw))
        if xs_new in self.metric_xs:
            logger.warning("ERROR: duplicated timestamp!")
            return None
        y_new = float(y_raw)

        self.train_and_predict()
        is_anomaly = 0
        is_alert = 0
        alert_len = None
        json_payload = {
            "cluster_id": self.cluster_id,
            "timestamp": xs_new,  # xs_new.isoformat(),
            "is_anomaly": is_anomaly,
            "metric_name": self.metric_name,
            "alert_score": 0,
            "is_alert": is_alert,
            "y": y_new,
            "yhat": y_new,
            "yhat_lower": y_new,
            "yhat_upper": y_new,
            "confidence_score": 0,
            "alert_id": self.anomaly_start_time,
            "alert_len": alert_len,
        }
        if len(self.metric_xs) >= MIN_TRAINING_SIZE:
            y_pred, y_pred_low, y_pred_high = self.pred_history[len(self.metric_xs)]
            if self.metric_name == "disk_usage":
                y_pred = None
                y_pred_high = 80
                is_anomaly = 1 if y_new >= y_pred_high else 0  # trigger: above boundary
            else:
                is_anomaly = (
                    1 if (y_new < y_pred_low or y_new > y_pred_high) else 0
                )  # trigger: above or below

            if is_anomaly == 1:
                if self.alert_score_counter == 0:
                    self.anomaly_start_time = xs_raw
                self.alert_score_counter += 1
            else:
                if self.alert_score_counter > 0:
                    self.alert_score_counter -= 2
                    if self.alert_score_counter <= 0:
                        self.anomaly_start_time = None
                        self.alert_score_counter = 0
            if self.anomaly_start_time:
                alert_len = f"{int((xs_raw - self.anomaly_start_time) // LOOP_TIME_SECOND)} minutes"

            if self.alert_score_counter >= ALERT_SCORE_THRESHOLD:
                is_alert = 1

            if is_alert == 1:
                logger.warning(
                    f"Alert for {self.metric_name} at time : {xs_new.isoformat()}"
                )

            if y_pred:
                json_payload["yhat"] = y_pred
            if y_pred_low:
                json_payload["yhat_lower"] = min(max(0, y_pred_low), 100)
            if y_pred_high:
                json_payload["yhat_upper"] = min(max(y_pred_high, 0), 100)
            json_payload["confidence_score"] = 1  ##TODO
            json_payload["is_anomaly"] = is_anomaly
            json_payload["is_alert"] = is_alert
            json_payload["alert_score"] = self.alert_score_counter
            json_payload["alert_id"] = self.anomaly_start_time
            json_payload["alert_len"] = alert_len

        else:  ## for first 10 mins, simply use y as pred values
            self.pred_history.append((None, None, None))

        if self.metric_name == "cpu_usage" and is_alert == 0 and is_anomaly == 1:
            logger.debug("Experimental: correct anomaly value.")
            if y_new < y_pred_low:
                y_train = y_pred_low
            else:
                y_train = y_pred_high
        else:
            y_train = y_new

        self.anomaly_history.append(is_anomaly)
        self.metric_xs.append(xs_new)
        self.metric_y.append(y_train)
        self.metric_rawy.append(y_new)

        if (
            len(self.anomaly_history) >= CPD_TRACE_BACK_TIME
            and self.anomaly_history[-CPD_TRACE_BACK_TIME] == 1
            and self.metric_name == "cpu_usage"
        ):
            self.change_point_detection()

        return json_payload

    def update_alert_score(self, is_anomaly):
        """
        This function update the alert score if an anomaly gets detected.
        ## rule for alerting.
            Two kinds of alert decision rules: ref: https://github.com/nfrumkin/forecast-prometheus/blob/master/notebooks/Anomaly%20Detection%20Decision%20Rules.ipynb
                1. The Accumulator.
                    The accumulator detection rules is based on the assumption that anomalous data is persistent.
                    Rather than detecting anomalies point-wise, we have a running counter which increments when a point is flagged as anomalous and
                     decremenets by 2 when a normal point is detected. If the counter reaches a certain threshhold value, then we raise a flag.
                2. The Tail Probability
                    This anomaly detection rule uses the idea that the recent past's noise is comparable to the current noise.
                    Using the gaussian noise assumption, we calculate the tail probability that that the mean of the current values is comparable
                     to the values in the recent past.
                    The implementation can be found in this paper: https://arxiv.org/pdf/1607.02480.pdf
            To emsemble them, we can either take the intersect or the union.
        """
        if is_anomaly == 1:
            if self.alert_score_counter == 0:
                self.anomaly_start_time = xs_raw
            self.alert_score_counter += 1
        else:
            if self.alert_score_counter > 0:
                self.alert_score_counter -= 2
                if self.alert_score_counter <= 0:
                    alert_len = (xs_raw - self.anomaly_start_time) // LOOP_TIME_SECOND
                    self.anomaly_start_time = None
                    self.alert_score_counter = 0

        if self.alert_score_counter >= ALERT_SCORE_THRESHOLD:
            return 1
        return 0

    def change_point_detection(self):
        """
        change point detection function.
        This is to find the change points in the time-series trend (in terms of changed mean or variance).
        """
        training_y = self.metric_y[self.start_index :]
        if len(training_y) >= 2 * CPD_TRACE_BACK_TIME:
            cpd = rpt.Pelt(model="rbf").fit(np.array(training_y))
            change_locations = (cpd.predict(pen=10))[
                :-1
            ]  # remove last one because it's always the end of array so it's meaningless
            for l in reversed(change_locations):
                if (
                    abs(l - len(training_y) + CPD_TRACE_BACK_TIME) <= 5
                ):  # the change point should nearby an anomaly
                    logger.debug(
                        f"reset start_index from {self.metric_xs[self.start_index]} to {self.metric_xs[self.start_index + l]}"
                    )
                    self.start_index = self.start_index + l
                    break

    def train_and_predict(self):
        """
        modeling includes train and predict.
        every time there's a new data point, train a new model and forecast one step in future.
        """
        if len(self.metric_xs) >= MIN_TRAINING_SIZE:
            tdf = pd.Series(self.metric_y, self.metric_xs)
            training_dataseries = (tdf[~tdf.index.duplicated(keep="first")]).asfreq(
                freq="T"
            )  ## TODO: better efficiency
            ## max training size should be [ROLLING_TRAINING_SIZE]
            if len(self.metric_y) - self.start_index > ROLLING_TRAINING_SIZE:
                self.start_index = len(self.metric_y) - ROLLING_TRAINING_SIZE
            training_dataseries = training_dataseries[self.start_index :]

            eval_interval = 10  # every 10 mins
            # if len(training_dataseries) % eval_interval == 0:
            #     self.metric_model.evaluate(training_dataseries)
            self.metric_model.train(training_dataseries)
            preds, lower_bounds, upper_bounds = self.metric_model.predict()
            for p in zip(preds, lower_bounds, upper_bounds):
                self.pred_history.append(p)
