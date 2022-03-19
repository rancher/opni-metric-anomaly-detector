FROM rancher/opni-python-base:3.8

EXPOSE 8000

RUN zypper update && zypper install -y curl vim

COPY ./requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

COPY ./metric-forecasting /app/

WORKDIR /app

CMD [ "python", "metric_streaming.py" ]
# CMD ["sleep", "10000000000000000"]
