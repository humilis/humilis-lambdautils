"""Utilities to work with Kinesis streams."""

import base64
from datetime import datetime
import json
import logging
import uuid

from dateutil import tz
import boto3

logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())

try:
    from base64 import decodebytes as b64decode
except ImportError:
    # Python 2.x compatibility
    from base64 import decodestring as b64decode


class BadKinesisEventError(Exception):

    """Malformed Kinesis Event."""

    pass


def unpack_kinesis_event(kinesis_event, deserializer=None, unpacker=None,
                         embed_timestamp=False):
    """Extracts events (a list of dicts) from a Kinesis event."""
    records = kinesis_event["Records"]
    events = []
    shard_ids = set()
    for rec in records:
        data = rec["kinesis"]["data"]
        try:
            payload = b64decode(data)
        except TypeError:
            payload = b64decode(data.encode("utf-8"))
        if unpacker:
            payload = unpacker(payload)
        shard_ids.add(rec["eventID"].split(":")[0])
        if deserializer:
            try:
                payload = deserializer(payload)
            except TypeError:
                payload = deserializer(payload.decode())
            except ValueError:
                logger.error("Error deserializing Kinesis payload: {}".format(
                    payload))
                raise

        if isinstance(payload, dict) and embed_timestamp:
            ts = rec["kinesis"].get("approximateArrivalTimestamp")
            if ts:
                ts = datetime.fromtimestamp(ts, tz=tz.tzutc())
                ts_str = ("{year:04d}-{month:02d}-{day:02d} "
                          "{hour:02d}:{minute:02d}:{second:02d}").format(
                    year=ts.year,
                    month=ts.month,
                    day=ts.day,
                    hour=ts.hour,
                    minute=ts.minute,
                    second=ts.second)
            else:
                ts_str = ""

            payload[embed_timestamp] = ts_str
        events.append(payload)

    if len(shard_ids) > 1:
        msg = "Kinesis event contains records from several shards: {}".format(
            shard_ids)
        raise(BadKinesisEventError(msg))

    return events, shard_ids.pop()


def send_to_delivery_stream(events, stream_name):
    """Sends a list of events to a Firehose delivery stream."""
    if not events:
        logger.info("No events provided: nothing delivered to Firehose")
        return

    records = []
    for event in events:
        if not isinstance(event, str):
            # csv events already have a newline
            event = json.dumps(event) + "\n"
        records.append({"Data": event})
    firehose = boto3.client("firehose")
    logger.info("Delivering %s records to Firehose stream '%s'",
                len(records), stream_name)
    resp = firehose.put_record_batch(
        DeliveryStreamName=stream_name,
        Records=records)
    return resp


def send_to_kinesis_stream(events, stream_name, partition_key=None,
                           packer=None, serializer=json.dumps):
    """Sends events to a Kinesis stream."""
    if not events:
        logger.info("No events provided: nothing delivered to Firehose")
        return

    records = []
    for event in events:
        if not partition_key:
            partition_key_value = str(uuid.uuid4())
        elif hasattr(partition_key, "__call__"):
            partition_key_value = partition_key(event)
        else:
            partition_key_value = partition_key

        if not isinstance(event, str):
            event = serializer(event)

        if packer:
            event = packer(event)

        record = {"Data": event,
                  "PartitionKey": partition_key_value}
        records.append(record)

    kinesis = boto3.client("kinesis")
    resp = kinesis.put_records(StreamName=stream_name, Records=records)
    return resp
