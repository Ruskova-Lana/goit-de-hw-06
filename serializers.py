"""Серіалізатори для kafka-python (класи замість lambda, щоб уникнути DeprecationWarning)."""

import json

from kafka.serializer import Deserializer, Serializer


class JsonSerializer(Serializer):
    def serialize(self, topic, value):
        return None if value is None else json.dumps(value).encode("utf-8")


class JsonDeserializer(Deserializer):
    def deserialize(self, topic, bytes_):
        return None if bytes_ is None else json.loads(bytes_.decode("utf-8"))


class StrSerializer(Serializer):
    def serialize(self, topic, value):
        return None if value is None else str(value).encode("utf-8")


class StrDeserializer(Deserializer):
    def deserialize(self, topic, bytes_):
        return None if bytes_ is None else bytes_.decode("utf-8")
