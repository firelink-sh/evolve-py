from confluent_kafka import Consumer, Producer, TopicPartition

from ..ir import IR, BaseBackend, get_global_backend
from ._base import BaseIO


class KafkaTopic(BaseIO):
    def __init__(
        self,
        topic: str,
        *,
        bootstrap_servers: str,
        group_id: str,
        auto_offset_reset: str = "earliest",
        enable_partition_eof: bool = True,
        backend: BaseBackend | None = None,
    ) -> None:
        """Initialize the kafka topic."""
        super().__init__(
            name=self.__class__.__name__,
            backend=backend or get_global_backend(),
        )

        config = {
            "bootstrap.servers": bootstrap_servers,
            "group.id": group_id,
            "auto.offset.reset": auto_offset_reset,
            "enable.partition.eof": enable_partition_eof,
        }

        self._topic = topic
        c = Consumer(config)
        metadata = c.list_topics(topic, timeout=5)
        self._partitions = [p.id for p in metadata.topics[topic].partitions.values()]
        assignments = [TopicPartition(topic, p, 0) for p in self._partitions]
        c.assign(assignments)

        for tp in assignments:
            low, high = c.get_watermark_offsets(tp)
            tp.offset = low

        c.assign(assignments)

        self._consumer = c
        self._producer = Producer(config)

    def read(self) -> IR:
        eof_count = 0
        messages = []
        while True:
            msg = self._consumer.poll(timeout=1.0)
            if msg is None:
                if eof_count >= len(self._partitions):
                    break
                continue
            if msg.error():
                # eof for partition
                if msg.error().code() == msg.error().PARTITION_EOF:
                    eof_count += 1
                    continue
                else:
                    print(f"kafka consumer error: {msg.error()}")
                    continue
            else:
                val = msg.value()
                messages.append(val.decode("utf-8"))

        # close the consumer when we read everything
        self._consumer.close()
        return messages

    def write(self, data: IR) -> None:
        pass
