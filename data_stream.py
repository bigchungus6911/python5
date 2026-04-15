from abc import ABC, abstractmethod
from typing import Any, Union


class DataProcessor(ABC):
    def __init__(self) -> None:
        self.storage: list[str] = []
        self.total: int = 0

    @abstractmethod
    def validate(self, data: Any) -> bool:
        pass

    @abstractmethod
    def ingest(self, data: Any) -> None:
        pass

    def output(self) -> tuple[int, str]:
        rank = self.total - len(self.storage)
        return (rank, self.storage.pop(0))


class NumericProcessor(DataProcessor):
    def validate(self, data: Any) -> bool:
        if isinstance(data, (int, float)):
            return True
        if isinstance(data, list):
            for x in data:
                if not isinstance(x, (int, float)):
                    return False
            return True
        return False

    def ingest(self, data: Union[int, float, list[Union[int, float]]]) -> None:
        if not self.validate(data):
            raise TypeError("Improper numeric data")

        if isinstance(data, list):
            for item in data:
                self.storage.append(str(item))
                self.total += 1
        else:
            self.storage.append(str(data))
            self.total += 1


class TextProcessor(DataProcessor):
    def validate(self, data: Any) -> bool:
        if isinstance(data, str):
            return True
        if isinstance(data, list):
            for x in data:
                if not isinstance(x, str):
                    return False
            return True
        return False

    def ingest(self, data: Union[str, list[str]]) -> None:
        if not self.validate(data):
            raise TypeError("Improper text data")

        if isinstance(data, list):
            for item in data:
                self.storage.append(item)
                self.total += 1
        else:
            self.storage.append(data)
            self.total += 1


class LogProcessor(DataProcessor):
    def validate(self, data: Any) -> bool:
        if isinstance(data, dict):
            return True
        if isinstance(data, list):
            for item in data:
                if not isinstance(item, dict):
                    return False
            return True
        return False

    def ingest(
        self, data: Union[dict[str, str], list[dict[str, str]]]
    ) -> None:
        if not self.validate(data):
            raise TypeError("Improper log data")

        if isinstance(data, list):
            for item in data:
                self.storage.append(
                    item["log_level"] + ": " + item["log_message"]
                )
                self.total += 1
        else:
            self.storage.append(
                data["log_level"] + ": " + data["log_message"]
            )
            self.total += 1


class DataStream:
    def __init__(self) -> None:
        self.processors: list[DataProcessor] = []

    def register_processor(self, proc: DataProcessor) -> None:
        self.processors.append(proc)

    def process_stream(self, stream: list[Any]) -> None:
        for element in stream:
            handled = False

            for proc in self.processors:
                if proc.validate(element):
                    proc.ingest(element)
                    handled = True

            if not handled:
                print(
                    "DataStream error - Can't process element in stream: "
                    f"{element}"
                )

    def print_processors_stats(self) -> None:
        print("== DataStream statistics ==")

        if len(self.processors) == 0:
            print("No processor found, no data")
            return

        for proc in self.processors:
            name = proc.__class__.__name__
            remaining = len(proc.storage)
            print(
                f"{name}: total {proc.total} items processed, "
                f"remaining {remaining} on processor"
            )


print("=== Code Nexus - Data Stream ===")

ds = DataStream()

ds.print_processors_stats()

print("Registering Numeric Processor")
np = NumericProcessor()
ds.register_processor(np)

stream = [
    "Hello world",
    [3.14, -1, 2.71],
    [
        {"log_level": "WARNING", "log_message": "Telnet access!"},
        {"log_level": "INFO", "log_message": "User connected"},
    ],
    42,
    ["Hi", "five"],
]

print("Send first batch...")
ds.process_stream(stream)
ds.print_processors_stats()

print("Registering other processors...")
tp = TextProcessor()
lp = LogProcessor()

ds.register_processor(tp)
ds.register_processor(lp)

print("Send same batch again...")
ds.process_stream(stream)
ds.print_processors_stats()

print("Consume some elements...")
for _ in range(3):
    np.output()

for _ in range(2):
    tp.output()

for _ in range(1):
    lp.output()


ds.print_processors_stats()