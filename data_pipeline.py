from abc import ABC, abstractmethod
from typing import Any, Protocol, Union


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


class ExportPlugin(Protocol):
    def process_output(self, data: list[tuple[int, str]]) -> None:
        ...


class CSVPlugin:
    def process_output(self, data: list[tuple[int, str]]) -> None:
        values: list[str] = []
        for _, value in data:
            values.append(value)
        print("CSV Output:")
        print(",".join(values))


class JSONPlugin:
    def process_output(self, data: list[tuple[int, str]]) -> None:
        print("JSON Output:")
        result = "{"
        for i, (rank, value) in enumerate(data):
            result += f"\"item_{rank}\": \"{value}\""
            if i != len(data) - 1:
                result += ", "
        result += "}"
        print(result)


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
                    break

            if not handled:
                print(
                    "DataStream error - Can't process element in stream: "
                    f"{element}"
                )

    def output_pipeline(self, nb: int, plugin: ExportPlugin) -> None:
        for proc in self.processors:
            data: list[tuple[int, str]] = []

            count = 0
            while count < nb and len(proc.storage) > 0:
                data.append(proc.output())
                count += 1

            if len(data) > 0:
                plugin.process_output(data)

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


print("=== Code Nexus - Data Pipeline ===")

ds = DataStream()

ds.print_processors_stats()

print("Registering Processors")
np = NumericProcessor()
tp = TextProcessor()
lp = LogProcessor()

ds.register_processor(np)
ds.register_processor(tp)
ds.register_processor(lp)

stream1 = [
    "Hello world",
    [3.14, -1, 2.71],
    [
        {
            "log_level": "WARNING",
            "log_message": "Telnet access! Use ssh instead",
        },
        {"log_level": "INFO", "log_message": "User wil is connected"},
    ],
    42,
    ["Hi", "five"],
]

print("Send first batch...")
ds.process_stream(stream1)
ds.print_processors_stats()

print("Send 3 items to CSV...")
csv = CSVPlugin()
ds.output_pipeline(3, csv)
ds.print_processors_stats()

stream2 = [
    21,
    ["I love AI", "LLMs are wonderful", "Stay healthy"],
    [
        {"log_level": "ERROR", "log_message": "500 server crash"},
        {"log_level": "NOTICE", "log_message": "Certificate expires"},
    ],
    [32, 42, 64, 84, 128, 168],
    "World hello",
]

print("Send second batch...")
ds.process_stream(stream2)
ds.print_processors_stats()

print("Send 5 items to JSON...")
json = JSONPlugin()
ds.output_pipeline(5, json)
ds.print_processors_stats()