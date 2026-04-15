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


# ================= TEST =================

print("=== Code Nexus - Data Processor ===")

print("Testing Numeric Processor...")
np = NumericProcessor()
print(f"Trying to validate input '42': {np.validate(42)}")
print(f"Trying to validate input 'Hello': {np.validate('Hello')}")

print(
    "Test invalid ingestion of string 'foo' without prior validation:"
)
try:
    np.ingest("foo")  # type: ignore[arg-type]
except TypeError as e:
    print(f"Got exception: {e}")

np.ingest([1, 2, 3, 4, 5])

print("Extracting 3 values...")
for _ in range(3):
    rank, value = np.output()
    print(f"Numeric value {rank}: {value}")


print("Testing Text Processor...")
tp = TextProcessor()
print(f"Trying to validate input '42': {tp.validate(42)}")

tp.ingest(["Hello", "Nexus", "World"])\n
print("Extracting 1 value...")
rank, value = tp.output()
print(f"Text value {rank}: {value}")


print("Testing Log Processor...")
lp = LogProcessor()
print(f"Trying to validate input 'Hello': {lp.validate('Hello')}")

logs = [
    {"log_level": "NOTICE", "log_message": "Connection to server"},
    {"log_level": "ERROR", "log_message": "Unauthorized access!!"},
]

lp.ingest(logs)

print("Extracting 2 values...")
for _ in range(2):
    rank, value = lp.output()
    print(f"Log entry {rank}: {value}")
