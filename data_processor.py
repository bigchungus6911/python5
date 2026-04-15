class DataProcessor:
    def __init__(self) -> None:
        self.data = []

    def process_data(self, data: list[str]) -> None:
        self.data.extend(data)