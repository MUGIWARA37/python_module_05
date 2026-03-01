from abc import ABC, abstractmethod
from typing import Any, List, Dict, Union, Optional


class DataProcessor(ABC):
    def __init__(self) -> None:
        self.processed_count = 0

    @abstractmethod
    def process(self, data: Any) -> str:
        pass

    @abstractmethod
    def validate(self, data: Any) -> bool:
        pass

    def format_output(self, result: str) -> str:
        return f"Output: {result}"


class NumericProcessor(DataProcessor):
    def __init__(self) -> None:
        super().__init__()

    def validate(self, data: Any) -> bool:
        if isinstance(data, (int, float)):
            return True
        if not isinstance(data, list):
            return False
        return all(isinstance(item, (int, float)) for item in data)

    def process(self, data: Any) -> str:
        try:
            if not self.validate(data):
                raise ValueError("Invalid numeric data")
            if isinstance(data, (int, float)):
                data = [data]
            total = sum(data)
            avg = total / len(data)
            self.processed_count += len(data)
            return (
                f"Processed {len(data)} numeric values,"
                f" sum={total}, avg={avg}"
            )
        except Exception as e:
            return f"Numeric Error: {e}"


class TextProcessor(DataProcessor):
    def __init__(self) -> None:
        super().__init__()

    def validate(self, data: Any) -> bool:
        return isinstance(data, str)

    def process(self, data: Any) -> str:
        try:
            if not self.validate(data):
                raise ValueError("Invalid text data")
            chars = len(data)
            words = len(data.split())
            self.processed_count += 1
            return f"Processed text: {chars} characters, {words} words"
        except Exception as e:
            return f"Text Error: {e}"


class LogProcessor(DataProcessor):
    def __init__(self) -> None:
        super().__init__()
        self.log_levels: List[str] = [
            "DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"
        ]

    def validate(self, data: Any) -> bool:
        if not isinstance(data, str):
            return False
        return any(level in data.upper() for level in self.log_levels)

    def process(self, data: Any) -> str:
        try:
            if not self.validate(data):
                raise ValueError("Invalid log entry")
            parts = data.split(":", 1)
            level = parts[0].strip()
            message = parts[1].strip() if len(parts) > 1 else data
            self.processed_count += 1
            if "ERROR" in level.upper() or "CRITICAL" in level.upper():
                return f"[ALERT] {level} level detected: {message}"
            return f"[{level}] {level} level detected: {message}"
        except Exception as e:
            return f"Log Error: {e}"

    def format_output(self, result: str) -> str:
        return f"Output: {result}"


def demo_polymorphic(
    processors: List[DataProcessor],
    data_items: List[Any]
) -> None:
    print("Processing multiple data types through same interface...")
    for i, (processor, data) in enumerate(
        zip(processors, data_items), start=1
    ):
        result = processor.process(data)
        print(f"Result {i}: {result}")


def get_stats(
    processors: List[DataProcessor]
) -> Dict[str, Union[str, int, float]]:
    return {
        "total_processors": len(processors),
        "total_processed": sum(p.processed_count for p in processors)
    }


if __name__ == "__main__":
    print("=== CODE NEXUS - DATA PROCESSOR FOUNDATION ===\n")

    print("Initializing Numeric Processor...")
    numeric = NumericProcessor()
    data_n = [1, 2, 3, 4, 5]
    print(f"Processing data: {data_n}")
    print("Validation: Numeric data verified")
    print(numeric.format_output(numeric.process(data_n)))

    print("\nInitializing Text Processor...")
    text = TextProcessor()
    data_t = "Hello Nexus World"
    print(f'Processing data: "{data_t}"')
    print("Validation: Text data verified")
    print(text.format_output(text.process(data_t)))

    print("\nInitializing Log Processor...")
    log = LogProcessor()
    data_l: Optional[str] = "ERROR: Connection timeout"
    print(f'Processing data: "{data_l}"')
    print("Validation: Log entry verified")
    print(log.format_output(log.process(data_l)))

    print("\n=== Polymorphic Processing Demo ===")
    processors: List[DataProcessor] = [
        NumericProcessor(),
        TextProcessor(),
        LogProcessor()
    ]
    demo_data: List[Any] = [
        [1, 2, 3],
        "Hello Nexus",
        "INFO: System ready"
    ]
    demo_polymorphic(processors, demo_data)

    print("\n=== Single Value Demo ===")
    numeric2 = NumericProcessor()
    print(numeric2.format_output(numeric2.process(42)))
    print(numeric2.format_output(numeric2.process(3.14)))

    print(
        "\nFoundation systems online. Nexus ready for advanced streams."
    )
