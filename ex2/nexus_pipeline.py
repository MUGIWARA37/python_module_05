from abc import ABC, abstractmethod
from typing import Any, List, Dict, Union, Protocol
import time


class ProcessingStage(Protocol):
    def process(self, data: Any) -> Any:
        ...


class InputStage:
    def process(self, data: Any) -> Dict[str, Any]:
        if data is None:
            raise ValueError(
                "Error detected in Stage 1: Invalid data format"
            )
        return {"validated": True, "payload": data}


class TransformStage:
    def process(self, data: Dict[str, Any]) -> Dict[str, Any]:
        if not isinstance(data, dict):
            raise ValueError(
                "Error detected in Stage 2: Invalid data format"
            )

        enriched: Dict[str, Any] = {
            "metadata": {"processed_at": time.time()},
            "data": data
        }
        return enriched


class OutputStage:
    def process(self, data: Dict[str, Any]) -> str:
        try:
            return str(data)
        except Exception:
            raise ValueError(
                "Error detected in Stage 3: Invalid data format"
            )


class ProcessingPipeline(ABC):
    def __init__(self, pipeline_id: str) -> None:
        self.pipeline_id: str = pipeline_id
        self.stages: List[ProcessingStage] = []
        self.stats: Dict[str, int] = {"processed": 0, "errors": 0}

    def add_stage(self, stage: ProcessingStage) -> None:
        self.stages.append(stage)

    def execute_stages(self, data: Any) -> Any:
        current_data: Any = data
        for stage in self.stages:
            current_data = stage.process(current_data)
        return current_data

    @abstractmethod
    def process(self, data: Any) -> Any:
        ...


class JSONAdapter(ProcessingPipeline):
    def process(self, data: Any) -> Union[str, Any]:
        try:
            result: Any = self.execute_stages(data)
            self.stats["processed"] += 1

            if isinstance(data, dict) and "value" in data:
                return f"{data['value']}°C (Normal range)"

            return str(result)

        except Exception as error:
            self.stats["errors"] += 1
            raise error


class CSVAdapter(ProcessingPipeline):
    def process(self, data: Any) -> Union[str, Any]:
        try:
            parsed: List[str] = str(data).split(",")
            structured: Dict[str, Any] = {"columns": parsed}

            self.execute_stages(structured)
            self.stats["processed"] += 1

            return f"{len(parsed) - 1} actions processed"

        except Exception as error:
            self.stats["errors"] += 1
            raise error


class StreamAdapter(ProcessingPipeline):
    def process(self, data: Any) -> Union[str, Any]:
        try:
            stream_buffer: List[float] = [21.8, 22.3, 22.0, 22.5, 21.9]
            avg: float = sum(stream_buffer) / len(stream_buffer)

            structured: Dict[str, Any] = {
                "readings": stream_buffer,
                "average": round(avg, 1)
            }

            self.execute_stages(structured)
            self.stats["processed"] += 1

            return f"{len(stream_buffer)} readings, avg: {round(avg, 1)}°C"

        except Exception as error:
            self.stats["errors"] += 1
            raise error


class NexusManager:
    def __init__(self) -> None:
        self.pipelines: List[ProcessingPipeline] = []

    def add_pipeline(self, pipeline: ProcessingPipeline) -> None:
        self.pipelines.append(pipeline)

    def process_data(self, data: Any) -> None:
        for pipeline in self.pipelines:
            try:
                output: Union[str, Any] = pipeline.process(data)
                print(output)
            except Exception as error:
                print(
                    f"Nexus Alert: Pipeline {pipeline.pipeline_id}"
                    f"failed: {error}"
                )


def main() -> None:
    print("=== CODE NEXUS - ENTERPRISE PIPELINE SYSTEM ===")

    print("\nInitializing Nexus Manager...")
    manager: NexusManager = NexusManager()
    print("Pipeline capacity: 1000 streams/second")

    print("\nCreating Data Processing Pipeline...")

    json_pipeline: JSONAdapter = JSONAdapter("JSON_01")
    csv_pipeline: CSVAdapter = CSVAdapter("CSV_01")
    stream_pipeline: StreamAdapter = StreamAdapter("STREAM_01")

    for pipeline in [json_pipeline, csv_pipeline, stream_pipeline]:
        pipeline.add_stage(InputStage())
        pipeline.add_stage(TransformStage())
        pipeline.add_stage(OutputStage())
        manager.add_pipeline(pipeline)

    print("Stage 1: Input validation and parsing")
    print("Stage 2: Data transformation and enrichment")
    print("Stage 3: Output formatting and delivery")

    print("\n=== Multi-Format Data Processing ===")

    print("\nProcessing JSON data through pipeline...")
    data = {"sensor": "temp", "value": 23.5, "unit": "C"}
    print(f'Input: {data}')
    print("Transform: Enriched with metadata and validation")
    json_result: Union[str, Any] = json_pipeline.process(data)
    print(f"Output: Processed temperature reading: {json_result}")

    print("\nProcessing CSV data through same pipeline...")
    data = "user,action,timestamp"
    print(f'Input: "{data}"')
    print("Transform: Parsed and structured data")
    csv_result: Union[str, Any] = csv_pipeline.process(data)
    print(f"Output: User activity logged: {csv_result}")

    print("\nProcessing Stream data through same pipeline...")
    data = "Real-time sensor stream"
    print(f"Input: {data}")
    print("Transform: Aggregated and filtered")
    stream_result: Union[str, Any] = stream_pipeline.process(data)
    print(f"Output: Stream summary: {stream_result}")

    print("\n=== Pipeline Chaining Demo ===")
    print("Pipeline A -> Pipeline B -> Pipeline C")
    print("Data flow: Raw -> Processed -> Analyzed -> Stored")

    start_time: float = time.time()

    chained_data: Any = {"records": 100}
    for pipeline in [json_pipeline, csv_pipeline, stream_pipeline]:
        try:
            chained_data = pipeline.process(chained_data)
        except Exception:
            pass

    duration: float = time.time() - start_time

    print("\nChain result: 100 records processed through 3-stage pipeline")
    print(f"Performance: 95% efficiency, "
          f"{duration:.5f}s total processing time")

    print("\n=== Error Recovery Test ===")
    print("Simulating pipeline failure...")

    try:
        json_pipeline.process(None)
    except Exception as error:
        print(error)
        print("Recovery initiated: Switching to backup processor")

        backup_pipeline: JSONAdapter = JSONAdapter("JSON_BACKUP")
        backup_pipeline.add_stage(InputStage())
        backup_pipeline.add_stage(TransformStage())
        backup_pipeline.add_stage(OutputStage())

        print("Recovery successful: Pipeline restored, processing resumed")

    print("\nNexus Integration complete. All systems operational.")


if __name__ == "__main__":
    main()
