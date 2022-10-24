#include <iostream>

#include <folly/Benchmark.h>
#include <folly/init/Init.h>
#include <gflags/gflags.h>

#include "velox/common/file/FileSystems.h"
#include "velox/connectors/hive/HiveConnector.h"
#include "velox/connectors/hive/HiveConnectorSplit.h"
#include "velox/dwio/dwrf/reader/DwrfReader.h"
#include "velox/dwio/parquet/RegisterParquetReader.h"
#include "velox/dwio/parquet/reader/ParquetReader.h"
#include "velox/exec/Aggregate.h"
#include "velox/exec/Task.h"
#include "velox/exec/tests/utils/PlanBuilder.h"
#include "velox/functions/prestosql/aggregates/RegisterAggregateFunctions.h"
#include "velox/vector/BaseVector.h"

using namespace facebook::velox;
using namespace facebook::velox::dwio::common;

class TaxiBenchmark {
 public:
  void initialize() {
    aggregate::prestosql::registerAllAggregateFunctions();
    // To be able to read local files, we need to register the local file
    // filesystem. We also need to register the parquet reader factory:
    filesystems::registerLocalFileSystem();
    parquet::registerParquetReaderFactory(parquet::ParquetReaderType::NATIVE);

    // Create a new connector instance from the connector factory and register
    // it:
    auto hiveConnector =
        connector::getConnectorFactory(
            connector::hive::HiveConnectorFactory::kHiveConnectorName)
            ->newConnector(kHiveConnectorId, nullptr);
    connector::registerConnector(hiveConnector);
  }

  void readFiles(const std::string& filePath) {
    this->filePath = filePath; // TODO
    ReaderOptions readerOpts;
    // To make ParquetReader reads ORC file, setFileFormat to FileFormat::ORC
    readerOpts.setFileFormat(FileFormat::PARQUET);
    auto reader_factory = parquet::ParquetReaderFactory();
    auto reader = reader_factory.createReader(
        std::make_unique<FileInputStream>(filePath), readerOpts);
    if (!reader->numberOfRows()) {
      throw std::runtime_error("Failed to read parquet file.");
    }
    std::cout << "Read " << *reader->numberOfRows() << " rows." << std::endl;
    inputRowType = reader->rowType();
  }

  // We need a connector id string to identify the connector.
  facebook::velox::RowTypePtr inputRowType;
  std::string filePath;
  const std::string kHiveConnectorId = "test-hive";
};

TaxiBenchmark benchmark;

BENCHMARK(Q1) {
  core::PlanNodeId scanNodeId;
  auto readPlanFragment = exec::test::PlanBuilder()
                              .tableScan(benchmark.inputRowType)
                              .capturePlanNodeId(scanNodeId)
                              .partialAggregation({"cab_type"}, {"count(1)"})
                              .planFragment();

  auto readTask = std::make_shared<exec::Task>(
      "my_read_task",
      readPlanFragment,
      /*destination=*/0,
      core::QueryCtx::createForTest());

  auto connectorSplit = std::make_shared<connector::hive::HiveConnectorSplit>(
      benchmark.kHiveConnectorId,
      "file:" + benchmark.filePath,
      dwio::common::FileFormat::PARQUET);
  readTask->addSplit(scanNodeId, exec::Split{connectorSplit});
  readTask->noMoreSplits(scanNodeId);

  // spin until completion
  while (auto result = readTask->next()) {
  }
}

// read in the taxi data and run the benchmark queries
int main(int argc, char** argv) {
  folly::init(&argc, &argv);

  benchmark.initialize();

  if (argc < 2) {
    std::cout << "No file path provided" << std::endl;
    return 1;
  }

  std::string filePath{argv[1]};
  benchmark.readFiles(filePath);

  folly::runBenchmarks();

#if 0
  while (auto result = readTask->next()) {
    LOG(INFO) << "Vector available after processing (scan + sort):";
    for (vector_size_t i = 0; i < result->size(); ++i) {
      LOG(INFO) << result->toString(i);
    }
  }
#endif

  return 0;
}