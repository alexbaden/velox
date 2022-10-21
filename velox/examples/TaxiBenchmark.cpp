#include <iostream>

#include <folly/init/Init.h>

#include "velox/common/file/FileSystems.h"
#include "velox/connectors/hive/HiveConnector.h"
#include "velox/connectors/hive/HiveConnectorSplit.h"
#include "velox/dwio/dwrf/reader/DwrfReader.h"
#include "velox/dwio/parquet/RegisterParquetReader.h"
#include "velox/dwio/parquet/reader/ParquetReader.h"
#include "velox/exec/Task.h"
#include "velox/exec/tests/utils/PlanBuilder.h"
#include "velox/vector/BaseVector.h"

using namespace facebook::velox;
using namespace facebook::velox::dwio::common;

// read in the taxi data and run the benchmark queries
int main(int argc, char** argv) {
  folly::init(&argc, &argv);

  if (argc < 2) {
    std::cout << "No file path provided" << std::endl;
    return 1;
  }

  // We need a connector id string to identify the connector.
  const std::string kHiveConnectorId = "test-hive";

  // Create a new connector instance from the connector factory and register
  // it:
  auto hiveConnector =
      connector::getConnectorFactory(
          connector::hive::HiveConnectorFactory::kHiveConnectorName)
          ->newConnector(kHiveConnectorId, nullptr);
  connector::registerConnector(hiveConnector);

  // To be able to read local files, we need to register the local file
  // filesystem. We also need to register the parquet reader factory:
  filesystems::registerLocalFileSystem();
  parquet::registerParquetReaderFactory(parquet::ParquetReaderType::NATIVE);

  std::string filePath{argv[1]};
  ReaderOptions readerOpts;
  // To make ParquetReader reads ORC file, setFileFormat to FileFormat::ORC
  readerOpts.setFileFormat(FileFormat::PARQUET);
  auto reader_factory = parquet::ParquetReaderFactory();
  auto reader = reader_factory.createReader(
      std::make_unique<FileInputStream>(filePath), readerOpts);
  if (!reader->numberOfRows()) {
    std::cerr << "Failed to read parquet file." << std::endl;
    return 1;
  }
  std::cout << "Read " << *reader->numberOfRows() << " rows." << std::endl;
  auto inputRowType = reader->rowType();

  core::PlanNodeId scanNodeId;
  auto readPlanFragment = exec::test::PlanBuilder()
                              .tableScan(inputRowType)
                              .capturePlanNodeId(scanNodeId)
                              .topN({"trip_id"}, 5, /*isPartial=*/false)
                              .planFragment();

  auto readTask = std::make_shared<exec::Task>(
      "my_read_task",
      readPlanFragment,
      /*destination=*/0,
      core::QueryCtx::createForTest());

  auto connectorSplit = std::make_shared<connector::hive::HiveConnectorSplit>(
      kHiveConnectorId, "file:" + filePath, dwio::common::FileFormat::PARQUET);
  readTask->addSplit(scanNodeId, exec::Split{connectorSplit});
  readTask->noMoreSplits(scanNodeId);

  while (auto result = readTask->next()) {
    LOG(INFO) << "Vector available after processing (scan + sort):";
    for (vector_size_t i = 0; i < result->size(); ++i) {
      LOG(INFO) << result->toString(i);
    }
  }

  return 0;
}