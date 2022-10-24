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

static void waitForFinishedDrivers(
    const std::shared_ptr<exec::Task>& task,
    uint32_t n) {
  while (task->numFinishedDrivers() < n) {
    /* sleep override */
    usleep(100'000); // 0.1 second.
  }
}

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

  void readFiles(const std::string& directoryPath) {
    auto localFs = filesystems::getFileSystem("file:", nullptr);
    this->filesList = localFs->list(directoryPath);
    CHECK_GE(filesList.size(), size_t(1));

    // read the first file to get schema
    ReaderOptions readerOpts;
    readerOpts.setFileFormat(FileFormat::PARQUET);
    auto reader_factory = parquet::ParquetReaderFactory();
    auto reader = reader_factory.createReader(
        std::make_unique<FileInputStream>(filesList[0]), readerOpts);
    if (!reader->numberOfRows()) {
      throw std::runtime_error("Failed to read parquet file.");
    }
    std::cout << "Read " << *reader->numberOfRows() << " rows." << std::endl;
    inputRowType = reader->rowType();
  }

  void runQuery(
      const std::string& queryName,
      core::PlanFragment& planFragment,
      core::PlanNodeId& scanNodeId) {
    auto queryTask = std::make_shared<exec::Task>(
        queryName,
        planFragment,
        /*destination=*/0,
        core::QueryCtx::createForTest());
    uint32_t num_threads = 112;
    // queryTask->start(queryTask, num_threads, 1);

    for (const auto& filePath : filesList) {
      auto connectorSplit =
          std::make_shared<connector::hive::HiveConnectorSplit>(
              kHiveConnectorId,
              "file:" + filePath,
              dwio::common::FileFormat::PARQUET);
      queryTask->addSplit(scanNodeId, exec::Split{connectorSplit});
    }

    queryTask->noMoreSplits(scanNodeId);
    // waitForFinishedDrivers(queryTask, num_threads);
    // spin until completion
    while (auto result = queryTask->next()) {
      LOG(INFO) << "Vector available after processing (scan + sort):";
      for (vector_size_t i = 0; i < result->size(); ++i) {
        LOG(INFO) << result->toString(i);
      }
    }
  }

  // We need a connector id string to identify the connector.
  facebook::velox::RowTypePtr inputRowType;
  std::vector<std::string> filesList;
  const std::string kHiveConnectorId = "test-hive";
};

TaxiBenchmark benchmark;

void warmup() {
  core::PlanNodeId scanNodeId;
  auto queryPlanFragment = exec::test::PlanBuilder()
                               .tableScan(benchmark.inputRowType)
                               .capturePlanNodeId(scanNodeId)
                               .partialAggregation({"cab_type"}, {"count(1)"})
                               .planFragment();

  benchmark.runQuery("Q1_warmup", queryPlanFragment, scanNodeId);
}

#if 0
BENCHMARK(TableScan) {
  core::PlanNodeId scanNodeId;
  auto queryPlanFragment = exec::test::PlanBuilder()
                               .tableScan(benchmark.inputRowType)
                               .capturePlanNodeId(scanNodeId)
                               .planFragment();

  benchmark.runQuery("TableScan", queryPlanFragment, scanNodeId);
}
#endif

BENCHMARK(Q1) {
  core::PlanNodeId scanNodeId;
  auto queryPlanFragment = exec::test::PlanBuilder()
                               .tableScan(benchmark.inputRowType)
                               .capturePlanNodeId(scanNodeId)
                               .partialAggregation({"cab_type"}, {"count(1)"})
                               .planFragment();

  benchmark.runQuery("Q1", queryPlanFragment, scanNodeId);
}

BENCHMARK(Q2) {
  core::PlanNodeId scanNodeId;
  auto queryPlanFragment =
      exec::test::PlanBuilder()
          .tableScan(benchmark.inputRowType)
          .capturePlanNodeId(scanNodeId)
          .partialAggregation({"passenger_count"}, {"avg(total_amount)"})
          .planFragment();

  benchmark.runQuery("Q2", queryPlanFragment, scanNodeId);
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

#if 1
  // warmup();
  folly::runBenchmarks();
#else
  core::PlanNodeId scanNodeId;
  auto queryPlanFragment = exec::test::PlanBuilder()
                               .tableScan(benchmark.inputRowType)
                               .capturePlanNodeId(scanNodeId)
                               .partialAggregation({"cab_type"}, {"count(1)"})
                               .planFragment();

  benchmark.runQuery("Q1", queryPlanFragment, scanNodeId);
#endif

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