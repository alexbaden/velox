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
#include "velox/exec/PlanNodeStats.h"
#include "velox/exec/Task.h"
#include "velox/exec/tests/utils/Cursor.h"
#include "velox/exec/tests/utils/PlanBuilder.h"
#include "velox/exec/tests/utils/QueryAssertions.h"
#include "velox/functions/prestosql/aggregates/RegisterAggregateFunctions.h"
#include "velox/functions/prestosql/registration/RegistrationFunctions.h"
#include "velox/parse/TypeResolver.h"
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
    functions::prestosql::registerAllScalarFunctions();
    aggregate::prestosql::registerAllAggregateFunctions();
    parse::registerTypeResolver();
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
    CHECK(!planFragment.isGroupedExecution());

    uint32_t num_threads = 8;
    exec::test::CursorParameters params;
    params.planNode = planFragment.planNode;
    params.maxDrivers = num_threads;
    params.numSplitGroups = 1;
    params.numConcurrentSplitGroups = 1;

    auto cursor = std::make_unique<exec::test::TaskCursor>(params);
    auto queryTask = cursor->task();
    cursor->start();

    for (const auto& filePath : filesList) {
      auto connectorSplit =
          std::make_shared<connector::hive::HiveConnectorSplit>(
              kHiveConnectorId,
              "file:" + filePath,
              dwio::common::FileFormat::PARQUET);
      queryTask->addSplit(scanNodeId, exec::Split{connectorSplit});
    }

    queryTask->noMoreSplits(scanNodeId);

    // spin until completion
    while (cursor->moveNext()) {
      LOG(INFO) << "Vector available after processing (scan + sort):";
      auto result = cursor->current();
      for (vector_size_t i = 0; i < result->size(); ++i) {
        LOG(INFO) << result->toString(i);
      }
    }

    CHECK(exec::test::waitForTaskCompletion(queryTask.get()));
    const auto stats = queryTask->taskStats();
    std::cout << fmt::format(
                     "Execution time: {}",
                     succinctMillis(
                         stats.executionEndTimeMs - stats.executionStartTimeMs))
              << std::endl;
    std::cout << fmt::format(
                     "Splits total: {}, finished: {}",
                     stats.numTotalSplits,
                     stats.numFinishedSplits)
              << std::endl;
    std::cout << printPlanWithStats(
                     *planFragment.planNode,
                     stats,
                     /*includeCustomStats=*/true)
              << std::endl;
  }

  // We need a connector id string to identify the connector.
  facebook::velox::RowTypePtr inputRowType;
  std::vector<std::string> filesList;
  const std::string kHiveConnectorId = "test-hive";
};

TaxiBenchmark benchmark;

BENCHMARK(TotalCount) {
  core::PlanNodeId scanNodeId;
  auto queryPlanFragment = exec::test::PlanBuilder()
                               .tableScan(benchmark.inputRowType)
                               .capturePlanNodeId(scanNodeId)
                               .partialAggregation({}, {"count(1)"})
                               .localPartition({})
                               .finalAggregation()
                               .planFragment();

  benchmark.runQuery("TotalCount", queryPlanFragment, scanNodeId);
}

BENCHMARK(Q1) {
  core::PlanNodeId scanNodeId;
  auto queryPlanFragment = exec::test::PlanBuilder()
                               .tableScan(benchmark.inputRowType)
                               .capturePlanNodeId(scanNodeId)
                               .partialAggregation({"cab_type"}, {"count(1)"})
                               .localPartition({})
                               .finalAggregation()
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
          .localPartition({})
          .finalAggregation()
          .planFragment();

  benchmark.runQuery("Q2", queryPlanFragment, scanNodeId);
}

BENCHMARK(Q3) {
  core::PlanNodeId scanNodeId;
  auto queryPlanFragment =
      exec::test::PlanBuilder()
          .tableScan(benchmark.inputRowType)
          .capturePlanNodeId(scanNodeId)
          .project({"passenger_count", "year(pickup_datetime) AS pickup_year"})
          .partialAggregation({"passenger_count", "pickup_year"}, {"count(1)"})
          .localPartition({})
          .finalAggregation()
          .planFragment();

  benchmark.runQuery("Q3", queryPlanFragment, scanNodeId);
}

BENCHMARK(Q4) {
  core::PlanNodeId scanNodeId;
  auto queryPlanFragment =
      exec::test::PlanBuilder()
          .tableScan(benchmark.inputRowType)
          .capturePlanNodeId(scanNodeId)
          .project(
              {"passenger_count",
               "year(pickup_datetime) AS pickup_year",
               "cast(trip_distance as int) AS distance"})
          .partialAggregation(
              {"passenger_count", "pickup_year", "distance"},
              {"count(1) AS the_count"})
          .localPartition({})
          .finalAggregation()
          .orderBy({"pickup_year", "the_count DESC"}, false)
          .planFragment();

  benchmark.runQuery("Q4", queryPlanFragment, scanNodeId);
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
  folly::runBenchmarks();
#else
  core::PlanNodeId scanNodeId;
  auto queryPlanFragment = exec::test::PlanBuilder()
                               .tableScan(benchmark.inputRowType)
                               .capturePlanNodeId(scanNodeId)
                               .partialAggregation({"cab_type"}, {"count(1)"})
                               .localPartition({})
                               .finalAggregation()
                               .planFragment();

  benchmark.runQuery("Q1", queryPlanFragment, scanNodeId);
#endif
  return 0;
}