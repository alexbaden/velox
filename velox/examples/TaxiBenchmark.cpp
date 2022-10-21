#include <iostream>

#include <folly/init/Init.h>

#include "velox/common/file/FileSystems.h"
#include "velox/dwio/parquet/RegisterParquetReader.h"
#include "velox/dwio/parquet/reader/ParquetReader.h"
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

  VectorPtr batch;
  RowReaderOptions rowReaderOptions;
  auto rowReader = reader->createRowReader(rowReaderOptions);
  while (rowReader->next(500, batch)) {
    auto rowVector = batch->as<RowVector>();
    for (vector_size_t i = 0; i < rowVector->size(); ++i) {
      std::cout << rowVector->toString(i) << std::endl;
    }
  }

  return 0;
}