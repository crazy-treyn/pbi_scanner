#pragma once

#include "duckdb/common/types.hpp"

#include <functional>
#include <string>

namespace duckdb {

std::string DecompressXpressLz77Framed(const std::string &compressed);

class XpressLz77FrameStream {
public:
  bool
  Feed(const_data_ptr_t data, idx_t size,
       const std::function<bool(const_data_ptr_t, idx_t)> &on_decompressed);
  void Finish() const;
  idx_t BufferedBytes() const;
  idx_t DecompressedBytes() const;

private:
  static constexpr idx_t FRAME_HEADER_SIZE = 8;

  void Compact(idx_t offset);

  std::string buffer;
  idx_t decompressed_bytes = 0;
};

} // namespace duckdb
