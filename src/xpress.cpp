#include "xpress.hpp"

#include "duckdb/common/constants.hpp"
#include "duckdb/common/exception.hpp"

namespace duckdb {

namespace {

static uint16_t ReadLittleEndian16(const_data_ptr_t data, idx_t size,
                                   idx_t offset) {
  if (offset + 2 > size) {
    throw IOException("XPRESS payload ended unexpectedly");
  }
  return static_cast<uint16_t>(data[offset]) |
         static_cast<uint16_t>(data[offset + 1] << 8);
}

static uint32_t ReadLittleEndian32(const_data_ptr_t data, idx_t size,
                                   idx_t offset) {
  if (offset + 4 > size) {
    throw IOException("XPRESS payload ended unexpectedly");
  }
  return static_cast<uint32_t>(data[offset]) |
         (static_cast<uint32_t>(data[offset + 1]) << 8) |
         (static_cast<uint32_t>(data[offset + 2]) << 16) |
         (static_cast<uint32_t>(data[offset + 3]) << 24);
}

static void DecompressXpressLz77Block(const_data_ptr_t input, idx_t input_size,
                                      std::string &output,
                                      idx_t expected_output_size) {
  idx_t input_pos = 0;
  idx_t output_start = output.size();
  idx_t last_length_half_byte = DConstants::INVALID_INDEX;

  while (output.size() - output_start < expected_output_size) {
    auto flags = ReadLittleEndian32(input, input_size, input_pos);
    input_pos += 4;

    for (int bit = 31;
         bit >= 0 && output.size() - output_start < expected_output_size;
         bit--) {
      if ((flags & (uint32_t(1) << bit)) == 0) {
        if (input_pos >= input_size) {
          throw IOException("XPRESS literal exceeded compressed block");
        }
        output.push_back(static_cast<char>(input[input_pos++]));
        continue;
      }

      auto match_bytes = ReadLittleEndian16(input, input_size, input_pos);
      input_pos += 2;
      uint32_t match_length = match_bytes & 0x7;
      auto match_offset = static_cast<idx_t>((match_bytes >> 3) + 1);
      if (match_length == 7) {
        if (last_length_half_byte == DConstants::INVALID_INDEX) {
          if (input_pos >= input_size) {
            throw IOException("XPRESS match length exceeded compressed block");
          }
          match_length = input[input_pos] & 0x0F;
          last_length_half_byte = input_pos++;
        } else {
          match_length = input[last_length_half_byte] >> 4;
          last_length_half_byte = DConstants::INVALID_INDEX;
        }
        if (match_length == 15) {
          if (input_pos >= input_size) {
            throw IOException("XPRESS extended match length missing");
          }
          match_length = input[input_pos++];
          if (match_length == 255) {
            match_length = ReadLittleEndian16(input, input_size, input_pos);
            input_pos += 2;
            if (match_length == 0) {
              match_length = ReadLittleEndian32(input, input_size, input_pos);
              input_pos += 4;
            }
            if (match_length < 22) {
              throw IOException("XPRESS extended match length was invalid");
            }
            match_length -= 22;
          }
          match_length += 15;
        }
        match_length += 7;
      }
      match_length += 3;

      if (match_offset == 0 || match_offset > output.size() - output_start) {
        throw IOException("XPRESS match offset was invalid");
      }
      for (uint32_t i = 0; i < match_length; i++) {
        output.push_back(output[output.size() - match_offset]);
        if (output.size() - output_start > expected_output_size) {
          throw IOException("XPRESS match exceeded expected block size");
        }
      }
    }
  }
}

} // namespace

std::string DecompressXpressLz77Framed(const std::string &compressed) {
  std::string output;
  auto input = const_data_ptr_cast(compressed.data());
  auto input_size = static_cast<idx_t>(compressed.size());
  idx_t input_pos = 0;

  while (input_pos < input_size) {
    auto original_size = ReadLittleEndian32(input, input_size, input_pos);
    input_pos += 4;
    auto compressed_size = ReadLittleEndian32(input, input_size, input_pos);
    input_pos += 4;
    if (original_size > 65535 || compressed_size > 65535) {
      throw IOException("XPRESS block sizes must not exceed 65535 bytes");
    }
    if (input_pos + compressed_size > input_size) {
      throw IOException("XPRESS block exceeded payload size");
    }
    if (original_size == compressed_size) {
      output.append(reinterpret_cast<const char *>(input + input_pos),
                    compressed_size);
    } else {
      DecompressXpressLz77Block(input + input_pos, compressed_size, output,
                                original_size);
    }
    input_pos += compressed_size;
  }
  return output;
}

bool XpressLz77FrameStream::Feed(
    const_data_ptr_t data, idx_t size,
    const std::function<bool(const_data_ptr_t, idx_t)> &on_decompressed) {
  buffer.append(reinterpret_cast<const char *>(data), size);
  idx_t offset = 0;
  while (buffer.size() - offset >= FRAME_HEADER_SIZE) {
    auto input = const_data_ptr_cast(buffer.data());
    auto original_size = ReadLittleEndian32(input, buffer.size(), offset);
    auto compressed_size = ReadLittleEndian32(input, buffer.size(), offset + 4);
    if (original_size > 65535 || compressed_size > 65535) {
      throw IOException("XPRESS block sizes must not exceed 65535 bytes");
    }
    auto frame_size = FRAME_HEADER_SIZE + static_cast<idx_t>(compressed_size);
    if (buffer.size() - offset < frame_size) {
      break;
    }

    std::string output;
    output.reserve(original_size);
    auto frame_payload = input + offset + FRAME_HEADER_SIZE;
    if (original_size == compressed_size) {
      output.append(reinterpret_cast<const char *>(frame_payload),
                    compressed_size);
    } else {
      DecompressXpressLz77Block(frame_payload, compressed_size, output,
                                original_size);
    }
    offset += frame_size;
    decompressed_bytes += output.size();
    if (!output.empty() &&
        !on_decompressed(const_data_ptr_cast(output.data()), output.size())) {
      Compact(offset);
      return false;
    }
  }
  Compact(offset);
  return true;
}

void XpressLz77FrameStream::Finish() const {
  if (!buffer.empty()) {
    throw IOException("XPRESS payload ended unexpectedly");
  }
}

idx_t XpressLz77FrameStream::BufferedBytes() const { return buffer.size(); }

idx_t XpressLz77FrameStream::DecompressedBytes() const {
  return decompressed_bytes;
}

void XpressLz77FrameStream::Compact(idx_t offset) {
  if (offset == 0) {
    return;
  }
  buffer.erase(0, offset);
}

} // namespace duckdb
