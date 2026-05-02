#include "vector_writer.hpp"

#include "duckdb/common/limits.hpp"
#include "duckdb/common/types/date.hpp"
#if __has_include("duckdb/common/vector/string_vector.hpp")
#include "duckdb/common/vector/string_vector.hpp"
#endif
#if __has_include("duckdb/common/vector/flat_vector.hpp")
#include "duckdb/common/vector/flat_vector.hpp"
#define PBI_SCANNER_HAS_FLAT_VECTOR_GET_DATA_MUTABLE 1
#else
#define PBI_SCANNER_HAS_FLAT_VECTOR_GET_DATA_MUTABLE 0
#endif

#include <cmath>

namespace duckdb {

namespace {

template <class T> static T *GetFlatVectorDataMutable(Vector &vector) {
#if PBI_SCANNER_HAS_FLAT_VECTOR_GET_DATA_MUTABLE
  return FlatVector::GetDataMutable<T>(vector);
#else
  return FlatVector::GetData<T>(vector);
#endif
}

static bool TryConvertDoubleToDate(double serial_value, date_t &out_date) {
  if (!std::isfinite(serial_value)) {
    return false;
  }
  static constexpr int64_t MICROS_PER_DAY = 24LL * 60LL * 60LL * 1000000LL;
  static const int32_t OLE_BASE_EPOCH_DAYS =
      Date::EpochDays(Date::FromString("1899-12-30", true));
  auto total_micros =
      static_cast<int64_t>(serial_value * static_cast<double>(MICROS_PER_DAY) +
                           (serial_value >= 0 ? 0.5 : -0.5));
  auto day_delta = total_micros / MICROS_PER_DAY;
  auto micros_in_day = total_micros % MICROS_PER_DAY;
  if (micros_in_day < 0) {
    micros_in_day += MICROS_PER_DAY;
    day_delta--;
  }
  auto date_days = static_cast<int64_t>(OLE_BASE_EPOCH_DAYS) + day_delta;
  if (date_days < NumericLimits<int32_t>::Minimum() ||
      date_days > NumericLimits<int32_t>::Maximum()) {
    return false;
  }
  out_date = Date::EpochDaysToDate(static_cast<int32_t>(date_days));
  return true;
}

} // namespace

bool TrySetFlatVectorValue(Vector &vector, idx_t row_idx, const Value &value,
                           const LogicalType &target_type) {
  if (value.IsNull()) {
    FlatVector::SetNull(vector, row_idx, true);
    return true;
  }

  auto value_type = value.type().id();
  switch (target_type.id()) {
  case LogicalTypeId::BOOLEAN:
    if (value_type == LogicalTypeId::BOOLEAN) {
      GetFlatVectorDataMutable<bool>(vector)[row_idx] =
          value.GetValueUnsafe<bool>();
      return true;
    }
    break;
  case LogicalTypeId::BIGINT:
    if (value_type == LogicalTypeId::BIGINT) {
      GetFlatVectorDataMutable<int64_t>(vector)[row_idx] =
          value.GetValueUnsafe<int64_t>();
      return true;
    }
    break;
  case LogicalTypeId::UBIGINT:
    if (value_type == LogicalTypeId::UBIGINT) {
      GetFlatVectorDataMutable<uint64_t>(vector)[row_idx] =
          value.GetValueUnsafe<uint64_t>();
      return true;
    }
    break;
  case LogicalTypeId::DOUBLE:
    if (value_type == LogicalTypeId::DOUBLE) {
      GetFlatVectorDataMutable<double>(vector)[row_idx] =
          value.GetValueUnsafe<double>();
      return true;
    }
    break;
  case LogicalTypeId::DATE:
    if (value_type == LogicalTypeId::DATE) {
      GetFlatVectorDataMutable<date_t>(vector)[row_idx] =
          value.GetValueUnsafe<date_t>();
      return true;
    }
    if (value_type == LogicalTypeId::DOUBLE) {
      date_t converted_date;
      if (TryConvertDoubleToDate(value.GetValueUnsafe<double>(),
                                 converted_date)) {
        GetFlatVectorDataMutable<date_t>(vector)[row_idx] = converted_date;
        return true;
      }
    }
    break;
  case LogicalTypeId::TIME:
    if (value_type == LogicalTypeId::TIME) {
      GetFlatVectorDataMutable<dtime_t>(vector)[row_idx] =
          value.GetValueUnsafe<dtime_t>();
      return true;
    }
    break;
  case LogicalTypeId::TIMESTAMP:
    if (value_type == LogicalTypeId::TIMESTAMP) {
      GetFlatVectorDataMutable<timestamp_t>(vector)[row_idx] =
          value.GetValueUnsafe<timestamp_t>();
      return true;
    }
    break;
  case LogicalTypeId::TIMESTAMP_TZ:
    if (value_type == LogicalTypeId::TIMESTAMP_TZ) {
      GetFlatVectorDataMutable<timestamp_tz_t>(vector)[row_idx] =
          value.GetValueUnsafe<timestamp_tz_t>();
      return true;
    }
    break;
  case LogicalTypeId::VARCHAR:
    if (value_type == LogicalTypeId::VARCHAR) {
      auto &text = StringValue::Get(value);
      GetFlatVectorDataMutable<string_t>(vector)[row_idx] =
          StringVector::AddString(vector, text.data(), text.size());
      return true;
    }
    break;
  default:
    break;
  }
  return false;
}

} // namespace duckdb
