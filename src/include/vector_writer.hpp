#pragma once

#include "duckdb/common/types/value.hpp"
#include "duckdb/common/types/vector.hpp"

namespace duckdb {

bool TrySetFlatVectorValue(Vector &vector, idx_t row_idx, const Value &value,
                           const LogicalType &target_type);

} // namespace duckdb
