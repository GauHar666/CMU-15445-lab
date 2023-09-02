//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// aggregation_executor.cpp
//
// Identification: src/execution/aggregation_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>
#include <vector>

#include "execution/executors/aggregation_executor.h"

namespace bustub {

AggregationExecutor::AggregationExecutor(ExecutorContext *exec_ctx, const AggregationPlanNode *plan,
                                         std::unique_ptr<AbstractExecutor> &&child)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      child_(std::move(child)),
      aht_(plan_->GetAggregates(), plan_->GetAggregateTypes()),
      aht_iterator_(aht_.End()) {}

void AggregationExecutor::TupleSchemaTranformUseEvaluateAggregate(const std::vector<Value> &group_bys,
                                                                  const std::vector<Value> &aggregates,
                                                                  Tuple *dest_tuple, const Schema *dest_schema) {
  auto colums = dest_schema->GetColumns();
  std::vector<Value> dest_value;
  dest_value.reserve(colums.size());
  for (const auto &col : colums) {
    dest_value.emplace_back(col.GetExpr()->EvaluateAggregate(group_bys, aggregates));
  }
  *dest_tuple = Tuple(dest_value, dest_schema);
}
void AggregationExecutor::Init() {
  child_->Init();
  AggregateKey key;
  AggregateValue value;
  Tuple child_tuple;
  RID child_rid;
  bool res;
  while (true) {
    res = child_->Next(&child_tuple, &child_rid);
    if (!res) {
      break;
    }
    key = MakeAggregateKey(&child_tuple);
    value = MakeAggregateValue(&child_tuple);
    aht_.InsertCombine(key, value);
  }
  aht_iterator_ = aht_.Begin();
}

auto AggregationExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  auto output_schema = plan_->OutputSchema();
  auto having_exr = plan_->GetHaving();
  bool res;
  while (aht_iterator_ != aht_.End()) {
    res = true;
    if (having_exr != nullptr) {
      res =
          having_exr->EvaluateAggregate(aht_iterator_.Key().group_bys_, aht_iterator_.Val().aggregates_).GetAs<bool>();
    }

    if (res) {
      TupleSchemaTranformUseEvaluateAggregate(aht_iterator_.Key().group_bys_, aht_iterator_.Val().aggregates_, tuple,
                                              output_schema);
      ++aht_iterator_;  // 指向下一位置
      return true;
    }

    ++aht_iterator_;  // 指向下一位置
  }
  return false;
}

const AbstractExecutor *AggregationExecutor::GetChildExecutor() const { return child_.get(); }

}  // namespace bustub
