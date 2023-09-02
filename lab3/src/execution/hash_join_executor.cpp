//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_join_executor.cpp
//
// Identification: src/execution/hash_join_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/hash_join_executor.h"
#include "execution/expressions/abstract_expression.h"

namespace bustub {

HashJoinExecutor::HashJoinExecutor(ExecutorContext *exec_ctx, const HashJoinPlanNode *plan,
                                   std::unique_ptr<AbstractExecutor> &&left_child,
                                   std::unique_ptr<AbstractExecutor> &&right_child)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      left_executor_(std::move(left_child)),
      right_executor_(std::move(right_child)) {}

void HashJoinExecutor::TupleSchemaTranformUseEvaluateJoin(const Tuple *left_tuple, const Schema *left_schema,
                                                          const Tuple *right_tuple, const Schema *right_schema,
                                                          Tuple *dest_tuple, const Schema *dest_schema) {
  auto colums = dest_schema->GetColumns();
  std::vector<Value> dest_value;
  dest_value.reserve(colums.size());
  for (const auto &col : colums) {
    dest_value.emplace_back(col.GetExpr()->EvaluateJoin(left_tuple, left_schema, right_tuple, right_schema));
  }
  *dest_tuple = Tuple(dest_value, dest_schema);
}

void HashJoinExecutor::Init() {
  left_executor_->Init();
  right_executor_->Init();
  hash_table_.clear();
  auto right_schema = right_executor_->GetOutputSchema();
  Tuple right_tuple;
  RID right_rid;
  Value right_key;
  bool res;
  while (true) {  // 构建右半部的key-tuple映射
    res = right_executor_->Next(&right_tuple, &right_rid);
    if (!res) {
      break;
    }

    right_key = plan_->RightJoinKeyExpression()->Evaluate(&right_tuple, right_schema);

    if (hash_table_.count(right_key) == 0) {  // 如果map中不存在该key
      hash_table_.insert({right_key, std::vector<Tuple>{right_tuple}});
    } else {
      hash_table_[right_key].emplace_back(right_tuple);
    }
  }
  first_execution_ = true;
}

// 找到存在于哈希表中左半部元组
auto HashJoinExecutor::FindLeftTuple(const Schema *left_schema) -> bool {
  bool res;
  do {
    res = left_executor_->Next(&left_tuple_, &left_rid_);
    if (!res) {
      return false;
    }
    left_key_ = plan_->LeftJoinKeyExpression()->Evaluate(&left_tuple_, left_schema);
  } while (hash_table_.count(left_key_) == 0);
  array_index_ = 0;  // 重置访问位置
  return true;
}

auto HashJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  auto left_schema = left_executor_->GetOutputSchema();
  auto right_schema = right_executor_->GetOutputSchema();
  auto final_schema = plan_->OutputSchema();
  bool res;

  if (hash_table_.empty()) {  // 右半部为空
    return false;
  }

  if (first_execution_) {  // 找到第一个在hash表的左半部元组
    res = FindLeftTuple(left_schema);
    if (!res) {
      return false;
    }
    first_execution_ = false;
  }

  while (true) {
    if (array_index_ >= hash_table_[left_key_].size()) {  // 超出右半部value数组范围，重新开始，需要寻找下一个左半部元组
      res = FindLeftTuple(left_schema);
      if (!res) {
        return false;
      }
    }
    TupleSchemaTranformUseEvaluateJoin(&left_tuple_, left_schema, &hash_table_[left_key_][array_index_], right_schema,
                                       tuple, final_schema);
    array_index_++;  // 指向下一位置
    return true;
  }
  return false;
}

}  // namespace bustub
