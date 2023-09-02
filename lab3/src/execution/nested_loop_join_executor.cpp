//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nested_loop_join_executor.cpp
//
// Identification: src/execution/nested_loop_join_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/nested_loop_join_executor.h"

namespace bustub {

NestedLoopJoinExecutor::NestedLoopJoinExecutor(ExecutorContext *exec_ctx, const NestedLoopJoinPlanNode *plan,
                                               std::unique_ptr<AbstractExecutor> &&left_executor,
                                               std::unique_ptr<AbstractExecutor> &&right_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      left_executor_(std::move(left_executor)),
      right_executor_(std::move(right_executor)) {}

void NestedLoopJoinExecutor::TupleSchemaTranformUseEvaluateJoin(const Tuple *left_tuple, const Schema *left_schema,
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

void NestedLoopJoinExecutor::Init() {
  left_executor_->Init();
  right_executor_->Init();
  first_execution_ = true;
}

auto NestedLoopJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  auto predicate = plan_->Predicate();
  auto left_schema = left_executor_->GetOutputSchema();
  auto right_schema = right_executor_->GetOutputSchema();
  auto final_schema = plan_->OutputSchema();
  Tuple right_tuple;
  bool left_res;   // 左半部执行结果
  bool right_res;  // 右半部执行结果
  bool predicate_res;

  if (first_execution_) {  // 第一次next调用
    left_res = left_executor_->Next(&left_tuple_, &left_rid_);
    right_res = right_executor_->Next(&right_tuple_, &right_rid_);
    first_execution_ = false;
    if (!left_res || !right_res) {  // 如果左半部为空或右半部为空
      return false;
    }
    predicate_res = predicate->EvaluateJoin(&left_tuple_, left_schema, &right_tuple_, right_schema).GetAs<bool>();
    if (predicate_res) {
      TupleSchemaTranformUseEvaluateJoin(&left_tuple_, left_schema, &right_tuple_, right_schema, tuple, final_schema);
      *rid = tuple->GetRid();
      return true;
    }
  }

  while (true) {
    right_res = right_executor_->Next(&right_tuple_, &right_rid_);
    if (!right_res) {  // 右半部到了末尾：左半部加一，右半部重新开始
      left_res = left_executor_->Next(&left_tuple_, &left_rid_);
      if (!left_res) {  // 左半部到达末尾，结束执行
        return false;
      }
      right_executor_->Init();
      right_executor_->Next(&right_tuple_, &right_rid_);
    }

    predicate_res = predicate->EvaluateJoin(&left_tuple_, left_schema, &right_tuple_, right_schema).GetAs<bool>();
    if (predicate_res) {
      TupleSchemaTranformUseEvaluateJoin(&left_tuple_, left_schema, &right_tuple_, right_schema, tuple, final_schema);
      return true;
    }
  }
}

}  // namespace bustub
