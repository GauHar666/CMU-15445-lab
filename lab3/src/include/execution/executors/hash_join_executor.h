//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_join_executor.h
//
// Identification: src/include/execution/executors/hash_join_executor.h
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <map>
#include <memory>
#include <utility>
#include <vector>

#include "execution/executor_context.h"
#include "execution/executors/abstract_executor.h"
#include "execution/plans/hash_join_plan.h"
#include "storage/table/tuple.h"

namespace bustub {

/**
 * HashJoinExecutor executes a nested-loop JOIN on two tables.
 */
class HashJoinExecutor : public AbstractExecutor {
 public:
  /**
   * Construct a new HashJoinExecutor instance.
   * @param exec_ctx The executor context
   * @param plan The HashJoin join plan to be executed
   * @param left_child The child executor that produces tuples for the left side of join
   * @param right_child The child executor that produces tuples for the right side of join
   */
  HashJoinExecutor(ExecutorContext *exec_ctx, const HashJoinPlanNode *plan,
                   std::unique_ptr<AbstractExecutor> &&left_child, std::unique_ptr<AbstractExecutor> &&right_child);

  /** Initialize the join */
  void Init() override;

  /**
   * Yield the next tuple from the join.
   * @param[out] tuple The next tuple produced by the join
   * @param[out] rid The next tuple RID produced by the join
   * @return `true` if a tuple was produced, `false` if there are no more tuples
   */
  auto Next(Tuple *tuple, RID *rid) -> bool override;

  /** @return The output schema for the join */
  auto GetOutputSchema() -> const Schema * override { return plan_->OutputSchema(); };

 private:
  void TupleSchemaTranformUseEvaluateJoin(const Tuple *left_tuple, const Schema *left_schema, const Tuple *right_tuple,
                                          const Schema *right_schema, Tuple *dest_tuple, const Schema *dest_schema);
  bool FindLeftTuple(const Schema *left_schema);

  struct MapComparator {  // 重载map的key值排序方式
    bool operator()(const Value &v1, const Value &v2) const { return v1.CompareLessThan(v2) == CmpBool::CmpTrue; }
  };
  /** The NestedLoopJoin plan node to be executed. */
  const HashJoinPlanNode *plan_;
  std::unique_ptr<AbstractExecutor> left_executor_;
  std::unique_ptr<AbstractExecutor> right_executor_;

  std::map<Value, std::vector<Tuple>, MapComparator> hash_table_;  // 不使用unordered_map，需要实现两个方法hash和==
  bool first_execution_;
  uint8_t array_index_;  // 哈希表下一次访问的vector索引

  Tuple left_tuple_;  // 存储左半部当前元组
  Value left_key_;
  RID left_rid_;
};

}  // namespace bustub
