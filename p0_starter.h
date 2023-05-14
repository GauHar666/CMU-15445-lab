//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// p0_starter.h
//
// Identification: src/include/primer/p0_starter.h
//
// Copyright (c) 2015-2020, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <memory>
#include <stdexcept>
#include <vector>

#include "common/exception.h"

namespace bustub {

/**
 * The Matrix type defines a common
 * interface for matrix operations.
 */
template <typename T>
class Matrix {
 protected:
  /**
   * TODO(P0): Add implementation
   *
   * Construct a new Matrix instance.
   * @param rows The number of rows
   * @param cols The number of columns
   *
   */
  Matrix(int rows, int cols) {
    this->rows_ = rows;
    this->cols_ = cols;
    linear_ = new T[rows*cols];
  }

  /** The number of rows in the matrix */
  int rows_;
  /** The number of columns in the matrix */
  int cols_;

  /**
   * TODO(P0): Allocate the array in the constructor.
   * TODO(P0): Deallocate the array in the destructor.
   * A flattened array containing the elements of the matrix.
   */
  T *linear_;

 public:
  /** @return The number of rows in the matrix */
  virtual int GetRowCount() const = 0;

  /** @return The number of columns in the matrix */
  virtual int GetColumnCount() const = 0;

  /**
   * Get the (i,j)th matrix element.
   *
   * Throw OUT_OF_RANGE if either index is out of range.
   *
   * @param i The row index
   * @param j The column index
   * @return The (i,j)th matrix element
   * @throws OUT_OF_RANGE if either index is out of range
   */
  virtual T GetElement(int i, int j) const = 0;

  /**
   * Set the (i,j)th matrix element.
   *
   * Throw OUT_OF_RANGE if either index is out of range.
   *
   * @param i The row index
   * @param j The column index
   * @param val The value to insert
   * @throws OUT_OF_RANGE if either index is out of range
   */
  virtual void SetElement(int i, int j, T val) = 0;

  /**
   * Fill the elements of the matrix from `source`.
   *
   * Throw OUT_OF_RANGE in the event that `source`
   * does not contain the required number of elements.
   *
   * @param source The source container
   * @throws OUT_OF_RANGE if `source` is incorrect size
   */
  virtual void FillFrom(const std::vector<T> &source) = 0;

  /**
   * Destroy a matrix instance.
   * TODO(P0): Add implementation
   */
  virtual ~Matrix() {delete[] linear_;}
};

/**
 * The RowMatrix type is a concrete matrix implementation.
 * It implements the interface defined by the Matrix type.
 */

//RowMatrix是相当于一个二维矩阵。
template <typename T>
class RowMatrix : public Matrix<T> {
 public:
  /**
   * TODO(P0): Add implementation
   *
   * Construct a new RowMatrix instance.
   * @param rows The number of rows
   * @param cols The number of columns
   */
  RowMatrix(int rows, int cols) : Matrix<T>(rows, cols) {
    this->data_ = new T* [rows];//先把第一列构造出来
    for (int i = 0; i < rows; i++){
      this->data_[i] = new T[cols];//再从每个列构造后面的行
    } 
  }

  /**
   * TODO(P0): Add implementation
   * @return The number of rows in the matrix
   */
  int GetRowCount() const override { return this->rows_; }

  /**
   * TODO(P0): Add implementation
   * @return The number of columns in the matrix
   */
  int GetColumnCount() const override { return this->cols_; }

  /**
   * TODO(P0): Add implementation
   *
   * Get the (i,j)th matrix element.
   *
   * Throw OUT_OF_RANGE if either index is out of range.
   *
   * @param i The row index
   * @param j The column index
   * @return The (i,j)th matrix element
   * @throws OUT_OF_RANGE if either index is out of range
   */
  T GetElement(int i, int j) const override {
    if(i<=0 || i>=this->rows_ || j<0 || j>=this->cols_){
      throw bustub::Exception(bustub::ExceptionType::OUT_OF_RANGE, "out of range");
    }
    return data_[i][j];
  }

  /**
   * Set the (i,j)th matrix element.
   *
   * Throw OUT_OF_RANGE if either index is out of range.
   *
   * @param i The row index
   * @param j The column index
   * @param val The value to insert
   * @throws OUT_OF_RANGE if either index is out of range
   */
  void SetElement(int i, int j, T val) override {
    if(i<=0 || i>=this->rows_ || j<0 || j>=this->cols_){
      throw bustub::Exception(bustub::ExceptionType::OUT_OF_RANGE, "out of range");
    }
    data_[i][j] = val;
  }

  /**
   * TODO(P0): Add implementation
   *
   * Fill the elements of the matrix from `source`.
   *
   * Throw OUT_OF_RANGE in the event that `source`
   * does not contain the required number of elements.
   *
   * @param source The source container
   * @throws OUT_OF_RANGE if `source` is incorrect size
   */
  //用传入的数组来填充真的matrix：
  void FillFrom(const std::vector<T> &source) override {
    int size = static_cast<int>(source.size());
    if(size != this->rows_ * this->cols_){
      throw bustub::Exception(bustub::ExceptionType::OUT_OF_RANGE,"out of range");
    }
    for(int i=0;i<size;i++){
      int cur_row = i / this->cols_; //表示第几行
      int cur_col = i % this->cols_;//表示第几列
      data_[cur_row][cur_col] = source[i];
    }
  }

  /**
   * TODO(P0): Add implementation
   *
   * Destroy a RowMatrix instance.
   */
  ~RowMatrix() override {
    for(int i=0;i<this->rows_;i++){
      delete[] data_[i]; 
    }

    delete[] data_;
  };

 private:
  /**
   * A 2D array containing the elements of the matrix in row-major format.
   *
   * TODO(P0):
   * - Allocate the array of row pointers in the constructor.
   * - Use these pointers to point to corresponding elements of the `linear` array.
   * - Don't forget to deallocate the array in the destructor.
   */
  T **data_;
};

/**
 * The RowMatrixOperations class defines operations
 * that may be performed on instances of `RowMatrix`.
 */

//矩阵运算的类：
template <typename T>
class RowMatrixOperations {
 public:
  /**
   * Compute (`matrixA` + `matrixB`) and return the result.
   * Return `nullptr` if dimensions mismatch for input matrices.
   * @param matrixA Input matrix
   * @param matrixB Input matrix
   * @return The result of matrix addition
   */
  static std::unique_ptr<RowMatrix<T>> Add(const RowMatrix<T> *matrixA, const RowMatrix<T> *matrixB) {
    //矩阵相加的类，
    //1.判断行列数是否相等：
    if(matrixA->GetRowCount() != matrixB->GetRowCount() || matrixA->GetColumnCount() != matrixB->GetColumnCount()){
      return {};
    }
    int rows = matrixA->GetRowCount();
    int cols = matrixA->GetColumnCount();
    T sum;
    auto mat_c = std::unique_ptr<RowMatrix<T>>(std::make_unique<RowMatrix<T>>(rows, cols));
    for(int i=0; i<rows;i++){
      for(int j=0;j<cols;j++){
        sum = matrixA->GetElement(i,j) + matrixB->GetElement(i,j);
        mat_c->SetElement(i,j,sum);
      }
    }
    return mat_c;
  }

  /**
   * Compute the matrix multiplication (`matrixA` * `matrixB` and return the result.
   * Return `nullptr` if dimensions mismatch for input matrices.
   * @param matrixA Input matrix
   * @param matrixB Input matrix
   * @return The result of matrix multiplication
   */

  //矩阵乘法：
  static std::unique_ptr<RowMatrix<T>> Multiply(const RowMatrix<T> *matrixA, const RowMatrix<T> *matrixB) {
    if(matrixA->GetColumnCount() != matrixB->GetRowCount()){
      return {};
    }
    int rows = matrixA->GetRowCount();
    int cols = matrixB->GetColumnCount();
    int k_size = matrixA->GetColumnCount();
    T sum;
    auto mat_c = std::unique_ptr<RowMatrix<T>>(std::make_unique<RowMatrix<T>>(rows,cols));
    for(int i=0;i<rows;i++){
      for(int j=0;j<cols;j++){
        sum=0;
        for(int k=0;k<k_size;k++){
          sum += matrixA->GetElement(i,k) * matrixB->GetElement(k,j);
        }
        mat_c->SetElement(i, j, sum);
      }
    }
    return mat_c;
  }

  /**
   * Simplified General Matrix Multiply operation. Compute (`matrixA` * `matrixB` + `matrixC`).
   * Return `nullptr` if dimensions mismatch for input matrices.
   * @param matrixA Input matrix
   * @param matrixB Input matrix
   * @param matrixC Input matrix
   * @return The result of general matrix multiply
   */
  //复合计算：(`matrixA` * `matrixB` + `matrixC`)
  static std::unique_ptr<RowMatrix<T>> GEMM(const RowMatrix<T> *matrixA, const RowMatrix<T> *matrixB,
                                            const RowMatrix<T> *matrixC) {
    auto matrix_tmp = Multiply(matrixA,matrixB);
    if(matrix_tmp != nullptr){
      return Add(matrix_tmp.get(),matrixC);
    }
    return {};
  }
};
}  // namespace bustub
