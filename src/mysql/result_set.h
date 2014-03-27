/*
 * result_set.h
 *
 *  Created on: Mar 17, 2013
 *      Author: root
 */

#ifndef RESULT_SET_H_
#define RESULT_SET_H_

#include <vector>
#include <string>

class ResultSet {
 public:
  ResultSet()
      : current(0) {
  }

  void addRow(const std::vector<std::string>& row) {
    resultSet.push_back(row);
  }

  bool fetch(size_t field, std::string& fieldValue) {
    size_t sz = resultSet.size();

    if (sz) {
      if (sz > current) {
        fieldValue = resultSet[current++][field];
        return true;
      }
    }

    current = 0;
    return false;
  }

  bool fetch(std::vector<std::string>& rowValue) {
    size_t sz = resultSet.size();
    if (sz) {
      if (sz > current) {
        rowValue = resultSet[current++];
        return true;
      }
    }

    current = 0;
    return false;
  }

  void clean() {
    resultSet.clear();
  }

  std::string get(size_t row, size_t field) {
    return resultSet[row][field];
  }

  std::vector<std::string> get(size_t row) {
    return resultSet[row];
  }

  size_t countRows() {
    if (resultSet.empty())
      return 0;
    return resultSet.size();
  }

  size_t countFields() {
    if (resultSet[0].empty())
      return 0;
    return resultSet[0].size();
  }

 private:
  std::vector<std::vector<std::string> > resultSet;
  size_t current;
};

#endif /* RESULT_SET_H_ */
