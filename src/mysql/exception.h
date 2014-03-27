/*
 * exception.h
 *
 *  Created on: Mar 17, 2013
 *      Author: root
 */

#ifndef EXCEPTION_H_
#define EXCEPTION_H_

#include <stdexcept>
#include <string>

class DataBaseError : public std::runtime_error {
 public:
  DataBaseError(const std::string& message)
      : std::runtime_error(message) {
  }
  ;
};

#endif /* EXCEPTION_H_ */
