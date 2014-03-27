/*
 * log.cpp
 *
 *  Created on: May 24, 2013
 *      Author: minhnh3
 */

#include "log.h"
#include <iostream>

void logging::file_log_policy::open_ostream(const std::string& name) {
  out_stream->open(
      name.c_str(),
      std::ios_base::binary | std::ios_base::out | std::ios_base::app);
  if (!out_stream->is_open()) {
    throw(std::runtime_error("LOGGER: Unable to open an output stream"));
  }
}

void logging::file_log_policy::close_ostream() {
  if (out_stream) {
    out_stream->close();
  }
}

void logging::file_log_policy::write(const std::string& msg) {
  (*out_stream) << msg << std::endl;
}

logging::file_log_policy::~file_log_policy() {
  if (out_stream) {
    close_ostream();
  }
}

