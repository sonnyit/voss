//
// Argsparser.cpp
//
//  Created on: May 20, 2013
//      Author: tanpd
//

#include "Argsparser.h"
#include <exception>
#include <stdexcept>
#include "utils.h"

Args_parser::Args_parser(int argc, char* argv[])
    : d_prog_name(argv[0]) {
  d_help = false;
  d_range = false;
  d_day = false;
  d_stat_time = "";
  d_stat_time_end = "";
  server_src = "";
  server_dest = "";
  max_server_src_id = "";

  std::vector<std::string> tmp(argv, argv + argc);
  int i = 1;
  int end = tmp.size();
  while (i < end) {
    if (tmp.at(i).compare("-d") == 0) {
      ++i;
      d_stat_time = tmp.at(i);
      d_day = true;
    } else if (tmp.at(i).compare("-t") == 0) {
      ++i;
      d_stat_time = tmp.at(i);
    } else if (tmp.at(i).compare("-r") == 0) {
      ++i;
      d_stat_time = tmp.at(i);
      ++i;
      d_stat_time_end = tmp.at(i);
      d_range = true;
    } else if (tmp.at(i).compare("-h") == 0
        || tmp.at(i).compare("--help") == 0) {
      d_help = true;
    } else if (tmp.at(i).compare("-m") == 0) {
      ++i;
      server_src = tmp.at(i);
      ++i;
      server_dest = tmp.at(i);
      ++i;
      max_server_src_id = tmp.at(i);
      d_merge = true;
    } else {
      throw(std::runtime_error("Unknown argument : " + tmp[i]));
    }
    ++i;
  }
}

Args_parser::~Args_parser() {
}

