/*
 * main.cpp
 *
 *  Created on: Feb 25, 2013
 *      Author: minhnh3
 */

#ifndef LOGGING_LEVEL_1
#define LOGGING_LEVEL_1
#endif

#include "user.h"
#include "depositor.h"
#include "utils.h"
#include "Argsparser.h"
#include "logger.h"
#include "redis.h"

#include "boost/date_time/gregorian/gregorian.hpp"
#include <iostream>

using namespace boost::gregorian;
using namespace std;

int main(int argc, char* argv[]) {
  User user;
  Depositor depositor;
  try {
    Args_parser args(argc, argv);
    std::string start_day("");
    int range = 0;
    if (args.has_help()) {
      cout << args.get_usage();
      exit(0);
    } else if (args.has_range()) {
      start_day = args.get_start_day();
      range = Utils::get_day_diff(args.get_start_day(), args.get_end_day()) + 1;
    } else if (args.has_day()) {
      start_day = args.get_start_day();
      range = 1;
    }

    for (int i = 0; i < range; i++) {
      std::string stat_day = Utils::shift_date(start_day, i, false);
      user.import_registered(stat_day, INDEX::USER);
    }
  } catch (runtime_error &e) {
    LOG_ERR(e.what());
    cerr << "We got an error : " << e.what() << "\nSee ya!\n";
  } catch (...) {
    LOG_ERR("UNKNOWN ERROR");
    cerr << "We got trouble. Unknown Error. CODE RED!\n";
    throw;
  }
}

