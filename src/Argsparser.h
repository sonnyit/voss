//
// Argsparser.h
//
//  Created on: May 20, 2013
//      Author: tanpd
//

#ifndef ARGSPARSER_H_
#define ARGSPARSER_H_

#include <vector>
#include <string>

/*!\class Args_parser
 * \brief read args when run in console
 *
 * \author tanpd
 */

class Args_parser {
 public:
  Args_parser(int argc, char* argv[]);
  inline std::string get_stat_day();
  inline std::string get_start_day();
  inline std::string get_end_day();
  inline std::string get_stat_time();
  inline std::string get_usage();
  inline std::string get_src_server();
  inline std::string get_dest_server();
  inline std::string get_max_server_src_id();
  inline bool has_help();
  inline bool has_range();
  inline bool has_day();
  inline bool has_merge();
  ~Args_parser();

 private:
  bool d_help;
  bool d_range;
  bool d_day;
  bool d_merge;
  std::string d_stat_time;
  std::string d_stat_time_end;
  std::string d_prog_name;
  std::string server_src;
  std::string server_dest;
  std::string max_server_src_id;
};

//TODO : should inlined these functions
std::string Args_parser::get_stat_time() {
  return d_stat_time;
}

std::string Args_parser::get_stat_day() {
  return d_stat_time;
}

std::string Args_parser::get_start_day() {
  return d_stat_time;
}

std::string Args_parser::get_end_day() {
  return d_stat_time_end;
}

std::string Args_parser::get_src_server() {
  return server_src;
}

std::string Args_parser::get_dest_server() {
  return server_dest;
}

std::string Args_parser::get_max_server_src_id() {
  return max_server_src_id;
}

bool Args_parser::has_help() {
  return d_help;
}

bool Args_parser::has_range() {
  return d_range;
}

bool Args_parser::has_day() {
  return d_day;
}

bool Args_parser::has_merge() {
  return d_merge;
}

std::string Args_parser::get_usage() {
  return std::string(
      "Usage : " + d_prog_name
          + " ( -h | --help | -d day | -r start_day end_day )\n"
              "      -h, --help            : Show this message\n"
              "      -d day                : Run statistic for date <day>\n"
              "      -r start_day end_day  : Run statistic for date from <start_day> to <end_day>\n"
              "      -m merge server       : Merge server from <server_src> to <server_dest> with <max_server_src_id>\n");
}

#endif // ARGSPARSER_H_
