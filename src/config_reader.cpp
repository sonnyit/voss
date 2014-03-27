/*
 * config_reader.cpp
 *
 *  Created on: May 10, 2013
 *      Author: minhnh3
 */

#include "config_reader.h"
#include "utils.h"

#include <boost/foreach.hpp>
#include <boost/property_tree/xml_parser.hpp>
#include <string>

using namespace std;

bool ConfigReader::destroyed_ = false;
boost::property_tree::ptree ConfigReader::property_tree;

ConfigReader::ConfigReader() {
  using boost::property_tree::xml_parser::read_xml;
  // try open config file:q

  // throw exception
  read_xml("config.xml", property_tree);
}

ConfigReader::~ConfigReader() {
  destroyed_ = true;
}

ConfigReader& ConfigReader::get_instance() {
  static ConfigReader instance;
  if (destroyed_)
    throw std::runtime_error("QueryBuilder: Dead Reference Access");
  else
    return instance;
}

const std::string ConfigReader::build_query(const string& index,
                                            const std::string & str_iso_date) {

  using boost::property_tree::ptree;

  get_instance();
  ptree index_node = property_tree.get_child("QUERY." + index);

  std::string query("SELECT ");
  BOOST_FOREACH(ptree::value_type& field, index_node.get_child("FIELDS.<xmlattr>")){
  query += field.second.data();
  query += " AS ";
  query += field.first.data();
  query += ", ";
}
//delete last comma (,)
  query.resize(query.length() - 2);

  // from table statement
  query += " FROM ";
  query += index_node.get<std::string>("<xmlattr>.TABLE");

  //where condition
  query += " WHERE ";
  query += index_node.get<std::string>("<xmlattr>.dtLogTime");
  query += ">='";
  query += Utils::get_begin_day(str_iso_date);
  query += "' AND ";
  query += index_node.get<std::string>("<xmlattr>.dtLogTime");
  query += "<='";
  query += Utils::get_end_day(str_iso_date);
  query += "'";

  //check for additional where conditions
  if (index_node.get<std::string>("CONDITION.<xmlattr>.WHERE").compare("")
      != 0) {
    query += " AND ";
    query += index_node.get<std::string>("CONDITION.<xmlattr>.WHERE");
  }

  //group by statement
  if (index_node.get<std::string>("CONDITION.<xmlattr>.GROUPBY").compare("")
      != 0) {
    query += " GROUP BY ";
    query += index_node.get<std::string>("CONDITION.<xmlattr>.GROUPBY");
  }

  query += ";";

  return query;
}

const std::string ConfigReader::build_query_for_import_registered(
    const string& index) {
  using boost::property_tree::ptree;

  get_instance();
  ptree index_node = property_tree.get_child("QUERY." + index);

  std::string query("SELECT ");
  BOOST_FOREACH(ptree::value_type& field, index_node.get_child("FIELDS.<xmlattr>")){
  query += field.second.data();
  query += " AS ";
  query += field.first.data();
  query += ", ";
}
//delete last comma (,)
  query.resize(query.length() - 2);

  // from table statement
  query += " FROM ";
  query += index_node.get<std::string>("<xmlattr>.TABLE");

  //where condition
  //check for additional where conditions
  if (index_node.get<std::string>("CONDITION.<xmlattr>.WHERE").compare("")
      != 0) {
    query += " WHERE ";
    query += index_node.get<std::string>("CONDITION.<xmlattr>.WHERE");
  }

  //group by statement
  if (index_node.get<std::string>("CONDITION.<xmlattr>.GROUPBY").compare("")
      != 0) {
    query += " GROUP BY ";
    query += index_node.get<std::string>("CONDITION.<xmlattr>.GROUPBY");
  }

  //limit statement
  if (index_node.get<std::string>("CONDITION.<xmlattr>.LIMIT").compare("")
      != 0) {
    query += " LIMIT ";
    query += index_node.get<std::string>("CONDITION.<xmlattr>.LIMIT");
  }

  query += ";";

  return query;
}

void ConfigReader::get_config_index_effective_user(unsigned& continuous,
                                                   unsigned& at_least,
                                                   unsigned& from_day_n) {
  using boost::property_tree::ptree;

  get_instance();
  ptree index_node = property_tree.get_child(
      "STATISTIC_PARAMETER.EFFECTIVE_USER");
  continuous = index_node.get("<xmlattr>.CONTINUOUS", 4);
  from_day_n = index_node.get("<xmlattr>.FROM_DAY_N", 5);
  at_least = index_node.get("<xmlattr>.AT_LEAST", 2);
}

void ConfigReader::get_db_config(std::string& url_src, std::string& url_result,
                                 std::string& user_name_src,
                                 std::string& user_name_result,
                                 std::string& pass_src,
                                 std::string& pass_result,
                                 std::string& db_name_src,
                                 std::string& db_name_result) {
  using boost::property_tree::ptree;

  get_instance();
  ptree index_node = property_tree.get_child("DB.SOURCE");
  url_src = index_node.get<string>("<xmlattr>.HOST");
  user_name_src = index_node.get<string>("<xmlattr>.ACCOUNT");
  pass_src = index_node.get<string>("<xmlattr>.PASSWORD");
  db_name_src = index_node.get<string>("<xmlattr>.NAME");

  index_node = property_tree.get_child("DB.DEST");
  url_result = index_node.get<string>("<xmlattr>.HOST");
  user_name_result = index_node.get<string>("<xmlattr>.ACCOUNT");
  pass_result = index_node.get<string>("<xmlattr>.PASSWORD");
  db_name_result = index_node.get<string>("<xmlattr>.NAME");
}

void ConfigReader::get_db_src_name(std::string& db_src_name) {
  using boost::property_tree::ptree;

  get_instance();
  ptree index_node = property_tree.get_child("DB.SOURCE");
  db_src_name = index_node.get<string>("<xmlattr>.NAME");
}

void ConfigReader::get_db_result_name(std::string& db_result_name) {
  using boost::property_tree::ptree;

  get_instance();
  ptree index_node = property_tree.get_child("DB.DEST");
  db_result_name = index_node.get<string>("<xmlattr>.NAME");
}

void ConfigReader::get_game_open_date(std::string& game_open_date) {
  using boost::property_tree::ptree;

  get_instance();
  ptree index_node = property_tree.get_child("GAME");
  game_open_date = index_node.get<string>("<xmlattr>.STARTDATE");
}

void ConfigReader::get_redis_server_config(string& host, unsigned& port,
                                           string& password) {
  using boost::property_tree::ptree;
  get_instance();
  ptree index_node = property_tree.get_child("DB.REDIS");
  host = index_node.get<string>("<xmlattr>.HOST");
  port = index_node.get<int>("<xmlattr>.PORT");
  password = index_node.get<string>("<xmlattr>.PASSWORD");
}

void ConfigReader::get_number_of_hash_server(unsigned& number) {
  using boost::property_tree::ptree;

  get_instance();
  ptree index_node = property_tree.get_child("STATISTIC_PARAMETER.HASH_SERVER");
  number = index_node.get<unsigned>("<xmlattr>.NUMBER", 30000);
}

void ConfigReader::get_gamepoint_seg_conf(string& segment) {
  using boost::property_tree::ptree;
  get_instance();
  ptree index_node = property_tree.get_child("STATISTIC_PARAMETER.GAMEPOINT");
  segment = index_node.get<string>("<xmlattr>.RANGE");
}

void ConfigReader::get_id_generation_type(string& type) {
  using boost::property_tree::ptree;
  get_instance();
  ptree index_node = property_tree.get_child(
      "STATISTIC_PARAMETER.ID_GENERATOR");
  type = index_node.get<string>("<xmlattr>.TYPE");
}

