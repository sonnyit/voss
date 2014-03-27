/*
 * config_reader.h
 *
 *  Created on: May 10, 2013
 *      Author: minhnh3
 */

#ifndef CONFIG_READER_H_
#define CONFIG_READER_H_

#include <boost/property_tree/ptree_fwd.hpp>
#include <boost/property_tree/ptree.hpp>
#include <string>

/*!\class ConfigReader
 * \brief This class read config for program from xml file.
 */
class ConfigReader {
 private:
  static boost::property_tree::ptree property_tree;
  static bool destroyed_;

 private:
  ConfigReader();
  ~ConfigReader();
  ConfigReader(const ConfigReader&);                    // don't implement
  ConfigReader& operator=(const ConfigReader&);         // don't implement

 public:
  static ConfigReader& get_instance();

  //!\brief Build sql query by info in file config and date.
  static const std::string build_query(const std::string& index,
                                       const std::string & str_iso_date);
  //!\brief Build sql query to import old registered user.
  static const std::string build_query_for_import_registered(
      const std::string& index);
  //!\brief Get config for the rule to find effective user.
  static void get_config_index_effective_user(unsigned& continuous,
                                              unsigned& at_least,
                                              unsigned& from_day_n);
  //!\brief Get date start game (open game).
  static void get_game_open_date(std::string& game_open_date);
  //!\brief Get info of connection to database (source and result).
  static void get_db_config(std::string& url_src, std::string& url_result,
                            std::string& user_name_src,
                            std::string& user_name_result,
                            std::string& pass_src, std::string& pass_result,
                            std::string& db_name_src,
                            std::string& db_name_result);

  static void get_db_src_name(std::string& db_src_name);
  static void get_db_result_name(std::string& db_result_name);
  //!\brief Get redis server connection config
  static void get_redis_server_config(std::string& host, unsigned& port,
                                      std::string& password);
  //!\bried Get number of hash server
  static void get_number_of_hash_server(unsigned& number);
  //!\brief Get segment gamepoint
  static void get_gamepoint_seg_conf(std::string& segment);
  //!\brief Get id generation type
  static void get_id_generation_type(std::string& type);
};

#endif /* CONFIG_READER_H_ */
