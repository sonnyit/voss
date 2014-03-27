/*
 * base.cpp
 *
 *  Created on: May 12, 2013
 *      Author: root
 */

#ifndef LOGGING_LEVEL_1
#define LOGGING_LEVEL_1
#endif

#include "base.h"
#include "pools_manager.h"
#include "def.h"
#include "utils.h"
#include "redis.h"
#include "config_reader.h"
#include "logger.h"

#include <fstream>
#include <map>
#include <iostream>

using namespace std;

void Base::get_data(const string& str_iso_date, const string& index,
                    bool force) {

  LOG("\t<get data>");
  Utils::loadbar(1);
  string bitmap = Utils::build_key(DATA_TYPE::BITMAP, index, str_iso_date);

  if (!force && Redis::exists(bitmap)) {
    LOG("\t\t+ data of index ", index, " exists in redis databases - return");
    LOG("\t<done get data>");
    Utils::loadbar(100);
    cout << endl;
    return;
  }

  // execute query to get data from source database
  timestamp_t t0 = get_timestamp();
  Connection* conn = PoolsManager::get_instance().get_src_db_connection();
  boost::shared_ptr<sql::Statement> stmt(conn->createStatement());
  string db_src = "";
  ConfigReader::get_db_src_name(db_src);
  stmt->execute("use " + db_src);

  boost::shared_ptr<sql::ResultSet> res(
      stmt->executeQuery(ConfigReader::build_query(index, str_iso_date)));
  //ResultSetMetaData *res_meta = res->getMetaData();

  LOG("\t\t+ query done in source database in: ",
      (get_timestamp() - t0) / 1000000.0L, "s.");
  Utils::loadbar(50);

  t0 = get_timestamp();
  vector<string> row;
  unsigned NUMBER_OF_HASH = 0;
  ConfigReader::get_number_of_hash_server(NUMBER_OF_HASH);
  string id_generation_type = "";
  ConfigReader::get_id_generation_type(id_generation_type);

  string id = "";
  string user = "";
  string server = "";
  string server_hash = "";

  while (res->next()) {
    user = res->getString("iRoleId");
    Utils::to_lower(user);
    if (id_generation_type.compare("ACCOUNT NAME") == 0) {
      server_hash = Utils::convert<unsigned, string>(
          Utils::murmur2a(user) % NUMBER_OF_HASH);

    } else if (id_generation_type.compare("ROLE") == 0) {
      server_hash = res->getString("iServer") + ":"
          + Utils::convert<unsigned, string>(
              Utils::murmur2a(user) % NUMBER_OF_HASH);
    } else if (id_generation_type.compare("ACCOUNT ID") == 0) {
      server_hash = Utils::convert<unsigned, string>(
          res->getUInt("iRoleId") / 100000);
    }
    id = Utils::get_uuid(user, server_hash);
    Redis::free_reply();

    if (id == "NIL") {
      id = Utils::set_uuid(user, server_hash);
    }
    
    //------------------ revhash ----------------------
    
    if (id_generation_type.compare("ROLE") == 0) {
      server = res->getString("iServer");
      Redis::hset_comm(CONSTANT_KEY::RHASH + Utils::convert<unsigned, string>(Utils::convert<string, unsigned>(id)/1000),
                    id, user + ":" + server);
    } else {
      Redis::hset_comm(CONSTANT_KEY::RHASH + Utils::convert<unsigned, string>(Utils::convert<string, unsigned>(id)/1000),
                    id, user);
    }
    Redis::free_reply();
    //------------------------------------------

    Redis::setbit_comm(bitmap, id);
    Redis::free_reply();

    if (index == INDEX::USER) {
      row.push_back(res->getString("iServer"));
      row.push_back(res->getString("iLevel"));
      row.push_back(res->getString("iTimes"));
    } else if (index == INDEX::DEPOSIT) {
      row.push_back(res->getString("iMoney"));
      row.push_back(res->getString("iServer"));
      row.push_back(res->getString("iLevel"));
      row.push_back(res->getString("iTimes"));
    }

    if (Utils::verify_ip(res->getString("vIp").c_str())) {
      row.push_back(
          Utils::convert<unsigned, string>(
              Utils::iptou(res->getString("vIp").c_str())));
    } else
      row.push_back("0");
    row.push_back(str_iso_date);
    Redis::zadd_comm(index + ":" + id, str_iso_date,
                     Utils::build_value_to_input_redis(row));
    Redis::free_reply();

    row.clear();
  }
  Utils::loadbar(90);

  // update sorted set, which contain bitmaps of login users
  Redis::zadd_comm(ZSET::STORE_BITMAP + index, str_iso_date, bitmap);
  Redis::free_reply();

  PoolsManager::get_instance().release_src_db_connection(conn);
  LOG("\t\t+ input data to redis database done in: ",
      (get_timestamp() - t0) / 1000000.0L, "s.");
  Utils::loadbar(95);

  LOG("\t<done get data>");
  Utils::loadbar(100);
  cout << endl;
}

