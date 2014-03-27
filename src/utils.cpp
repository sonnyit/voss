/*
 * utils.cpp
 *
 *  Created on: Mar 19, 2013
 *      Author: minhnh3
 */

#include "utils.h"

#include <iostream>
#include <bitset>
#include <fstream>

using namespace std;
using namespace boost::gregorian;

void Utils::create_bm_of_period(const string& index, const string& minISODate,
    const string& maxISODate,
    const string& dest_key) {
  Redis::zrangebyscore_comm(ZSET::STORE_BITMAP + index, minISODate, maxISODate,
      "");

  string keys = "";
  for (size_t i = 0; i < Redis::reply->elements; ++i) {
    keys += string(Redis::reply->element[i]->str) + " ";
  }
  Redis::free_reply();

  Redis::bitop_comm("OR", dest_key, keys);
  Redis::free_reply();
}

void Utils::get_ids_from_bitmap(vector<unsigned>& ids, char* ptr_bm,
    unsigned bm_size) {
  char a;
  for (unsigned i = 0; i < bm_size; ++i) {
    a = *(ptr_bm + i);
    bitset<8> mybits(a);
    if (mybits.any()) {
      for (size_t j = 0; j < mybits.size(); ++j) {
        if (mybits.test(j)) {
          ids.push_back(i * 8 + (7 - j));
        }
      }
    }
  }
}

void Utils::get_account_from_bitmap(std::vector<std::string>& account_names,
    char* ptr_bm,
    unsigned bm_size, std::string file_name) {
  char a;
  vector<unsigned> ids;
  for (unsigned i = 0; i < bm_size; ++i) {
    a = *(ptr_bm + i);
    bitset<8> mybits(a);
    if (mybits.any()) {
      for (size_t j = 0; j < mybits.size(); ++j) {
        if (mybits.test(j)) {
          ids.push_back(i * 8 + (7 - j));
        }
      }
    }
  }
  Redis::free_reply();

  for (unsigned i = 0; i < ids.size(); ++i) {
    Redis::hget_append_comm(CONSTANT_KEY::RHASH + Utils::convert<unsigned, string>(ids[i]/1000), 
        Utils::convert<unsigned, string>(ids[i]));
  }

  //-- replies
  Redis::get_and_free_non_array_replies(account_names);

  ofstream outf(file_name, ios::trunc);

  for (unsigned i = 0; i < account_names.size(); ++i) {
    outf << account_names[i] << "\n";
  }
  outf.close();
}

void Utils::get_account_from_bitmap(std::vector<std::string>& account_names, 
    const std::string& bitmap_name, 
    const std::string& file_name,
    const bool& to_file) {

  unsigned bm_size = 0; 
  Redis::strlen_comm(bitmap_name);
  bm_size = Redis::reply->integer;
  Redis::free_reply();

  Redis::get_comm(bitmap_name);

  char* ptr_bm = Redis::reply->str;
  char a;
  vector<unsigned> ids;
  for (unsigned i = 0; i < bm_size; ++i) {
    a = *(ptr_bm + i);
    bitset<8> mybits(a);
    if (mybits.any()) {
      for (size_t j = 0; j < mybits.size(); ++j) {
        if (mybits.test(j)) {
          ids.push_back(i * 8 + (7 - j));
        }
      }
    }
  }
  Redis::free_reply();

  for (unsigned i = 0; i < ids.size(); ++i) {
    Redis::hget_append_comm(CONSTANT_KEY::RHASH + Utils::convert<unsigned, string>(ids[i]/1000), 
        Utils::convert<unsigned, string>(ids[i]));
  }

  //-- replies
  Redis::get_and_free_non_array_replies(account_names);

  if (to_file) {
    ofstream outf(file_name, ios::trunc);

    for (unsigned i = 0; i < account_names.size(); ++i) {
      outf << account_names[i] << "\n";
    }
    outf.close();
  }
}

void Utils::set_login_to_user_bitmap(std::map<unsigned, LoginBitmap>& active_user_of_month, const std::string& key_redis_name, 
    const unsigned& nth) {
  unsigned bm_size = 0; 

  Redis::strlen_comm(key_redis_name);
  bm_size = Redis::reply->integer;
  Redis::free_reply();

  Redis::get_comm(key_redis_name);

  char a;
  char* ptr_bm = Redis::reply->str;
  //vector<unsigned> ids;
  for (unsigned i = 0; i < bm_size; ++i) {
    a = *(ptr_bm + i); 
    bitset<8> mybits(a);
    if (mybits.any()) {
      for (size_t j = 0; j < mybits.size(); ++j) {
        if (mybits.test(j)) {
          //ids.push_back(i * 8 + (7 - j));
          active_user_of_month[i * 8 + (7 - j)].add_dayth(nth);
        }
      }   
    }   
  }
  Redis::free_reply();

  /*
     vector<std::string> account_names;
     for (unsigned i = 0; i < ids.size(); ++i) {
     Redis::hget_append_comm(CONSTANT_KEY::RHASH + Utils::convert<unsigned, string>(ids[i]/1000), Utils::convert<unsigned, string>(ids[i]));
     }
     Redis::get_and_free_non_array_replies(account_names);

     for (unsigned i = 0; i < account_names.size(); ++i) {
     active_user_of_month[account_names[i]].add_dayth(nth);
     }
     */
}

void Utils::get_ips_from_ids(const std::string& index, vector<unsigned>& ids,
    vector<unsigned>& ips,
    const string& str_iso_date) {
  vector<string> store_replies;
  store_replies.clear();

  for (unsigned i = 0; i < ids.size(); ++i) {
    Redis::zrangebyscore_append_comm(
        index + ":" + Utils::convert<unsigned, string>(ids[i]), str_iso_date,
        "+inf", "limit 0 1");
  }
  Redis::get_and_free_replies(store_replies);

  vector<unsigned> store;
  store.clear();
  for (unsigned i = 0; i < store_replies.size(); ++i) {
    Utils::split(store_replies[i], store);
    ips.push_back(store[3]);
    store.clear();
  }
}

void Utils::get_info_from_bitmap(const string& bm_name,
    vector<string>& store_relies,
    const string& str_iso_date_begin,
    const string& str_iso_date_end,
    const string& index) {
  unsigned bm_size = 0;
  vector<unsigned> ids;
  // take size
  Redis::strlen_comm(bm_name);
  bm_size = Redis::reply->integer;
  Redis::free_reply();

  Redis::get_comm(bm_name);
  Utils::get_ids_from_bitmap(ids, Redis::reply->str, bm_size);
  Redis::free_reply();

  for (unsigned i = 0; i < ids.size(); ++i) {
    Redis::zrevrangebyscore_append_comm(
        index + ":" + Utils::convert<unsigned, string>(ids[i]),
        str_iso_date_end, str_iso_date_begin, "");
  }
  Redis::get_and_free_replies(store_relies);
}

string Utils::build_name_key(const string& topic, const string& str) {
  return topic + str;
}

const boost::gregorian::date Utils::convert_to_date(const std::string & day) {
  using boost::gregorian::from_undelimited_string;
  using boost::gregorian::from_simple_string;
  using boost::gregorian::from_string;
  using boost::gregorian::from_uk_string;
  using boost::gregorian::from_us_string;

  try {
    return from_undelimited_string(day);
  } catch (...) {
  }

  try {
    return from_simple_string(day);
  } catch (...) {
  }

  try {
    return from_string(day);
  } catch (...) {
  }

  try {
    return from_uk_string(day);
  } catch (...) {
  }

  try {
    return from_us_string(day);
  } catch (...) {
  }

  return boost::gregorian::date();
}

std::string Utils::shift_date(const std::string& str_iso_date,
    const unsigned& number, const bool& isBefore) {
  using namespace boost::gregorian;
  if (isBefore)
    return to_iso_string(convert_to_date(str_iso_date) - days(number));
  else
    return to_iso_string(convert_to_date(str_iso_date) + days(number));
}

std::string Utils::get_uuid(const std::string& role,
    const std::string& server) {
  std::string key = build_key(DATA_TYPE::HASH, server);
  Redis::hget_comm(key, role);
  if (Redis::reply->type == REDIS_REPLY_NIL)
    return "NIL";

  return Redis::reply->str;
}

std::string Utils::set_uuid(const string& role, const string& server) {
  //transaction
  std::string uuid = Redis::incr_comm(CONSTANT_KEY::UUID);
  Redis::free_reply();

  std::string key = build_key(DATA_TYPE::HASH, server);
  Redis::hset_comm(key, role, uuid);
  Redis::free_reply();

  return uuid;
}

void Utils::find_login_location(
    const unsigned& ip, const unsigned& server,
    ResultTable<TableDayLoginRegionDis>::Type& result_table) {
  Redis::zrangebyscore_comm(CONSTANT_KEY::IP2LOC,
      Utils::convert<unsigned, string>(ip), "+inf",
      "limit 0 1");
  if (Redis::reply->elements > 0) {
    unsigned province = Utils::convert<string, unsigned>(
        Redis::reply->element[0]->str);
    Redis::free_reply();

    if (result_table.find(Keys(server, province)) == result_table.end()) {
      result_table.insert(
          pair<Keys, TableDayLoginRegionDis>(Keys(server, province),
            TableDayLoginRegionDis()));
    }
    result_table[Keys(server, province)].increase();
  } else {
    Redis::free_reply();
  }
}

unsigned Utils::murmur2a(const std::string & str) {
  static const unsigned int RANGE = 24;
  static const unsigned int SEED = 2166136261U;
  static const unsigned int MULTIPLIER = 0x5bd1e995;

  //init 
  unsigned int result = SEED;

  //update
  {
    unsigned int mmh2ak;
    size_t len = str.length();
    for (unsigned i = 0; i < len; i++) {
      mmh2ak = (str[i]) * MULTIPLIER;
      mmh2ak ^= mmh2ak >> RANGE;
      mmh2ak *= MULTIPLIER;
      result *= MULTIPLIER;
      result ^= mmh2ak;
    }
  }

  //finish
  result ^= result >> 13;
  result *= MULTIPLIER;
  result ^= result >> 15;
  return result;
}

void Utils::create_bitmap_by_list(const std::vector<unsigned>& ids, const std::string& bitmap_name) {
  for (unsigned i = 0; i < ids.size(); ++i) {
    Redis::setbit_comm(bitmap_name, convert<unsigned, std::string>(ids[i]));
    Redis::free_reply();
  }
}

void Utils::create_bm_reg_in(const std::string& str_iso_start_date, const std::string& str_iso_end_date, const std::string& dest_key) {
  string bm_reg_to_before_start_date = "bm_reg_to_before_start_dateasdlfjoieds";
  string bm_login_between_start_end = "bm_login_between_start_endojnvyugtrs";
  string bm_reg_to_end_date = "bm_reg_to_end_dateiuhfyv";

  Utils::create_bm_of_period(INDEX::USER, "-inf", to_iso_string(convert_to_date(str_iso_start_date) - days(1)), bm_reg_to_before_start_date);
  Utils::create_bm_of_period(INDEX::USER, str_iso_start_date, str_iso_end_date, bm_login_between_start_end);
  Redis::bitop_append_comm("OR", bm_reg_to_end_date, bm_reg_to_before_start_date + " " + bm_login_between_start_end);
  Redis::bitop_append_comm("XOR", dest_key, bm_reg_to_end_date + " " + bm_reg_to_before_start_date);

  Redis::del_append_comm(bm_reg_to_before_start_date);
  Redis::del_append_comm(bm_login_between_start_end);
  Redis::del_append_comm(bm_reg_to_end_date);
  Redis::free_replies();
}

