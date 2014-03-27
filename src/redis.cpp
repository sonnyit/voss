/*
 * redis.cpp
 *
 *  Created on: Apr 25, 2013
 *      Author: minhnh3
 */

#ifndef LOGGING_LEVEL_1
#define LOGGING_LEVEL_1
#endif

#include "redis.h"
#include "logger.h"
#include "utils.h"
#include "config_reader.h"

#include "boost/date_time/gregorian/gregorian.hpp"
#include <iostream>
#include <cstdlib>

using namespace boost::gregorian;
using namespace std;

bool Redis::destroyed = false;
unsigned Redis::commanded = 0;
redisContext* Redis::context = redisConnect("127.0.0.1", 6379);
redisReply* Redis::reply = 0;

Redis::Redis() {
  commanded = 0;
  ConfigReader::get_redis_server_config(this->host, this->port, this->password);
  context = redisConnect((this->host).c_str(), this->port);
  reply = 0;

  LOG("Constructor of redis - ok");
  LOG("\t+ host: ", this->host);
  LOG("\t+ port: ", this->port);
  LOG("\t+ password: ", this->password);
}

Redis::~Redis() {
  destroyed = true;

  if (reply) {
    freeReplyObject(this->reply);
    this->reply = 0;
  }

  if (context)
    redisFree(context);

  LOG("Destructor of redis");
}

Redis& Redis::get_instance() {
  static Redis instance;
  if (destroyed)
    throw std::runtime_error("Dead reference access\n");
  else
    return instance;
}

void Redis::del_append_comm(const string& key) {
  get_instance();
  redisAppendCommand(context, ("DEL " + key).c_str());
  ++commanded;
}

void Redis::get_and_free_resident_reply(
    ResultTableUseCerberus<TableResidentUserLvDis>::Type& result_table,
    unsigned isoDateReg, unsigned isoDateStat) {
  unsigned dayth;

  vector<unsigned> store;
  while (commanded > 0) {
    --commanded;
    if (redisGetReply(context, (void**) &reply) != REDIS_OK) {
      LOG_ERR("REDIS_ERR - exit");
      exit(1);
    }

    Utils::split(Redis::reply->element[0]->str, store);

    dayth = (from_undelimited_string(Redis::reply->element[1]->str)
        - from_undelimited_string(Utils::convert<unsigned, string>(isoDateReg)))
        .days();

    if (result_table.find(Cerberus(isoDateReg, dayth, 0))
        == result_table.end()) {
      result_table.insert(
          pair<Cerberus, TableResidentUserLvDis>(
              Cerberus(isoDateReg, dayth, 0),
              TableResidentUserLvDis(
                  Utils::convert<unsigned, string>(isoDateReg),
                  Utils::convert<unsigned, string>(isoDateStat), dayth, 0, 1,
                  0)));
    } else
      result_table[Cerberus(isoDateReg, dayth, 0)].increase();

    if (result_table.find(Cerberus(isoDateReg, dayth, store[1]))
        == result_table.end()) {
      result_table.insert(
          pair<Cerberus, TableResidentUserLvDis>(
              Cerberus(isoDateReg, dayth, store[1]),
              TableResidentUserLvDis(
                  Utils::convert<unsigned, string>(isoDateReg),
                  Utils::convert<unsigned, string>(isoDateStat), dayth,
                  store[1], 1, 0)));
    } else
      result_table[Cerberus(isoDateReg, dayth, store[1])].increase();

    store.clear();
    freeReplyObject(reply);
    reply = 0;
  }
}

void Redis::select_database(unsigned index) {
  get_instance();
  reply = (redisReply*) redisCommand(
      context, ("SELECT " + Utils::convert<unsigned, string>(index)).c_str());
  ++commanded;
}

bool Redis::exists(const string& key_name) {
  bool result;
  get_instance();
  free_replies();
  reply = (redisReply*) redisCommand(context, ("EXISTS " + key_name).c_str());
  result = reply->integer;
  free_reply();
  return result;
}

std::string Redis::incr_comm(const std::string& key) {
  get_instance();
  reply = (redisReply*) redisCommand(context, ("INCR " + key).c_str());

  int uuid = reply->integer;
//  Redis::free_reply();
//
//  reply = (redisReply*) redisCommand(context, ("GET " + key).c_str());
  ++commanded;

  //return reply->str;
  return Utils::convert<int, std::string>(uuid);
}

void Redis::incr_append_comm(const std::string& key) {
  get_instance();
  redisAppendCommand(context, ("INCR " + key).c_str());
  ++commanded;
}

void Redis::search_keys_comm(const std::string& key_template) {
  get_instance();
  reply = (redisReply*) redisCommand(context, ("KEYS " + key_template).c_str());
  ++commanded;
}

//==============================  Get Reply  =====================================

void Redis::free_replies() {
  while (commanded > 0) {
    --commanded;
    if (redisGetReply(context, (void**) &reply) != REDIS_OK) {
      LOG_ERR("REDIS_ERR - exit");
      exit(1);
    }
    freeReplyObject(reply);
    reply = 0;
  }
}

void Redis::free_reply() {
  if (commanded > 0) {
    freeReplyObject(reply);
    reply = 0;
    --commanded;
    if (commanded > 0)
      LOG_WARN("REDIS: not syn reply");
  }
}
/*
void Redis::get_and_free_get_comm(vector<string>& store_replies) {
  string str = "";
  while (commanded > 0) {
    --commanded;
    if (redisGetReply(context, (void**) &reply) != REDIS_OK) {
      LOG_ERR("REDIS_ERR - exit");
      exit(1);
    }

    if (Redis::reply->elements > 0) {
      //str = Redis::reply->element[Redis::reply->elements - 1]->str;
      str = Redis::reply->element[0]->str;
      str += ":" + Utils::convert<size_t, string>(Redis::reply->elements);
      store_replies.push_back(str);
      str = "";
      
      store_replies.push_back(Redis);
    }
    freeReplyObject(reply);
    reply = 0;
  }
}
*/
void Redis::get_and_free_replies(vector<string>& store_replies) {
  string str = "";
  while (commanded > 0) {
    --commanded;
    if (redisGetReply(context, (void**) &reply) != REDIS_OK) {
      LOG_ERR("REDIS_ERR - exit");
      exit(1);
    }

    if (Redis::reply->elements > 0) {
      //str = Redis::reply->element[Redis::reply->elements - 1]->str;
      str = Redis::reply->element[0]->str;
      str += ":" + Utils::convert<size_t, string>(Redis::reply->elements);
      store_replies.push_back(str);
      str = "";
    }
    freeReplyObject(reply);
    reply = 0;
  }
}

void Redis::get_and_free_incr_replies(vector<unsigned>& store_replies) {
  while (commanded > 0) {
    --commanded;
    if (redisGetReply(context, (void**) &reply) != REDIS_OK) {
      LOG_ERR("REDIS_ERR - exit");
      exit(1);
    }

    store_replies.push_back(Redis::reply->integer);
    freeReplyObject(reply);
  }
}

void Redis::get_and_free_non_array_replies(std::vector<std::string>& store_replies) {
  string str = "";
  while (commanded > 0) {
    --commanded;
    if (redisGetReply(context, (void**) &reply) != REDIS_OK) {
      LOG_ERR("REDIS_ERR - exit");
      exit(1);
    }

    if (Redis::reply->type == REDIS_REPLY_NIL) {
      LOG_WARN("REDIS_REPLY_NIL");
    } else {
      store_replies.push_back(Redis::reply->str);
    }
    freeReplyObject(reply);
    reply = 0;
  }
}

void Redis::get_and_free_merge_server(ResultTable<string>::Type& store) {
  while (commanded > 0) {
    --commanded;
    if (redisGetReply(context, (void**) &reply) != REDIS_OK) {
      LOG_ERR("REDIS_ERR - exit");
      exit(1);
    }

    if (reply != 0 || reply->type == REDIS_REPLY_ARRAY) {
      for (size_t i = 0; i < reply->elements; i += 2) {
        store.insert(
            pair<Keys, string>(
                Keys(
                    Utils::convert<string, unsigned>(reply->element[i]->str),
                    Utils::convert<string, unsigned>(
                        reply->element[i + 1]->str)),
                ""));
      }

      freeReplyObject(reply);
      reply = 0;
    } else {
      LOG_WARN("REPLY IS NULL OR NOT AN ARRAY");
      return;
    }
  }
}

const string Redis::pop_string_reply() {
  string temp;

  if (reply != 0 && commanded == 1) {
    try {
      if (reply->type != REDIS_REPLY_STRING)
        throw "REPLY IS NOT A STRING";

      temp = Utils::convert<char*, string>(reply->str);
      freeReplyObject(reply);
      reply = 0;
      commanded = 0;
    } catch (string& status) {
      cout << status << endl;
      exit(1);
    }
  } else {
    cout << "reply is null / not syn reply\n";
    exit(1);
  }
  return temp;
}

void Redis::pop_strings_reply(vector<string>& store_replies) {
  while (commanded > 0) {
    if (redisGetReply(context, (void**) &reply) != REDIS_OK) {
      cout << "REDIS_ERR\n";
      exit(1);
    }

    if (reply == 0 || reply->type == REDIS_REPLY_ARRAY) {

      for (size_t i = 0; i < reply->elements; ++i) {
        store_replies.push_back(
            Utils::convert<char*, string>(reply->element[i]->str));
      }

      freeReplyObject(reply);
      reply = 0;
      --commanded;
    } else {
      cout << "REPLY IS NULL OR NOT AN ARRAY\n";
      return;
    }
  }
}

void Redis::store_and_free_reply(vector<string>& store_replies) {
  while (commanded > 0) {
    --commanded;
    // if (redisGetReply(context, (void**) &reply) != REDIS_OK) {
    // LOG_ERR("REDIS_ERR - exit");
    // exit(1);
    // }
    redisGetReply(context, (void**) &reply);
    if (reply == NULL)
      LOG("REDIS NULL");

    //if (reply != 0 || reply->type == REDIS_REPLY_ARRAY) {

    for (size_t i = 0; i < reply->elements; ++i) {
      LOG(reply->element[i]->str);
      store_replies.push_back(
          Utils::convert<char*, string>(reply->element[i]->str));
    }

    freeReplyObject(reply);
    reply = 0;
    // } else {
    // LOG_WARN("REPLY IS NULL OR NOT AN ARRAY");
    // return;
    // }
  }
}

//==============================  Strings Type ===================================

void Redis::setbit_append_comm(const string& key, const string& offset) {
  get_instance();
  redisAppendCommand(context, ("SETBIT " + key + " " + offset + " 1").c_str());
  ++commanded;
}

void Redis::setbit_comm(const string& key, const string& offset) {
  get_instance();
  reply = (redisReply*) redisCommand(
      context, ("SETBIT " + key + " " + offset + " 1").c_str());
  ++commanded;
}

void Redis::getbit_comm(const string& key, const string& offset) {
  get_instance();
  reply = (redisReply*) redisCommand(context,
                                     ("GETBIT " + key + " " + offset).c_str());
  ++commanded;
}

void Redis::getbit_append_comm(const string& key, const string& offset) {
  get_instance();
  redisAppendCommand(context, ("GETBIT " + key + " " + offset).c_str());
  ++commanded;
}

void Redis::bitcount_comm(const string& key) {
  get_instance();
  reply = (redisReply*) redisCommand(context, ("BITCOUNT " + key).c_str());
  ++commanded;
}

void Redis::bitop_comm(const string& operation, const string& dest_key,
                       const string& keys) {
  get_instance();
  reply = (redisReply*) redisCommand(
      context, ("BITOP " + operation + " " + dest_key + " " + keys).c_str());
  ++commanded;
}

void Redis::bitop_append_comm(const string& operation, const string& dest_key,
                              const string& keys) {
  get_instance();
  redisAppendCommand(
      context, ("BITOP " + operation + " " + dest_key + " " + keys).c_str());
  ++commanded;
}

void Redis::get_comm(const string& key) {
  get_instance();
  reply = (redisReply*) redisCommand(context, ("GET " + key).c_str());
  ++commanded;
}

void Redis::strlen_comm(const string& key) {
  get_instance();
  reply = (redisReply*) redisCommand(context, ("STRLEN " + key).c_str());
  ++commanded;
}

//==============================  Sorted sets Type ===================================

void Redis::zadd_comm(const string& key, const string& score,
                      const string& value) {
  get_instance();
  reply = (redisReply*) redisCommand(
      context, ("ZADD " + key + " " + score + " " + value).c_str());
  ++commanded;
}

void Redis::zadd_append_comm(const string& key, const string& score,
                             const string& value) {
  get_instance();
  redisAppendCommand(context,
                     ("ZADD " + key + " " + score + " " + value).c_str());
  ++commanded;
}

void Redis::zrangebyscore_comm(const string& key, const string& min,
                               const string& max, const string& option) {
  get_instance();
  reply = (redisReply*) redisCommand(
      context,
      ("ZRANGEBYSCORE " + key + " " + min + " " + max + " " + option).c_str());
  ++commanded;
}

void Redis::zrangebyscore_append_comm(const string& key, const string& min,
                                      const string& max, const string& option) {
  get_instance();
  redisAppendCommand(
      context,
      ("ZRANGEBYSCORE " + key + " " + min + " " + max + " " + option).c_str());
  ++commanded;
}

void Redis::zrevrangebyscore_comm(const string& key, const string& max,
                                  const string& min, const string& option) {
  get_instance();
  reply =
      (redisReply*) redisCommand(
          context,
          ("ZREVRANGEBYSCORE " + key + " " + max + " " + min + " " + option)
              .c_str());
  ++commanded;
}

void Redis::zrevrangebyscore_append_comm(const string& key, const string& max,
                                         const string& min,
                                         const string& option) {
  get_instance();
  redisAppendCommand(
      context,
      ("ZREVRANGEBYSCORE " + key + " " + max + " " + min + " " + option).c_str());
  ++commanded;
}

void Redis::zremrangebyscore_append_comm(const std::string& key,
                                         const std::string& min,
                                         const std::string& max) {
  get_instance();
  // redisAppendCommand(
  // context,
  // ("ZREMRANGEBYSCORE " + key + " " + min + " " + max).c_str());
  redisAppendCommand(context, "ZREMRANGEBYSCORE %b %b %b", key.c_str(),
                     (size_t) key.length(), min.c_str(), (size_t) min.length(),
                     max.c_str(), (size_t) max.length());
  ++commanded;
}
//==============================  Hash Type ===================================

void Redis::hget_comm(const string& key, const string& field) {
  get_instance();
  // reply = (redisReply*) redisCommand(context,
  // ("HGET " + key + " " + field).c_str());
  reply = (redisReply*) redisCommand(context, "HGET %b %b", key.c_str(),
                                     (size_t) key.length(), field.c_str(),
                                     (size_t) field.length());
  ++commanded;
}

void Redis::hget_append_comm(const string& key, const string& field) {
  get_instance();
  // reply = (redisReply*) redisCommand(context,
  // ("HGET " + key + " " + field).c_str());
  redisAppendCommand(context, "HGET %b %b", key.c_str(),
                                     (size_t) key.length(), field.c_str(),
                                     (size_t) field.length());
  ++commanded;
}

void Redis::hget_all_comm(const std::string& key) {
  get_instance();
  // reply = (redisReply*) redisCommand(context,
  // ("HGETALL " + key).c_str());
  reply = (redisReply*) redisCommand(context, "HGETALL %b", key.c_str(),
                                     (size_t) key.length());
  ++commanded;
}

void Redis::hgetall_append_comm(const std::string& key) {
  get_instance();
  // redisAppendCommand(context,("HGETALL " + key).c_str());
  redisAppendCommand(context, "HGETALL %b", key.c_str(), (size_t) key.length());
  ++commanded;
}

void Redis::hset_comm(const std::string& key, const std::string& field,
                      const std::string& value) {
  get_instance();
  // reply = (redisReply*) redisCommand(
  // context, ("HSET " + key + " " + field + " " + value).c_str());
  reply = (redisReply*) redisCommand(context, "HSET %b %b %b", key.c_str(),
                                     (size_t) key.length(), field.c_str(),
                                     (size_t) field.length(), value.c_str(),
                                     (size_t) value.length());
  ++commanded;
}

void Redis::hset_append_comm(const std::string& key, const std::string& field,
                             const std::string& value) {
  get_instance();
  // redisAppendCommand(context,
  // ("HSET " + key + " " + field + " " + value).c_str());
  redisAppendCommand(context, "HSET %b %b %b", key.c_str(),
                     (size_t) key.length(), field.c_str(),
                     (size_t) field.length(), value.c_str(),
                     (size_t) value.length());
  ++commanded;
}

