/*
 * redis.h
 *
 *  Created on: Apr 25, 2013
 *      Author: minhnh3
 */

#ifndef REDIS_H_
#define REDIS_H_

#include "redis/hiredis.h"
#include "def.h"

#include <stdexcept>
#include <string>
#include <vector>

/*!\class Redis
 * \brief manage communication to redis database.
 */
class Redis {
 private:
  std::string host;
  unsigned port;
  std::string password;

  static redisContext* context;
  static bool destroyed;
  static unsigned commanded;

 private:
  Redis();
  ~Redis();
  Redis(Redis const&);      // don't implement
  void operator=(Redis const&);  // don't implement

 public:
  static redisReply* reply;

 public:
  static Redis& get_instance();
  //!\brief Tell redis delete a key, using pipeline
  static void del_append_comm(const std::string& key);
  //!\brief Particular for index resident user.
  static void get_and_free_resident_reply(
      ResultTableUseCerberus<TableResidentUserLvDis>::Type& result_table,
      unsigned isoDateReg, unsigned isoDateStat);
  //!\brief Choose redis database.
  static void select_database(unsigned index);
  //!\brief Check a key is exists?
  static bool exists(const std::string& key_name);

  //!\brief Increase uuid key
  static std::string incr_comm(const std::string& key);
  static void incr_append_comm(const std::string& key);

  //!\brief search
  static void search_keys_comm(const std::string& key_template);

  //==============================  get reply  =====================================
  //!\brief Free reply be created by pipeline before.
  static void free_replies();
  //!\brief Free a reply.
  static void free_reply();
  //!\brief Store replies in vector and free them
  static void get_and_free_replies(std::vector<std::string>& store_replies);
  static void get_and_free_incr_replies(std::vector<unsigned>& store_replies);
  static void get_and_free_non_array_replies(std::vector<std::string>& store_replies);
  static const std::string pop_string_reply();
  static void get_and_free_merge_server(ResultTable<std::string>::Type& store);
  static void pop_strings_reply(std::vector<std::string>&);
  static void store_and_free_reply(std::vector<std::string>&);

  //==============================  Strings Type ===================================

  static void setbit_append_comm(const std::string& key,
                                 const std::string& offset);
  static void setbit_comm(const std::string& key, const std::string& offset);
  static void getbit_comm(const std::string& key, const std::string& offset);
  static void getbit_append_comm(const std::string& key,
                                 const std::string& offset);
  static void bitcount_comm(const std::string& key);
  static void bitop_comm(const std::string& operation,
                         const std::string& dest_key, const std::string& keys);
  static void bitop_append_comm(const std::string& operation,
                                const std::string& dest_key,
                                const std::string& keys);
  static void get_comm(const std::string& key);
  static void strlen_comm(const std::string& key);

  //==============================  Sorted sets Type ===================================

  static void zadd_comm(const std::string& key, const std::string& score,
                        const std::string& value);
  static void zadd_append_comm(const std::string& key, const std::string& score,
                               const std::string& value);
  static void zrangebyscore_comm(const std::string& key, const std::string& min,
                                 const std::string& max,
                                 const std::string& option);
  static void zrangebyscore_append_comm(const std::string& key,
                                        const std::string& min,
                                        const std::string& max,
                                        const std::string& option);
  static void zrevrangebyscore_comm(const std::string& key,
                                    const std::string& max,
                                    const std::string& min,
                                    const std::string& option);
  static void zrevrangebyscore_append_comm(const std::string& key,
                                           const std::string& max,
                                           const std::string& min,
                                           const std::string& option);

  static void zremrangebyscore_append_comm(const std::string& key,
                                           const std::string& min,
                                           const std::string& max);

  //==============================  Hash Type ===================================

  static void hget_comm(const std::string& key, const std::string& field);
  static void hget_append_comm(const std::string& key, const std::string& field);
  static void hget_all_comm(const std::string& key);
  static void hgetall_append_comm(const std::string& key);
  static void hset_comm(const std::string& key, const std::string& field,
                        const std::string& value);
  static void hset_append_comm(const std::string& key, const std::string& field,
                               const std::string& value);
};

#endif /* REDIS_H_ */
