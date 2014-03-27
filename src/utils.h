/*
 * utils.h
 *
 *  Created on: Mar 19, 2013
 *      Author: minhnh3
 */

#ifndef UTILS_H_
#define UTILS_H_

#ifndef LOGGING_LEVEL_1
#define LOGGING_LEVEL_1
#endif

#include "def.h"
#include "redis.h"
#include "logger.h"

#include <boost/asio.hpp>
#include <boost/lexical_cast.hpp>
#include <boost/unordered/unordered_map.hpp>
#include "boost/date_time/gregorian/gregorian.hpp"
#include <boost/algorithm/string.hpp>

#include <map>
#include <iomanip>
#include <string>
#include <unistd.h>
#include <sys/time.h>

/*!\namespace Utils
 * !\brief utility for application
 */
namespace Utils {

  //!\brief AND/OR many bitmap to one
  void create_bm_of_period(const std::string& index,
      const std::string& minISODate,
      const std::string& maxISODate,
      const std::string& dest_key);

  /*!\brief get a date before or after n day
   *
   * \param str_iso_date original date
   * \param number is n day
   * \isBefore before original date or after (true / false)
   */
  std::string shift_date(const std::string& str_iso_date, const unsigned& number,
      const bool& isBefore);

  //!\brief convert from F type to T type
  template<typename F, typename T>
    inline T convert(const F& input) {
      return boost::lexical_cast<T>(input);
    }

  //!\brief get string of begin day time
  inline const std::string get_begin_day(const std::string& str_iso_date) {
    return str_iso_date + "000000";
  }

  //!\brief get string of end day time
  inline const std::string get_end_day(const std::string& str_iso_date) {
    return str_iso_date + "235959";
  }

  /*!\brief build name of key in redis database
   *
   * \param topic is index
   * \param str for something like date .. to unique key
   */
  std::string build_name_key(const std::string& topic, const std::string& str);

  /*!\brief convert string to lowercase
  */
  inline void to_lower(std::string& str) {
    boost::algorithm::to_lower(str);
  }

  //!\brief get list ids from bitmap: get offset of all bit on (1) of a bitmap.
  void get_ids_from_bitmap(std::vector<unsigned>& ids, char* bitmap,
      unsigned bm_size);

  //!\brief get list ids from bitmap: get offset of all bit on (1) of a bitmap.
  void set_login_to_user_bitmap(std::map<unsigned, LoginBitmap>& active_user_of_month, const std::string& key_redis_name,
      const unsigned& n);

  //!\brief get list account name from bitmap: get offset of all bit on (1) of a bitmap.
  void get_account_from_bitmap(std::vector<std::string>& account_names, char* bitmap, unsigned bm_size, std::string file_name);

  //!\brief get list account name from bitmap
  void get_account_from_bitmap(std::vector<std::string>& account_names, 
      const std::string& bitmap_name, 
      const std::string& file_name = "out.txt",
      const bool& to_file = false);

  //!\brief get ips from ids
  void get_ips_from_ids(const std::string& index, std::vector<unsigned>& ids,
      std::vector<unsigned>& ips,
      const std::string& str_iso_date);

  //!\brief find location of an ip
  void find_login_location(
      const unsigned& ip, const unsigned& server,
      ResultTable<TableDayLoginRegionDis>::Type& result_table);
  /*!\brief get information about all user in a bitmap
   *
   * \param bmName name of bitmap be get more info
   */
  void get_info_from_bitmap(const std::string& bmName,
      std::vector<std::string>& info,
      const std::string& minISODate,
      const std::string& maxISODate,
      const std::string& index);

  /*!\brief build info of user/depositor as id, money, login time ... to a string
   *
   * \param info vector, which each member is a info of user/depositor
   */
  inline std::string build_value_to_input_redis(
      const std::vector<std::string>& info) {
    std::string temp = "";
    for (size_t i = 0; i < info.size(); ++i) {
      temp += info[i] + ":";
    }
    return temp.substr(0, temp.size() - 1);
  }

  /*!\brief split a string by delimiter
  */
  inline void split_by_delimiter(std::string line, 
      const std::string& delimiter, 
      std::vector<std::string>& result) {
    size_t pos = 0;
    while ((pos = line.find(delimiter)) != std::string::npos) {
      result.push_back(line.substr(0, pos));
      line.erase(0, pos + delimiter.length());
    }
    result.push_back(line);
  }

  /*!\brief split a string to fields of user/depositor info
   *
   * \param result each fields store as a member of vector
   */
  inline void split(const std::string& line, std::vector<unsigned>& result) {
    unsigned int num = 0;
    char c;
    for (std::string::size_type i = 0; i < line.size(); ++i) {
      c = line[i];
      if (c >= '0' && c <= '9') {
        num *= 10;
        num += c - '0';
      } else if (c == ':') {
        result.push_back(num);
        num = 0;
      } else {
        throw std::runtime_error("Utils::split : Invalid number format");
      }
    }
    result.push_back(num);
  }

  /*!\brief This function try all posible format.
   *
   * \author tanpd
   * \detail Is try catch safe? does it terminated program? minhnh3
   */
  const boost::gregorian::date convert_to_date(const std::string & day);

  //!\brief get number of different date between (end) and (begin)
  inline int get_day_diff(const std::string & begin, const std::string & end) {
    return (convert_to_date(end) - convert_to_date(begin)).days();
  }

  /*!\brief for visualization progress status in console when index run
   *
   * \param x number of ratio done
   * \param n number of total work
   * \param w width of progress bar
   */
  static inline void loadbar(unsigned x, unsigned n = 100, unsigned w = 50) {
    if (x < 0 || x > n)
      return;

    float ratio = x / (float) n;
    unsigned c = ratio * w;

    std::cout << std::setw(3) << (int) (ratio * 100) << "% [";
    for (unsigned i = 0; i < c; ++i)
      std::cout << "=";
    for (unsigned i = c; i < w; ++i)
      std::cout << " ";
    std::cout << "]\r" << std::flush;
  }

  inline std::string build_key(const std::string& data_type,
      const std::string& index,
      const std::string& date_time) {
    return data_type + ":" + index + ":" + date_time;
  }

  inline std::string build_key(const std::string& data_type,
      const std::string& order) {
    return data_type + ":" + order;
  }

  std::string get_uuid(const std::string& role, const std::string& server);

  std::string set_uuid(const std::string& role, const std::string& server);

  inline unsigned iptou(const char *ip) {
    unsigned a, b, c, d;
    unsigned val;
    sscanf(ip, "%d.%d.%d.%d", &a, &b, &c, &d);

    /* (first octet * 256�) + (second octet * 256�) + (third octet * 256) + (fourth octet) */
    val = (a * 16777216) + (b * 65536) + (c * 256) + d;
    return val;
  }

  inline bool verify_ip(const std::string& ip) {
    boost::system::error_code ec;
    boost::asio::ip::address::from_string(ip, ec);
    if (ec)
      return false;
    return true;
  }

  inline unsigned find_level(std::map<unsigned, unsigned>& level_score,
      unsigned score) {
    if (score == 0)
      return 1;
    for (std::map<unsigned, unsigned>::iterator i = level_score.begin();
        i != level_score.end(); ++i) {
      if (score < (*i).second) {
        return (*i).first - 1;
      }
    }
    return level_score.rbegin()->first;
  }

  unsigned int murmur2a(const std::string& str);

  inline unsigned find_gamepoint_seg(std::vector<unsigned>& segment_table,
      unsigned money) {
    for (unsigned i = 0; i < segment_table.size(); ++i) {
      if (money < segment_table[i]) {
        return segment_table[i - 1];
      }
    }
    return segment_table[segment_table.size() - 1];
  }

  void create_bitmap_by_list(const std::vector<unsigned>& ids, const std::string& bitmap_name);

  void create_bm_reg_in(const std::string& str_iso_start_date,
      const std::string& str_iso_end_date,
      const std::string& dest_key);

}
;

#endif /* UTILS_H_ */
