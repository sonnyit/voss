/*
 * user.cpp
 *
 *  Created on: May 9, 2013
 *      Author: minhnh3
 */

#ifndef LOGGING_LEVEL_1
#define LOGGING_LEVEL_1
#endif

#include "user.h"
#include "utils.h"
#include "pools_manager.h"
#include "redis.h"
#include "config_reader.h"
#include "logger.h"
#include "audit_thread.h"
#include "fstream"
#include <regex>
#include <algorithm>
#include <boost/shared_ptr.hpp>

using namespace std;
using namespace boost::gregorian;

User::ConfigEffectiveUser User::config_effective_user;
User::KeyPointDate User::key_point_date;
string User::game_open_date = "";

User::User() {
  ConfigReader::get_config_index_effective_user(
      User::config_effective_user.continuous_,
      User::config_effective_user.at_least_,
      User::config_effective_user.from_day_n_);

  ConfigReader::get_game_open_date(game_open_date);
}

User::~User() {
}

void User::config_before_statistic(const string& str_iso_date) {
  greg_month m = from_undelimited_string(str_iso_date).month();
  greg_year y = from_undelimited_string(str_iso_date).year();

  key_point_date.statis_day = from_undelimited_string(str_iso_date);
  key_point_date.day_begin_month0 = date(y, m, 1);
  key_point_date.day_begin_month1 = date(y, m, 1) - months(1);
  key_point_date.day_begin_month2 = date(y, m, 1) - months(2);
  key_point_date.day_begin_month3 = date(y, m, 1) - months(3);

  key_point_date.day_end_month0 =
    key_point_date.day_begin_month0.end_of_month();
  key_point_date.day_end_month1 =
    key_point_date.day_begin_month1.end_of_month();
  key_point_date.day_end_month2 =
    key_point_date.day_begin_month2.end_of_month();
  key_point_date.day_end_month3 =
    key_point_date.day_begin_month3.end_of_month();
  LOG("\t+ read configuration");
}

void User::user_export(const string& str_iso_date) {
  LOG("-------------------------- <EXPORT USER> -- <", str_iso_date, 
      "> --------------------------");
  cout << "<EXPORT USER> <" << str_iso_date << ">:\n";

  config_before_statistic(str_iso_date);

  //cout << "export recovered user:\n";
  //fill_tbRecoveredUser(str_iso_date);

  //cout << "old effective user:\n";
  //fill_tbEffectiveUser_Old(str_iso_date);

  cout << "effective user:\n";
  fill_tbEffectiveUser(str_iso_date);

  //cout << "export right effective user:\n";
  //find_new_effective_users_wrong(str_iso_date);
  //find_new_effective_users_maybe(str_iso_date);
  //effective_state(str_iso_date);

  //cout << "effective voss fix:\n";
  //find_effective_fix(str_iso_date);

  // check if any thread not done
  while (AuditThread::get_instance().get_count_thread() != 0)
  { 
    if (!AuditThread::get_instance().wait(1))
      break;
  }
  AuditThread::get_instance().reset_wait_time();

  LOG("---------------------------- <DONE EXPORT USER> ----------------------------");
  cout << "<DONE EXPORT USER>\n";
}

void User::statistic(const string& str_iso_date) {
  LOG("-------------------------- <USER INDEX> -- <", str_iso_date,
      "> --------------------------");
  cout << "<USER INDEX> <" << str_iso_date << ">:\n";

  config_before_statistic(str_iso_date);

  cout << "get data:\n";
  get_data(str_iso_date);

  cout << "user register:\n";
  fill_tbUserRegister(str_iso_date);

  cout << "user login times dis:\n";
  fill_RoleLoginTimesDis(str_iso_date);

  cout << "user login:\n";
  fill_tbUserLoginLvDis(str_iso_date);

  cout << "user login by region:\n";
  fill_tbDayLoginRegionDis(str_iso_date);

  cout << "user resident:\n";
  fill_tbResidentUser(str_iso_date);

  cout << "user effective:\n";
  fill_tbEffectiveUser(str_iso_date);

  cout << "effect activity:\n";
  fill_tbEffectActivity(str_iso_date);

  // check if any thread not done
  while (AuditThread::get_instance().get_count_thread() != 0)
  {
    if (!AuditThread::get_instance().wait(1))
      break;
  }
  AuditThread::get_instance().reset_wait_time();

  LOG("----------------------------- <DONE USER INDEX> -----------------------------");
  cout << "<DONE USER INDEX>\n";
}

void User::get_data(const std::string& str_iso_date, const std::string& index) {
  Base::get_data(str_iso_date, index);
}

void User::import_registered(const std::string& str_iso_date,
    const string& index) {
  LOG("\t<import old registered> ---- date: ", str_iso_date, " --------------");
  Utils::loadbar(1);

  string bitmap = Utils::build_key(DATA_TYPE::BITMAP, index, str_iso_date);
  unsigned NUMBER_OF_HASH = 0;
  ConfigReader::get_number_of_hash_server(NUMBER_OF_HASH);

  // execute query to get data from source database
  timestamp_t t0 = get_timestamp();
  Connection* conn = PoolsManager::get_instance().get_src_db_connection();
  boost::shared_ptr<sql::Statement> stmt(conn->createStatement());
  string db_src = "";
  ConfigReader::get_db_src_name(db_src);
  stmt->execute("use " + db_src);

  boost::shared_ptr<sql::ResultSet> res(
      stmt->executeQuery(
        ConfigReader::build_query_for_import_registered(INDEX::REGISTERED)));

  LOG("\t\t+ query done in source database in: ",
      (get_timestamp() - t0) / 1000000.0L, "s.");
  Utils::loadbar(20);

  t0 = get_timestamp();
  string id_generation_type = "";
  ConfigReader::get_id_generation_type(id_generation_type);

  string id = "";
  string user = "";
  string server_hash = "";

  while (res->next()) {
    user = res->getString("iRoleId");
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

    Redis::setbit_comm(bitmap, id);
    Redis::free_reply();
  }

  Utils::loadbar(90);

  // update sorted set, which contain bitmaps of login users
  Redis::zadd_comm(ZSET::STORE_BITMAP + index, str_iso_date, bitmap);
  Redis::free_reply();

  PoolsManager::get_instance().release_src_db_connection(conn);
  LOG("\t\t+ input data to redis database done in: ",
      (get_timestamp() - t0) / 1000000.0L, "s.");
  LOG("\t<done import old registered>");
  Utils::loadbar(100);
  cout << endl;
}
void User::fill_tbUserRegister(const string& str_iso_date) {
  LOG("\t<user register>");
  Utils::loadbar(1);

  string bm_login_pre_month = "tm_bm_login_pre_month";
  string bm_login_in_month = "tm_bm_login_in_month";
  string bm_login_m2dw = "tm_bm_login_m2dw";
  string bm_login_dw2w = "tm_bm_login_dw2w";
  string bm_login_before_td = "tm_bm_login_before_td";

  string bm_reg_total = "tm_bm_reg_total";
  string bm_reg_in_month = "tm_bm_reg_in_month";
  string bm_reg_m2dw = "tm_bm_reg_m2dw";
  string bm_reg_dw2td = "tm_bm_reg_dw2td";
  string bm_reg_dw2w = "tm_bm_reg_dw2w";
  string bm_reg_w2td = "tm_bm_reg_w2td";
  string bm_reg_before_td = "tm_bm_reg_before_td";
  string bm_reg_td = "tm_bm_reg_td";

  TableUserRegister tbUserRegister;

  //-------------------find number of registers in month (not nature month) ----------------------

  // login statistic is: ((total register) XOR (register to pre month)) = register in this month

  // login statistic is:
  // total register:              ---------------------
  //                              XOR
  // register to pre month:       -----------
  //                              =
  // register in this month:                 ----------

  timestamp_t t0 = get_timestamp();
  Utils::create_bm_of_period(
      INDEX::USER,
      "-inf",
      to_iso_string(
        from_undelimited_string(str_iso_date) - days(TIME_CONST::MONTH + 1)),
      bm_login_pre_month);

  Utils::create_bm_of_period(
      INDEX::USER,
      to_iso_string(
        from_undelimited_string(str_iso_date) - days(TIME_CONST::MONTH)),
      str_iso_date, bm_login_in_month);

  Redis::bitop_append_comm("OR", bm_reg_total,
      bm_login_pre_month + " " + bm_login_in_month);
  Redis::bitop_append_comm("XOR", bm_reg_in_month,
      bm_reg_total + " " + bm_login_pre_month);
  Redis::free_replies();

  Redis::bitcount_comm(bm_reg_in_month);
  tbUserRegister.iMonthRegNum = Redis::reply->integer;
  Redis::free_reply();
  LOG("\t\t+ month register user: ", (get_timestamp() - t0) / 1000000.0L, "s.");
  Utils::loadbar(10);

  //------------------------- find total user register -------------------------
  t0 = get_timestamp();
  Redis::bitcount_comm(bm_reg_total);
  tbUserRegister.iAllRegNum = Redis::reply->integer;
  Redis::free_reply();
  LOG("\t\t+ all register user: ", (get_timestamp() - t0) / 1000000.0L, "s.");
  Utils::loadbar(30);

  //-------------------- find number of registers in biweek ------------------------

  // login statistic is:
  // register in this month:      ---------------------
  //                              XOR
  // register in 2 week last:     -----------
  //                              =
  // register in 2 bweek need:               ----------
  t0 = get_timestamp();
  Utils::create_bm_of_period(
      INDEX::USER,
      to_iso_string(
        from_undelimited_string(str_iso_date) - days(TIME_CONST::MONTH)),
      to_iso_string(
        from_undelimited_string(str_iso_date) - days(TIME_CONST::BIWEEK + 1)),
      bm_login_m2dw);

  Redis::bitop_append_comm("AND", bm_reg_m2dw,
      bm_login_m2dw + " " + bm_reg_in_month);
  Redis::bitop_append_comm("XOR", bm_reg_dw2td,
      bm_reg_m2dw + " " + bm_reg_in_month);
  Redis::free_replies();

  Redis::bitcount_comm(bm_reg_dw2td);
  tbUserRegister.iDWeekRegNum = Redis::reply->integer;
  Redis::free_reply();
  LOG("\t\t+ double week register user: ", (get_timestamp() - t0) / 1000000.0L,
      "s.");
  Utils::loadbar(50);

  //--------------------- find number of registers in week -----------------------

  // login statistic is:
  // register in 2 week:          ---------------------
  //                              XOR
  // register in 1 week last:     -----------
  //                              =
  // register in week need:                  ----------
  t0 = get_timestamp();
  Utils::create_bm_of_period(
      INDEX::USER,
      to_iso_string(
        from_undelimited_string(str_iso_date) - days(TIME_CONST::BIWEEK)),
      to_iso_string(
        from_undelimited_string(str_iso_date) - days(TIME_CONST::WEEK + 1)),
      bm_login_dw2w);

  Redis::bitop_append_comm("AND", bm_reg_dw2w,
      bm_login_dw2w + " " + bm_reg_dw2td);
  Redis::bitop_append_comm("XOR", bm_reg_w2td,
      bm_reg_dw2w + " " + bm_reg_dw2td);
  Redis::free_replies();

  Redis::bitcount_comm(bm_reg_w2td);
  tbUserRegister.iWeekRegNum = Redis::reply->integer;
  Redis::free_reply();
  LOG("\t\t+ week register user: ", (get_timestamp() - t0) / 1000000.0L, "s.");
  Utils::loadbar(70);

  //----------------------find number of registers in day --------------------------

  // login statistic is:
  // register in this week:      ---------------------
  //                              XOR
  // register in 6 day last:     -----------
  //                              =
  // register in date we need:              ----------
  t0 = get_timestamp();
  Utils::create_bm_of_period(
      INDEX::USER,
      to_iso_string(
        from_undelimited_string(str_iso_date) - days(TIME_CONST::WEEK)),
      to_iso_string(
        from_undelimited_string(str_iso_date) - days(TIME_CONST::DAY + 1)),
      bm_login_before_td);

  Redis::bitop_append_comm("AND", bm_reg_before_td,
      bm_login_before_td + " " + bm_reg_w2td);
  Redis::bitop_append_comm("XOR", bm_reg_td,
      bm_reg_before_td + " " + bm_reg_w2td);
  Redis::free_replies();

  Redis::bitcount_comm(bm_reg_td);
  tbUserRegister.iDayRegNum = Redis::reply->integer;
  Redis::free_reply();
  LOG("\t\t+ day register user: ", (get_timestamp() - t0) / 1000000.0L, "s.");
  Utils::loadbar(80);

  fill_tbDayRegRegionDis(bm_reg_td, str_iso_date);
  Utils::loadbar(90);

  //---------------------- delelte bitmap temp -----------------------------------

  Redis::del_append_comm(bm_login_pre_month);
  Redis::del_append_comm(bm_login_in_month);
  Redis::del_append_comm(bm_login_m2dw);
  Redis::del_append_comm(bm_login_dw2w);
  Redis::del_append_comm(bm_login_before_td);
  Redis::del_append_comm(bm_reg_total);
  Redis::del_append_comm(bm_reg_in_month);
  Redis::del_append_comm(bm_reg_m2dw);
  Redis::del_append_comm(bm_reg_dw2td);
  Redis::del_append_comm(bm_reg_dw2w);
  Redis::del_append_comm(bm_reg_w2td);
  Redis::del_append_comm(bm_reg_before_td);
  Redis::del_append_comm(bm_reg_td);
  Redis::free_replies();

  // create new thread to write to result db
  AuditThread::get_instance().increase_count_thread();
  boost::thread t1(&PoolsManager::write_to_tbUserRegister,
      &PoolsManager::get_instance(), str_iso_date, tbUserRegister);

  LOG("\t<done user register>");
  Utils::loadbar(100);
  cout << endl;
}

//=============================== tbUserLogin, tbUserLoginLvDis =======================

void User::find_active_users(
    const string& dest_key, const string& str_iso_date_begin,
    const string& str_iso_date_end, FieldsOfUserLoginLvDis field,
    ResultTable<TableUserLoginLvDis>::Type& tbUserLoginLvDis,
    ResultTableUseCerberus<TableActivityScaleLvDis>::Type& tbScaleLvDis) {
  vector<string> info;

  // logic is: find active user by bitmap
  // on this bitmap, get offset of all bit on (offset is user id)
  // get value from sorted set corresponding and find from this: server, level
  Utils::create_bm_of_period(INDEX::USER, str_iso_date_begin, str_iso_date_end,
      dest_key);
  Utils::get_info_from_bitmap(dest_key, info, str_iso_date_begin,
      str_iso_date_end, INDEX::USER);
  statistic_active_by_level_distributed(info, tbUserLoginLvDis, field);
  if (field != eDayActivityNum && field != eDMonthActivityNum) {
    statistic_frequency_active(info, tbScaleLvDis);
  }
  info.clear();
}

void User::find_lost_active_users(
    const string& circle1, const string& circle2, const string& destKey,
    const string& str_iso_date_begin, const string& str_iso_date_end,
    FieldsOfUserLoginLvDis field,
    ResultTable<TableUserLoginLvDis>::Type& svdis) {

  // logic is:
  // login circle2        --------------
  // login circle1                -------------
  //
  // login both circle            ------
  //                          XOR
  // login circle2        --------------
  // lost active          --------

  vector<string> info;
  Utils::create_bm_of_period(INDEX::USER, str_iso_date_begin, str_iso_date_end,
      circle2);

  string temp = "tm_temp";

  Redis::bitop_append_comm("AND", temp, circle1 + " " + circle2);
  Redis::bitop_append_comm("XOR", destKey, temp + " " + circle2);
  Redis::free_replies();

  Utils::get_info_from_bitmap(destKey, info, str_iso_date_begin,
      str_iso_date_end, INDEX::USER);

  statistic_active_by_level_distributed(info, svdis, field);

  info.clear();
  Redis::del_append_comm(temp);
  Redis::free_replies();
}

void User::find_back_active_users(
    const string& circle1, const string& circle2, const string& circle3,
    const string& destKey, const string& str_iso_start_circle3,
    const string& str_iso_end_circle3, const std::string& std_iso_stat_date,
    FieldsOfUserLoginLvDis field,
    ResultTable<TableUserLoginLvDis>::Type& svdis) {

  // logic is:
  // login circle3                ------------
  //                              AND
  // login circle2                      --------------
  // login both c23                     ------
  //                              XOR c3
  // login c3 not login c2        ------
  //                              AND
  // login circle1                   -----  ------   ---------
  // login c3 not c2, return c1      ---

  vector<string> info;
  Utils::create_bm_of_period(INDEX::USER, str_iso_start_circle3,
      str_iso_end_circle3, circle3);

  string temp1 = "tm_temp1";
  string temp2 = "tm_temp2";

  Redis::bitop_append_comm("AND", temp1, circle2 + " " + circle3);
  Redis::bitop_append_comm("XOR", temp2, temp1 + " " + circle3);
  Redis::bitop_append_comm("AND", destKey, temp2 + " " + circle1);
  Redis::free_replies();

  Utils::get_info_from_bitmap(destKey, info, str_iso_start_circle3,
      std_iso_stat_date, INDEX::USER);

  statistic_active_by_level_distributed(info, svdis, field);

  info.clear();
  Redis::del_append_comm(temp1);
  Redis::del_append_comm(temp2);
  Redis::free_replies();
}

void User::fill_tbUserLoginLvDis(const string& str_iso_date) {
  LOG("\t<user login>");
  Utils::loadbar(1);
  vector<string> info;

  // iDayActivityNum
  timestamp_t t0 = get_timestamp();
  string login_in_statis_date = "tm_login_in_statis_date";
  ResultTable<TableUserLoginLvDis>::Type result;
  ResultTableUseCerberus<TableActivityScaleLvDis>::Type tbScaleLvDis;
  ResultTableUseCerberus<TableActivityScaleLvDis>::Type tbWeekScaleLvDis;
  ResultTableUseCerberus<TableActivityScaleLvDis>::Type tbDWeekScaleLvDis;
  ResultTableUseCerberus<TableActivityScaleLvDis>::Type tbMonthScaleLvDis;

  find_active_users(login_in_statis_date, str_iso_date, str_iso_date,
      eDayActivityNum, result, tbScaleLvDis);
  LOG("\t\t+ day activity users: ", (get_timestamp() - t0) / 1000000.0L, "s.");
  Utils::loadbar(5);

  // iWeekActivityNum
  t0 = get_timestamp();
  string login_w1 = "tm_w1";
  find_active_users(
      login_w1,
      to_iso_string(
        from_undelimited_string(str_iso_date) - days(TIME_CONST::WEEK)),
      str_iso_date, eWeekActivityNum, result, tbWeekScaleLvDis);
  LOG("\t\t+ week activity user: ", (get_timestamp() - t0) / 1000000.0L, "s.");
  Utils::loadbar(10);

  // iWeekLostNum
  t0 = get_timestamp();
  string login_w2 = "tm_w2";
  string lost_w2 = "tm_lost_w2";
  find_lost_active_users(
      login_w1,
      login_w2,
      lost_w2,
      to_iso_string(
        from_undelimited_string(str_iso_date) - days(TIME_CONST::BIWEEK)),
      to_iso_string(
        from_undelimited_string(str_iso_date) - days(TIME_CONST::WEEK + 1)),
      eWeekLostNum, result);
  LOG("\t\t+ week lost user: ", (get_timestamp() - t0) / 1000000.0L, "s.");
  Utils::loadbar(15);

  // iWeekBackNum
  t0 = get_timestamp();
  string login_w3 = "tm_w3";
  string back_w3 = "tm_back_w3";
  find_back_active_users(
      login_w1,
      login_w2,
      login_w3,
      back_w3,
      to_iso_string(
        from_undelimited_string(str_iso_date) - days(TIME_CONST::THWEEK)),
      to_iso_string(
        from_undelimited_string(str_iso_date) - days(TIME_CONST::BIWEEK + 1)),
      str_iso_date, eWeekBackNum, result);
  LOG("\t\t+ week back user: ", (get_timestamp() - t0) / 1000000.0L, "s.");
  Utils::loadbar(20);

  // iDWeekActivityNum
  t0 = get_timestamp();
  string login_w12 = "tm_w12";
  find_active_users(
      login_w12,
      to_iso_string(
        from_undelimited_string(str_iso_date) - days(TIME_CONST::BIWEEK)),
      str_iso_date, eDWeekActivityNum, result, tbDWeekScaleLvDis);
  LOG("\t\t+ double week activity user: ", (get_timestamp() - t0) / 1000000.0L,
      "s.");
  Utils::loadbar(30);

  // iDWeekLostNum
  t0 = get_timestamp();
  string login_w34 = "tm_w34";
  string lost_w34 = "tm_lost_w34";
  find_lost_active_users(
      login_w12,
      login_w34,
      lost_w34,
      to_iso_string(
        from_undelimited_string(str_iso_date) - days(TIME_CONST::FOURWEEK)),
      to_iso_string(
        from_undelimited_string(str_iso_date) - days(TIME_CONST::BIWEEK + 1)),
      eDWeekLostNum, result);
  LOG("\t\t+ double week lost user: ", (get_timestamp() - t0) / 1000000.0L,
      "s.");
  Utils::loadbar(35);

  // iDWeekBackNum
  t0 = get_timestamp();
  string login_w56 = "tm_w56";
  string back_w56 = "tm_back_w56";
  find_back_active_users(
      login_w12,
      login_w34,
      login_w56,
      back_w56,
      to_iso_string(
        from_undelimited_string(str_iso_date) - days(TIME_CONST::SIXWEEK)),
      to_iso_string(
        from_undelimited_string(str_iso_date)
        - days(TIME_CONST::FOURWEEK + 1)),
      str_iso_date, eDWeekBackNum, result);
  LOG("\t\t+ double week back user: ", (get_timestamp() - t0) / 1000000.0L,
      "s.");
  Utils::loadbar(40);

  // iMonthActivityNum
  t0 = get_timestamp();
  string login_m1 = "tm_m1";
  find_active_users(
      login_m1,
      to_iso_string(
        from_undelimited_string(str_iso_date) - days(TIME_CONST::MONTH)),
      str_iso_date, eMonthActivityNum, result, tbMonthScaleLvDis);
  LOG("\t\t+ month activity user: ", (get_timestamp() - t0) / 1000000.0L, "s.");
  Utils::loadbar(55);

  // iMonthLostNum
  t0 = get_timestamp();
  string login_m2 = "tm_m2";
  string lost_m2 = "tm_lost_m2";
  find_lost_active_users(
      login_m1,
      login_m2,
      lost_m2,
      to_iso_string(
        from_undelimited_string(str_iso_date) - days(TIME_CONST::BIMONTH)),
      to_iso_string(
        from_undelimited_string(str_iso_date) - days(TIME_CONST::MONTH + 1)),
      eMonthLostNum, result);
  LOG("\t\t+ month lost user: ", (get_timestamp() - t0) / 1000000.0L, "s.");
  Utils::loadbar(60);

  // iDMonthActivityNum
  string login_m12 = "tm_m12";
  find_active_users(
      login_m12,
      to_iso_string(
        from_undelimited_string(str_iso_date) - days(TIME_CONST::BIMONTH)),
      str_iso_date, eDMonthActivityNum, result, tbScaleLvDis);
  LOG("\t\t+ double month activity user: ", (get_timestamp() - t0) / 1000000.0L,
      "s.");
  Utils::loadbar(70);

  // iMonthBackNum
  t0 = get_timestamp();
  string login_m3 = "tm_m3";
  string back_m3 = "tm_back_m3";
  find_back_active_users(
      login_m1,
      login_m2,
      login_m3,
      back_m3,
      to_iso_string(
        from_undelimited_string(str_iso_date) - days(TIME_CONST::THMONTH)),
      to_iso_string(
        from_undelimited_string(str_iso_date)
        - days(TIME_CONST::BIMONTH + 1)),
      str_iso_date, eMonthBackNum, result);
  LOG("\t\t+ month back user: ", (get_timestamp() - t0) / 1000000.0L, "s.");
  Utils::loadbar(75);

  Redis::del_append_comm(login_in_statis_date);
  Redis::del_append_comm(login_w1);
  Redis::del_append_comm(login_w2);
  Redis::del_append_comm(lost_w2);
  Redis::del_append_comm(login_w3);
  Redis::del_append_comm(back_w3);
  Redis::del_append_comm(login_w12);
  Redis::del_append_comm(login_w34);
  Redis::del_append_comm(login_w56);
  Redis::del_append_comm(back_w56);
  Redis::del_append_comm(login_m1);
  Redis::del_append_comm(login_m2);
  Redis::del_append_comm(lost_m2);
  Redis::del_append_comm(login_m12);
  Redis::del_append_comm(login_m3);
  Redis::del_append_comm(back_m3);
  Redis::del_append_comm(lost_w34);
  Redis::free_replies();

  // create new thread to write to result db
  AuditThread::get_instance().increase_count_thread();
  boost::thread t1(&PoolsManager::write_to_tbUserLoginLvDis,
      &PoolsManager::get_instance(), str_iso_date, result);

  AuditThread::get_instance().increase_count_thread();
  boost::thread t2(&PoolsManager::write_to_tbWeekActivityScaleLvDis,
      &PoolsManager::get_instance(), str_iso_date,
      tbWeekScaleLvDis);

  AuditThread::get_instance().increase_count_thread();
  boost::thread t3(&PoolsManager::write_to_tbDWeekActivityScaleLvDis,
      &PoolsManager::get_instance(), str_iso_date,
      tbDWeekScaleLvDis);

  AuditThread::get_instance().increase_count_thread();
  boost::thread t4(&PoolsManager::write_to_tbMonthActivityScaleLvDis,
      &PoolsManager::get_instance(), str_iso_date,
      tbMonthScaleLvDis);

  LOG("\t<done user login>");
  Utils::loadbar(100);
  cout << endl;
}

void User::fill_RoleLoginTimesDis(const std::string& str_iso_date) {
  LOG("\t<role login times / dis>");
  Utils::loadbar(1);
  timestamp_t t0 = get_timestamp();

  vector<string> info;
  string key = Utils::build_key(DATA_TYPE::BITMAP, INDEX::USER, str_iso_date);

  Utils::get_info_from_bitmap(key, info, str_iso_date, str_iso_date,
      INDEX::USER);

  ResultTable<TableRoleLoginTimesDis>::Type result_table;
  vector<unsigned> store;
  store.clear();
  unsigned server, loginTime;
  for (unsigned i = 0; i < info.size(); ++i) {
    Utils::split(info[i], store);
    server = store[0];
    //level = store[1];
    loginTime = store[2];
    store.clear();

    if (result_table.find(Keys(server, loginTime)) == result_table.end()) {
      result_table.insert(
          pair<Keys, TableRoleLoginTimesDis>(Keys(server, loginTime),
            TableRoleLoginTimesDis()));
    }
    if (result_table.find(Keys(server, 0)) == result_table.end()) {
      result_table.insert(
          pair<Keys, TableRoleLoginTimesDis>(Keys(server, 0),
            TableRoleLoginTimesDis()));
    }
    result_table[Keys(server, loginTime)].increase();
    result_table[Keys(server, 0)].increase(loginTime);
  }
  info.clear();
  LOG("\t\t+ role login times dis: ", (get_timestamp() - t0) / 1000000.0L,
      "s.");
  Utils::loadbar(75);

  // create new thread to write to result db
  AuditThread::get_instance().increase_count_thread();
  boost::thread t(&PoolsManager::write_to_tbRoleLoginTimesDis,
      &PoolsManager::get_instance(), str_iso_date, result_table);

  LOG("\t<done role login times / dis>");
  Utils::loadbar(100);
  cout << endl;
}

//================================== tbResidentUser ======================================

void User::find_last_date_new_reg_login(
    const string& bm_reg_in_statis_date, const string& str_iso_reg_date,
    const string& str_iso_stat_date,
    ResultTableUseCerberus<TableResidentUserLvDis>::Type& result_table) {
  unsigned bm_size = 0;
  vector<unsigned> ids;

  // get ids user register in statis date
  Redis::strlen_comm(bm_reg_in_statis_date);
  bm_size = Redis::reply->integer;
  Redis::free_reply();

  Redis::get_comm(bm_reg_in_statis_date);
  Utils::get_ids_from_bitmap(ids, Redis::reply->str, bm_size);
  Redis::free_reply();

  for (unsigned i = 0; i < ids.size(); ++i) {
    Redis::zrevrangebyscore_append_comm(
        INDEX::USER + ":" + Utils::convert<unsigned, string>(ids[i]),
        str_iso_stat_date, str_iso_reg_date, "limit 0 1 withscores");
  }

  // have list user (vector<> ids), find last date user login by sorted set
  Redis::get_and_free_resident_reply(
      result_table, Utils::convert<string, unsigned>(str_iso_reg_date),
      Utils::convert<string, unsigned>(str_iso_stat_date));

  for (ResultTableUseCerberus<TableResidentUserLvDis>::Type::iterator date_dayth_lv_it =
      result_table.begin(); date_dayth_lv_it != result_table.end();
      date_dayth_lv_it++) {
    if (date_dayth_lv_it->first.lv_ == 0) {
      date_dayth_lv_it->second.iCumulateUserNum = date_dayth_lv_it->second
        .iUserNum;
    } else {
      if (result_table.find(
            Cerberus(date_dayth_lv_it->first.sv_, date_dayth_lv_it->first.lv_ - 1,
              date_dayth_lv_it->first.days_)) != result_table.end()) {
        date_dayth_lv_it->second.iCumulateUserNum = result_table[Cerberus(
            date_dayth_lv_it->first.sv_, date_dayth_lv_it->first.lv_ - 1,
            date_dayth_lv_it->first.days_)].iCumulateUserNum
          + date_dayth_lv_it->second.iUserNum;
      } else
        date_dayth_lv_it->second.iCumulateUserNum = date_dayth_lv_it->second
          .iUserNum;
    }
  }
}

void User::fill_tbResidentUser(const string& str_iso_date) {
  LOG("\t<user resident>");
  timestamp_t t0 = get_timestamp();

  unsigned dayth = 1;
  Utils::loadbar(dayth);

  ResultTableUseCerberus<TableResidentUserLvDis>::Type result_table;
  string bm_reg2BeforeStatisDate = "tm_bm_reg2BeforeStatisDate";
  string bm_reg2StatisDate = "tm_bm_reg2StatisDate";
  string bm_regInStatisDate = "tm_bm_regInStatisDate";  // nho delete

  Redis::del_append_comm(bm_reg2StatisDate);
  Redis::del_append_comm(bm_reg2BeforeStatisDate);
  Redis::del_append_comm(bm_regInStatisDate);
  Redis::free_replies();

  // observation 90 days from statistic date
  date d = from_undelimited_string(str_iso_date) - days(90);
  while (d <= from_undelimited_string(str_iso_date)) {
    // at 90 days ago, we have n register
    // find last login of n register in 90 days
    // ...
    // at 89 days ago, we have n register
    // find last login of n register in 89 days
    // ...
    if (d >= from_simple_string(game_open_date)) {
      if (Redis::exists(bm_reg2BeforeStatisDate)) {
        Redis::bitop_append_comm(
            "XOR", bm_regInStatisDate,
            bm_reg2StatisDate + " " + bm_reg2BeforeStatisDate);
        Redis::free_replies();

        Redis::bitcount_comm(bm_regInStatisDate);
        if (Redis::reply->integer > 0) {
          Redis::free_reply();

          find_last_date_new_reg_login(bm_regInStatisDate, to_iso_string(d),
              str_iso_date, result_table);
        } else {
          LOG_WARN("\t+ ", d, " not have data.");
          Redis::free_reply();
        }
        Redis::bitop_append_comm(
            "OR", bm_reg2BeforeStatisDate,
            bm_reg2BeforeStatisDate + " " + bm_reg2StatisDate);
        Redis::bitop_append_comm(
            "OR",
            bm_reg2StatisDate,
            bm_reg2BeforeStatisDate + " "
            + Utils::build_key(DATA_TYPE::BITMAP, INDEX::USER,
              to_iso_string(d + days(1))));
        Redis::free_replies();
      } else {
        Utils::create_bm_of_period(INDEX::USER, "-inf",
            to_iso_string(d - days(1)),
            bm_reg2BeforeStatisDate);

        Redis::bitop_append_comm(
            "OR",
            bm_reg2StatisDate,
            bm_reg2BeforeStatisDate + " "
            + Utils::build_key(DATA_TYPE::BITMAP, INDEX::USER,
              to_iso_string(d)));

        Redis::bitop_append_comm(
            "XOR", bm_regInStatisDate,
            bm_reg2StatisDate + " " + bm_reg2BeforeStatisDate);
        Redis::free_replies();

        Redis::bitcount_comm(bm_regInStatisDate);
        if (Redis::reply->integer > 0) {
          Redis::free_reply();

          find_last_date_new_reg_login(bm_regInStatisDate, to_iso_string(d),
              str_iso_date, result_table);
        } else {
          LOG_WARN("\t+ ", d, " not have data.");
          Redis::free_reply();
        }
        Redis::bitop_append_comm(
            "OR", bm_reg2BeforeStatisDate,
            bm_reg2BeforeStatisDate + " " + bm_reg2StatisDate);
        Redis::bitop_append_comm(
            "OR",
            bm_reg2StatisDate,
            bm_reg2BeforeStatisDate + " "
            + Utils::build_key(DATA_TYPE::BITMAP, INDEX::USER,
              to_iso_string(d + days(1))));
        Redis::free_replies();
      }
    }
    d += days(1);

    Utils::loadbar(dayth);
    ++dayth;
  }
  Redis::del_append_comm(bm_reg2StatisDate);
  Redis::del_append_comm(bm_reg2BeforeStatisDate);
  Redis::del_append_comm(bm_regInStatisDate);
  Redis::free_replies();
  LOG("\t\t+ resident user: ", (get_timestamp() - t0) / 1000000.0L, "s.");

  // create new thread to write to result db
  AuditThread::get_instance().increase_count_thread();
  boost::thread t(&PoolsManager::write_to_tbResidentUser,
      &PoolsManager::get_instance(), str_iso_date, result_table);

  LOG("\t<done user resident>");
  Utils::loadbar(100);
  cout << endl;
}

//======================== tbEffectiveUser & tbEffectiveUserLvDis =====================

void User::find_num_reg_of_natural_month(
    const string& str_iso_stat_date,
    ResultTable<TableEffecUserLvDis>::Type& uomResult) {

  vector<unsigned> ids;
  vector<string> info;

  string r2m1 = "tm_r2m1";
  string r2s = "tm_r2s";
  string reg_in_m0 = "tm_reg_in_m0";
  ids.clear();
  info.clear();

  // logic is:
  // -------------month3---------month2-----------month1--------month0-------statis_day
  //
  // register up to statis date             -------------------------------------------
  // register up to end month1              --------------------
  // register in month0 (nature)                                -----------------------

  Utils::create_bm_of_period(INDEX::USER, "-inf", str_iso_stat_date, r2s);
  Utils::create_bm_of_period(INDEX::USER, "-inf",
      to_iso_string(key_point_date.day_end_month1),
      r2m1);

  Redis::bitop_comm("XOR", reg_in_m0, r2s + " " + r2m1);
  Redis::free_reply();

  // take size
  unsigned bm_size = 0;
  Redis::strlen_comm(reg_in_m0);
  bm_size = Redis::reply->integer;
  Redis::free_reply();

  Redis::get_comm(reg_in_m0);
  Utils::get_ids_from_bitmap(ids, Redis::reply->str, bm_size);
  Redis::free_reply();

  for (unsigned i = 0; i < ids.size(); ++i) {
    Redis::zrevrangebyscore_append_comm(
        INDEX::USER + ":" + Utils::convert<unsigned, string>(ids[i]),
        str_iso_stat_date, to_iso_string(key_point_date.day_begin_month0),
        "limit 0 1");
  }
  Redis::get_and_free_replies(info);
  statistic_effective_by_level_distributed(info, uomResult,
      eNatureMonthRegisterNum);

  Redis::del_append_comm(r2m1);
  Redis::del_append_comm(r2s);
  Redis::del_append_comm(reg_in_m0);
  Redis::free_replies();
}

void User::find_new_effective_users(
    const string& isoDateStat,
    ResultTable<TableEffecUserLvDis>::Type& uomResult) {

  if (key_point_date.statis_day - days(config_effective_user.continuous_)
      < key_point_date.day_begin_month0) {
    return;
  }

  date date_iterator0, date_iterator1, date_iterator2, date_iterator3;

  string r2d1 = "tm_r2d1";
  string r2d0 = "tm_r2d0";
  string r_in_d0 = "tm_r_in_d0";
  string r_in_month = "tm_r_in_month";

  vector<unsigned> ids_candidate;
  vector<string> info;
  ids_candidate.clear();
  info.clear();

  string local_candidate = "tm_local_candidate";
  string keys = "";

  Utils::create_bm_of_period(INDEX::USER, "-inf",
      to_iso_string(key_point_date.day_end_month1),
      r2d1);

  Redis::bitop_comm("OR", r2d0, r2d1);
  Redis::free_reply();

  // loop from begin month0 to statis date
  // for each day:
  //    case1: find numbers of user had reg in this day and continuous login 4 day next
  //    case2: find numbers of user had reg in this day and from 6th day after this day
  //            to statistic day, he login at least 2 days.
  for (date_iterator0 = key_point_date.day_begin_month0;
      date_iterator0
      <= key_point_date.statis_day - days(config_effective_user.continuous_);
      date_iterator0 += days(1)) {
    Redis::bitop_append_comm(
        "OR",
        r2d0,
        r2d1 + " "
        + Utils::build_key(DATA_TYPE::BITMAP, INDEX::USER,
          to_iso_string(date_iterator0)));

    Redis::bitop_append_comm("XOR", r_in_d0, r2d1 + " " + r2d0);
    Redis::bitop_append_comm("OR", r_in_month, r_in_month + " " + r_in_d0);

    //----------------  case 1
    // if reg in d0 and login continuous 4 days:
    // reg_d0 AND log_d0 AND log_d1 AND log_d2 AND log_d3 AND log_d4
    keys = "";
    for (unsigned i = 0; i <= config_effective_user.continuous_; ++i) {
      keys += Utils::build_key(DATA_TYPE::BITMAP, INDEX::USER,
          to_iso_string(date_iterator0 + days(i))) + " ";
    }
    keys += r_in_d0;

    Redis::bitop_append_comm("AND", local_candidate, keys);
    Redis::bitop_append_comm("OR", BM::EFFECTIVE_NEW_USERS,
        BM::EFFECTIVE_NEW_USERS + " " + local_candidate);

    Redis::del_append_comm(local_candidate);
    //------------------ case 2
    // from 6th day (day + 5) duyet toi statis day:
    // if from 6th day to statis day have at least 2 day login and reg in this month is effective
    date_iterator1 = date_iterator0 + days(config_effective_user.from_day_n_);
    for (date_iterator2 = date_iterator1 + days(1);
        date_iterator2 <= key_point_date.statis_day;
        date_iterator2 += days(1)) {

      // login_d1 AND login_d2 AND reg_in_month
      keys = Utils::build_key(DATA_TYPE::BITMAP, INDEX::USER,
          to_iso_string(date_iterator1)) + " "
        + Utils::build_key(DATA_TYPE::BITMAP, INDEX::USER,
            to_iso_string(date_iterator2)) + " " + r_in_month;

      Redis::bitop_append_comm("AND", local_candidate, keys);
      Redis::bitop_append_comm("OR", BM::EFFECTIVE_NEW_USERS,
          BM::EFFECTIVE_NEW_USERS + " " + local_candidate);

      Redis::del_append_comm(local_candidate);
    }

    Redis::bitop_append_comm(
        "OR",
        r2d1,
        r2d1 + " "
        + Utils::build_key(DATA_TYPE::BITMAP, INDEX::USER,
          to_iso_string(date_iterator0)));
    Redis::del_append_comm(r_in_d0);
  }
  Redis::free_replies();

  // have bitmap new effective user, find detail
  unsigned bm_size = 0;

  Redis::strlen_comm(BM::EFFECTIVE_NEW_USERS);
  bm_size = Redis::reply->integer;
  Redis::free_reply();

  Redis::get_comm(BM::EFFECTIVE_NEW_USERS);
  Utils::get_ids_from_bitmap(ids_candidate, Redis::reply->str, bm_size);
  Redis::free_reply();

  for (unsigned i = 0; i < ids_candidate.size(); ++i) {
    Redis::zrevrangebyscore_append_comm(
        INDEX::USER + ":" + Utils::convert<unsigned, string>(ids_candidate[i]),
        isoDateStat, to_iso_string(key_point_date.day_begin_month0),
        "limit 0 1");
  }

  Redis::get_and_free_replies(info);
  statistic_effective_by_level_distributed(info, uomResult, eNewEffectiveNum);

  vector<string> account_names;
  Utils::get_account_from_bitmap(account_names, BM::EFFECTIVE_NEW_USERS, "new_eff_wrong.txt");

  Redis::del_append_comm(r2d1);
  Redis::del_append_comm(r2d0);
  Redis::del_append_comm(r_in_d0);
  Redis::del_append_comm(r_in_month);
  Redis::del_append_comm(local_candidate);
  Redis::del_append_comm(BM::EFFECTIVE_NEW_USERS);
  Redis::free_replies();
}

void User::find_new_effective_users_of_month(
    const std::string& str_iso_stat_date) {
  // like find_new_effective_users

  //timestamp_t t0 = get_timestamp();
  greg_month m = from_undelimited_string(str_iso_stat_date).month();
  greg_year y = from_undelimited_string(str_iso_stat_date).year();

  date date_iterator0, date_iterator1, date_iterator2, date_iterator3;
  date begin_month0 = date(y, m, 1);
  date begin_month1 = date(y, m, 1) - months(1);

  date end_month0 = begin_month0.end_of_month();
  date end_month1 = begin_month1.end_of_month();

  string r2d1 = "tm_r2d1";
  string r2d0 = "tm_r2d0";
  string r_in_d0 = "tm_r_in_d0";
  string r_in_month = "tm_r_in_month";

  string local_candidate = "tm_local_candidate";
  string keys = "";

  Utils::create_bm_of_period(INDEX::USER, "-inf", to_iso_string(end_month1),
      r2d1);
  Redis::bitop_comm("OR", r2d0, r2d1);
  Redis::free_reply();

  for (date_iterator0 = begin_month0; date_iterator0 <= end_month0;
      date_iterator0 += days(1)) {
    Redis::bitop_append_comm(
        "OR",
        r2d0,
        r2d1 + " "
        + Utils::build_key(DATA_TYPE::BITMAP, INDEX::USER,
          to_iso_string(date_iterator0)));

    Redis::bitop_append_comm("XOR", r_in_d0, r2d1 + " " + r2d0);
    Redis::bitop_append_comm("OR", r_in_month, r_in_month + " " + r_in_d0);

    keys = "";
    for (unsigned i = 0; i <= config_effective_user.continuous_; ++i) {
      keys += Utils::build_key(DATA_TYPE::BITMAP, INDEX::USER,
          to_iso_string(date_iterator0 + days(i))) + " ";
    }
    keys += r_in_d0;

    Redis::bitop_append_comm("AND", local_candidate, keys);
    Redis::bitop_append_comm("OR", BM::EFFECTIVE_NEW_USERS,
        BM::EFFECTIVE_NEW_USERS + " " + local_candidate);
    Redis::del_append_comm(local_candidate);

    date_iterator1 = date_iterator0 + days(config_effective_user.from_day_n_);
    for (date_iterator2 = date_iterator1 + days(1);
        date_iterator2 <= end_month0 + days(5); date_iterator2 += days(1)) {
      keys = Utils::build_key(DATA_TYPE::BITMAP, INDEX::USER,
          to_iso_string(date_iterator1)) + " "
        + Utils::build_key(DATA_TYPE::BITMAP, INDEX::USER,
            to_iso_string(date_iterator2)) + " " + r_in_month;

      Redis::bitop_append_comm("AND", local_candidate, keys);
      Redis::bitop_append_comm("OR", BM::EFFECTIVE_NEW_USERS,
          BM::EFFECTIVE_NEW_USERS + " " + local_candidate);
      Redis::del_append_comm(local_candidate);
    }

    Redis::bitop_append_comm(
        "OR",
        r2d1,
        r2d1 + " "
        + Utils::build_key(DATA_TYPE::BITMAP, INDEX::USER,
          to_iso_string(date_iterator0)));
    Redis::del_append_comm(r_in_d0);
  }
  Redis::free_replies();

  Redis::bitcount_comm(BM::EFFECTIVE_NEW_USERS);
  Redis::free_reply();

  Redis::del_append_comm(r2d1);
  Redis::del_append_comm(r2d0);
  Redis::del_append_comm(r_in_d0);
  Redis::del_append_comm(r_in_month);
  Redis::del_append_comm(local_candidate);
  Redis::free_replies();
}

void User::find_back_effective_users(
    const string& str_iso_stat_date,
    ResultTable<TableEffecUserLvDis>::Type& uomResult) {

  date dit, dit1, dit2, dit3;

  string local_candidate = "tm_local_candidate";
  string keys = "";

  string l_in_month1 = "tm_l_in_month1";
  string nl_in_month1 = "tm_nl_in_month1";
  string r_2_month2 = "tm_r_2_month2";
  string bm_candidate = "tm_bm_candidate";

  // begin game ---------- end_month2 --------- end_month1 ----------- statis_day
  // find all users, who had registered from begin game to end month2
  // and not login in month1 as "candidate" back effective user

  Utils::create_bm_of_period(INDEX::USER, "-inf",
      to_iso_string(key_point_date.day_end_month2),
      r_2_month2);
  Utils::create_bm_of_period(INDEX::USER,
      to_iso_string(key_point_date.day_begin_month1),
      to_iso_string(key_point_date.day_end_month1),
      l_in_month1);

  Redis::bitop_append_comm("NOT", nl_in_month1, l_in_month1);
  Redis::bitop_append_comm("AND", bm_candidate,
      r_2_month2 + " " + nl_in_month1);
  Redis::free_replies();

  // loop from begin_month0 to statis day, for each day, find 2 case:
  //    case1: find users had login in this day, and continuous 4 days next
  //    case2: find users from 6th day from this day, to statis day, had login at least 2 days
  for (dit = key_point_date.day_begin_month0;
      dit <= key_point_date.statis_day - days(config_effective_user.continuous_);
      dit += days(1)) {

    keys = "";
    for (unsigned i = 0; i <= config_effective_user.continuous_; ++i) {
      keys += Utils::build_key(DATA_TYPE::BITMAP, INDEX::USER,
          to_iso_string(dit + days(i))) + " ";
    }
    keys += bm_candidate;

    Redis::bitop_append_comm("AND", local_candidate, keys);
    Redis::bitop_append_comm(
        "OR", BM::EFFECTIVE_RETURN_USERS,
        BM::EFFECTIVE_RETURN_USERS + " " + local_candidate);
    Redis::del_append_comm(local_candidate);

    dit1 = dit + days(config_effective_user.from_day_n_);
    for (dit2 = dit1 + days(1); dit2 <= key_point_date.statis_day;
        dit2 += days(1)) {
      keys = Utils::build_key(DATA_TYPE::BITMAP, INDEX::USER,
          to_iso_string(dit1)) + " "
        + Utils::build_key(DATA_TYPE::BITMAP, INDEX::USER,
            to_iso_string(dit2)) + " "
        + Utils::build_key(DATA_TYPE::BITMAP, INDEX::USER, to_iso_string(dit))
        + " " + bm_candidate;

      Redis::bitop_append_comm("AND", local_candidate, keys);
      Redis::bitop_append_comm(
          "OR", BM::EFFECTIVE_RETURN_USERS,
          BM::EFFECTIVE_RETURN_USERS + " " + local_candidate);
      Redis::del_append_comm(local_candidate);
    }
  }
  Redis::free_replies();

  // we have BM:EFFECTIVE_RETURN_USERS contain all user effective back
  // now, find detail: for each server, level
  vector<unsigned> ids_candidate;
  vector<string> info;
  ids_candidate.clear();
  info.clear();

  unsigned bm_size = 0;
  Redis::strlen_comm(BM::EFFECTIVE_RETURN_USERS);
  bm_size = Redis::reply->integer;
  Redis::free_reply();

  Redis::get_comm(BM::EFFECTIVE_RETURN_USERS);
  Utils::get_ids_from_bitmap(ids_candidate, Redis::reply->str, bm_size);
  Redis::free_reply();

  for (unsigned i = 0; i < ids_candidate.size(); ++i) {
    Redis::zrevrangebyscore_append_comm(
        INDEX::USER + ":" + Utils::convert<unsigned, string>(ids_candidate[i]),
        str_iso_stat_date, to_iso_string(key_point_date.day_begin_month0),
        "limit 0 1");
  }
  Redis::get_and_free_replies(info);
  statistic_effective_by_level_distributed(info, uomResult, eBackEffectiveNum);

  vector<string> account_names;
  Utils::get_account_from_bitmap(account_names, BM::EFFECTIVE_RETURN_USERS, "return_eff_wrong.txt");

  Redis::del_append_comm(r_2_month2);
  Redis::del_append_comm(l_in_month1);
  Redis::del_append_comm(nl_in_month1);
  Redis::del_append_comm(bm_candidate);
  Redis::del_append_comm(BM::EFFECTIVE_RETURN_USERS);
  Redis::free_replies();
}

void User::find_back_effective_users_of_month(const string& str_iso_stat_date) {
  // like find_back_effective_users
  date date_iterator0, date_iterator1, date_iterator2, date_iterator3;
  string local_candidate = "tm_local_candidate";
  string keys = "";

  greg_month m = from_undelimited_string(str_iso_stat_date).month();
  greg_year y = from_undelimited_string(str_iso_stat_date).year();
  date begin_month0 = date(y, m, 1);
  date begin_month1 = date(y, m, 1) - months(1);
  date begin_month2 = date(y, m, 1) - months(2);

  date end_month0 = begin_month0.end_of_month();
  date end_month1 = begin_month1.end_of_month();
  date end_month2 = begin_month2.end_of_month();

  string l_in_month1 = "tm_l_in_month1";
  string nl_in_month1 = "tm_nl_in_month1";
  string r_2_month2 = "tm_r_2_month2";
  string bm_candidate = "tm_bm_candidate";

  Utils::create_bm_of_period(INDEX::USER, "-inf", to_iso_string(end_month2),
      r_2_month2);
  Utils::create_bm_of_period(INDEX::USER, to_iso_string(begin_month1),
      to_iso_string(end_month1), l_in_month1);

  Redis::bitop_append_comm("NOT", nl_in_month1, l_in_month1);
  Redis::bitop_append_comm("AND", bm_candidate,
      r_2_month2 + " " + nl_in_month1);
  Redis::free_replies();

  for (date_iterator0 = begin_month0; date_iterator0 <= end_month0;
      date_iterator0 += days(1)) {

    keys = "";
    for (unsigned i = 0; i < config_effective_user.continuous_; ++i) {
      keys += Utils::build_key(DATA_TYPE::BITMAP, INDEX::USER,
          to_iso_string(date_iterator0 + days(i))) + " ";
    }
    keys += bm_candidate;

    Redis::bitop_append_comm("AND", local_candidate, keys);
    Redis::bitop_append_comm(
        "OR", BM::EFFECTIVE_RETURN_USERS,
        BM::EFFECTIVE_RETURN_USERS + " " + local_candidate);
    Redis::del_append_comm(local_candidate);

    date_iterator1 = date_iterator0 + days(config_effective_user.from_day_n_);
    for (date_iterator2 = date_iterator1 + days(1);
        date_iterator2 <= key_point_date.day_end_month0 + days(4);
        date_iterator2 += days(1)) {
      keys = Utils::build_key(DATA_TYPE::BITMAP, INDEX::USER,
          to_iso_string(date_iterator1)) + " "
        + Utils::build_key(DATA_TYPE::BITMAP, INDEX::USER,
            to_iso_string(date_iterator2)) + " "
        + Utils::build_key(DATA_TYPE::BITMAP, INDEX::USER,
            to_iso_string(date_iterator0)) + " "
        + bm_candidate;

      Redis::bitop_append_comm("AND", local_candidate, keys);
      Redis::bitop_append_comm(
          "OR", BM::EFFECTIVE_RETURN_USERS,
          BM::EFFECTIVE_RETURN_USERS + " " + local_candidate);
      Redis::del_append_comm(local_candidate);
    }
  }
  Redis::free_replies();

  Redis::del_append_comm(r_2_month2);
  Redis::del_append_comm(l_in_month1);
  Redis::del_append_comm(nl_in_month1);
  Redis::del_append_comm(bm_candidate);
  Redis::free_replies();
}

void User::find_old_effective_users(
    const string& str_iso_stat_date,
    ResultTable<TableEffecUserLvDis>::Type& uomResult) {

  date date_it = from_simple_string(game_open_date);  // from open date of game

  string l_in_month = "tm_l_in_month";
  date day_begin_month0;
  date day_end_month0;

  // for each month from begin game to month of statis day
  // find:
  //    effective user of last month and have at least 1 times login in this natural month (old effective)
  //    new effective user of this natural month
  //    back effective usre of this natural month
  // => have effective user of this natural month
  // => loop
  while ((date_it.year() < key_point_date.statis_day.year()
        || (date_it.year() == key_point_date.statis_day.year()
          && date_it.month() < key_point_date.statis_day.month()))) {

    if (Redis::exists(
          Utils::build_key(BM::EFFECTIVE_MONTH,
            to_iso_string(date_it).c_str()))) {
      Redis::del_append_comm(BM::EFFECTIVE_USERS);
      Redis::bitop_append_comm(
          "OR",
          BM::EFFECTIVE_USERS,
          BM::EFFECTIVE_USERS + " "
          + Utils::build_key(BM::EFFECTIVE_MONTH,
            to_iso_string(date_it).c_str()));
      Redis::free_replies();
    } else {
      Redis::del_append_comm(BM::EFFECTIVE_NEW_USERS);
      Redis::del_append_comm(BM::EFFECTIVE_RETURN_USERS);
      Redis::del_append_comm(BM::EFFECTIVE_STAY_USERS);
      Redis::del_append_comm(l_in_month);
      Redis::free_replies();

      day_begin_month0 = date(date_it.year(), date_it.month(), 1);
      day_end_month0 = day_begin_month0.end_of_month();

      find_new_effective_users_of_month(to_iso_string(date_it));
      find_back_effective_users_of_month(to_iso_string(date_it));

      Utils::create_bm_of_period(INDEX::USER, to_iso_string(day_begin_month0),
          to_iso_string(day_end_month0), l_in_month);

      Redis::bitop_append_comm("AND", BM::EFFECTIVE_STAY_USERS,
          l_in_month + " " + BM::EFFECTIVE_USERS);
      Redis::bitop_append_comm(
          "OR",
          BM::EFFECTIVE_USERS,
          BM::EFFECTIVE_NEW_USERS + " " + BM::EFFECTIVE_RETURN_USERS + " "
          + BM::EFFECTIVE_STAY_USERS);
      Redis::bitop_append_comm(
          "OR",
          Utils::build_key(BM::EFFECTIVE_MONTH, to_iso_string(date_it).c_str()),
          BM::EFFECTIVE_USERS);
      Redis::free_replies();
    }
    date_it = date_it + months(1);
  }

  //------------------- last month effective ---------------------------
  // take size
  vector<unsigned> ids_candidate;
  vector<string> info;
  ids_candidate.clear();
  info.clear();

  unsigned bm_size = 0;
  Redis::strlen_comm(BM::EFFECTIVE_USERS);
  bm_size = Redis::reply->integer;
  Redis::free_reply();

  Redis::get_comm(BM::EFFECTIVE_USERS);
  Utils::get_ids_from_bitmap(ids_candidate, Redis::reply->str, bm_size);
  Redis::free_reply();

  for (unsigned i = 0; i < ids_candidate.size(); ++i) {
    Redis::zrevrangebyscore_append_comm(
        INDEX::USER + ":" + Utils::convert<unsigned, string>(ids_candidate[i]),
        str_iso_stat_date, "-inf", "limit 0 1");
  }
  Redis::get_and_free_replies(info);
  statistic_effective_by_level_distributed(info, uomResult,
      eLastMonthEffectiveNum);

  //-------------------- lost effective -------------------
  Redis::del_append_comm(l_in_month);
  Redis::free_replies();

  ids_candidate.clear();
  info.clear();

  string nl_in_month = "tm_nl_in_month";
  string bm_candidate = "tm_bm_candidate";

  day_begin_month0 = date(date_it.year(), date_it.month(), 1);
  Utils::create_bm_of_period(INDEX::USER, to_iso_string(day_begin_month0),
      str_iso_stat_date, l_in_month);

  Redis::bitop_append_comm("NOT", nl_in_month, l_in_month);
  Redis::bitop_append_comm("AND", bm_candidate,
      BM::EFFECTIVE_USERS + " " + nl_in_month);
  Redis::free_replies();

  bm_size = 0;
  Redis::strlen_comm(bm_candidate);
  bm_size = Redis::reply->integer;
  Redis::free_reply();

  Redis::get_comm(bm_candidate);
  Utils::get_ids_from_bitmap(ids_candidate, Redis::reply->str, bm_size);
  Redis::free_reply();

  for (unsigned i = 0; i < ids_candidate.size(); ++i) {
    Redis::zrevrangebyscore_append_comm(
        INDEX::USER + ":" + Utils::convert<unsigned, string>(ids_candidate[i]),
        str_iso_stat_date, "-inf", "limit 0 1");
  }
  Redis::get_and_free_replies(info);
  statistic_effective_by_level_distributed(info, uomResult, eLostEffectiveNum);

  Redis::del_append_comm(l_in_month);
  Redis::del_append_comm(nl_in_month);
  Redis::del_append_comm(bm_candidate);
  Redis::free_replies();

  //------------------- Old effective --------------------------------------
  Redis::del_append_comm(BM::EFFECTIVE_NEW_USERS);
  Redis::del_append_comm(BM::EFFECTIVE_RETURN_USERS);
  Redis::del_append_comm(BM::EFFECTIVE_STAY_USERS);
  Redis::del_append_comm(l_in_month);
  Redis::free_replies();

  day_begin_month0 = date(date_it.year(), date_it.month(), 1);
  day_end_month0 = day_begin_month0.end_of_month();

  Utils::create_bm_of_period(INDEX::USER, to_iso_string(day_begin_month0),
      str_iso_stat_date, l_in_month);

  Redis::bitop_comm("AND", BM::EFFECTIVE_STAY_USERS,
      l_in_month + " " + BM::EFFECTIVE_USERS);
  Redis::free_reply();

  // for TEST wrong eff
  vector<string> account_names;
  Utils::get_account_from_bitmap(account_names, BM::EFFECTIVE_STAY_USERS, "stay_eff_wrong.txt");
  //

  // take size
  ids_candidate.clear();
  info.clear();

  bm_size = 0;
  Redis::strlen_comm(BM::EFFECTIVE_STAY_USERS);
  bm_size = Redis::reply->integer;
  Redis::free_reply();

  Redis::get_comm(BM::EFFECTIVE_STAY_USERS);
  Utils::get_ids_from_bitmap(ids_candidate, Redis::reply->str, bm_size);
  Redis::free_reply();

  for (unsigned i = 0; i < ids_candidate.size(); ++i) {
    Redis::zrevrangebyscore_append_comm(
        INDEX::USER + ":" + Utils::convert<unsigned, string>(ids_candidate[i]),
        str_iso_stat_date, to_iso_string(day_begin_month0), "limit 0 1");
  }
  Redis::get_and_free_replies(info);
  statistic_effective_by_level_distributed(info, uomResult, eOldEffectiveNum);

  Redis::del_append_comm(l_in_month);
  Redis::del_append_comm(BM::EFFECTIVE_STAY_USERS);
  Redis::del_append_comm(BM::EFFECTIVE_USERS);
  Redis::free_replies();
}

void User::fill_tbEffectiveUser_Old(const string& str_iso_date) {
  LOG("\t<user effective>");
  Utils::loadbar(1);

  ResultTable<TableEffecUserLvDis>::Type result_table;

  timestamp_t t0 = get_timestamp();
  find_new_effective_users(str_iso_date, result_table);
  Utils::loadbar(15);
  LOG("\t\t+ new effective users - done in: ",
      (get_timestamp() - t0) / 1000000.0L, "s.");

  t0 = get_timestamp();
  find_num_reg_of_natural_month(str_iso_date, result_table);
  Utils::loadbar(28);
  LOG("\t\t+ num reg of natural month - done in: ",
      (get_timestamp() - t0) / 1000000.0L, "s.");

  t0 = get_timestamp();
  find_old_effective_users(str_iso_date, result_table);
  Utils::loadbar(62);
  LOG("\t\t+ old effective users - done in: ",
      (get_timestamp() - t0) / 1000000.0L, "s.");

  t0 = get_timestamp();
  find_back_effective_users(str_iso_date, result_table);
  Utils::loadbar(79);
  LOG("\t\t+ back effective users - done in: ",
      (get_timestamp() - t0) / 1000000.0L, "s.");

  t0 = get_timestamp();
  for (ResultTable<TableEffecUserLvDis>::Type::iterator sv_lv_it = result_table
      .begin(); sv_lv_it != result_table.end(); ++sv_lv_it) {
    (*sv_lv_it).second.iEffectiveNum = (*sv_lv_it).second.iNewEffectiveNum
      + (*sv_lv_it).second.iOldEffectiveNum
      + (*sv_lv_it).second.iBackEffectiveNum;
  }
  LOG("\t\t+ others effective - done in: ", (get_timestamp() - t0) / 1000000.0L,
      "s.");
  Utils::loadbar(85);

  // create new thread to write to result db
  AuditThread::get_instance().increase_count_thread();
  boost::thread t(&PoolsManager::write_to_tbEffectiveUser,
      &PoolsManager::get_instance(), str_iso_date, result_table);

  LOG("\t<done user effective>");
  Utils::loadbar(100);
  cout << endl;
}

void User::fill_tbEffectiveUser(const std::string& str_iso_date) {
  LOG("\t<user effective>");
  Utils::loadbar(1);
  timestamp_t t0 = get_timestamp();

  ResultTable<TableEffecUserLvDis>::Type result_table;
  vector<string> info;
  date stat_date = from_undelimited_string(str_iso_date);
  date begin_month0 = date(stat_date.year(), stat_date.month(), 1);

  find_new_and_return_effective_users_new(BM::EFFECTIVE_NEW_USERS, 
      BM::EFFECTIVE_RETURN_USERS, str_iso_date, false);
  find_stay_and_lost_and_last_month_effective_users(BM::EFFECTIVE_STAY_USERS, 
      BM::EFFECTIVE_LOST_USERS, BM::EFFECTIVE_LAST_MONTH, str_iso_date);

  Utils::loadbar(25);
  LOG("\t\t+ find bitmap all effective users kind - done in: ",
      (get_timestamp() - t0) / 1000000.0L, "s.");

  // reg in natural month
  t0 = get_timestamp();
  find_num_reg_of_natural_month(str_iso_date, result_table);
  Utils::loadbar(30);
  LOG("\t\t+ num reg of natural month - done in: ",
      (get_timestamp() - t0) / 1000000.0L, "s.");

  // new effective
  t0 = get_timestamp();
  info.clear();
  get_level_info_of_effective_user(BM::EFFECTIVE_NEW_USERS, str_iso_date, to_iso_string(begin_month0), info);
  statistic_effective_by_level_distributed(info, result_table, eNewEffectiveNum);

  Utils::loadbar(35);
  LOG("\t\t+ new effective users - done in: ",
      (get_timestamp() - t0) / 1000000.0L, "s.");

  // return effective
  t0 = get_timestamp();
  info.clear();
  get_level_info_of_effective_user(BM::EFFECTIVE_RETURN_USERS, str_iso_date, to_iso_string(begin_month0), info);
  statistic_effective_by_level_distributed(info, result_table, eBackEffectiveNum);

  Utils::loadbar(45);
  LOG("\t\t+ return effective users - done in: ",
      (get_timestamp() - t0) / 1000000.0L, "s.");

  // last month effective
  t0 = get_timestamp();
  info.clear();
  get_level_info_of_effective_user(BM::EFFECTIVE_LAST_MONTH, str_iso_date, "-inf", info);
  statistic_effective_by_level_distributed(info, result_table, eLastMonthEffectiveNum);

  Utils::loadbar(55);
  LOG("\t\t+ last month effective users - done in: ",
      (get_timestamp() - t0) / 1000000.0L, "s.");

  // lost effective
  t0 = get_timestamp();
  info.clear();
  get_level_info_of_effective_user(BM::EFFECTIVE_LOST_USERS, str_iso_date, "-inf", info);
  statistic_effective_by_level_distributed(info, result_table, eLostEffectiveNum);

  Utils::loadbar(65);
  LOG("\t\t+ lost effective users - done in: ",
      (get_timestamp() - t0) / 1000000.0L, "s.");

  // stay effective
  t0 = get_timestamp();
  info.clear();
  get_level_info_of_effective_user(BM::EFFECTIVE_STAY_USERS, str_iso_date, to_iso_string(begin_month0), info);
  statistic_effective_by_level_distributed(info, result_table, eOldEffectiveNum);

  Utils::loadbar(75);
  LOG("\t\t+ stay effective users - done in: ",
      (get_timestamp() - t0) / 1000000.0L, "s.");

  // effective user
  t0 = get_timestamp();
  for (ResultTable<TableEffecUserLvDis>::Type::iterator sv_lv_it = result_table
      .begin(); sv_lv_it != result_table.end(); ++sv_lv_it) {
    (*sv_lv_it).second.iEffectiveNum = (*sv_lv_it).second.iNewEffectiveNum
      + (*sv_lv_it).second.iOldEffectiveNum
      + (*sv_lv_it).second.iBackEffectiveNum;
  }

  LOG("\t\t+ effective user - done in: ", (get_timestamp() - t0) / 1000000.0L,
      "s.");
  Utils::loadbar(85);

  // create new thread to write to result db
  t0 = get_timestamp();
  AuditThread::get_instance().increase_count_thread();
  boost::thread t(&PoolsManager::write_to_tbEffectiveUser,
      &PoolsManager::get_instance(), str_iso_date, result_table);

  Utils::loadbar(90);
  LOG("\t\t+ write to db - done in: ", (get_timestamp() - t0) / 1000000.0L,
      "s.");

  // write file for test
  /*
     vector<string> account_names;
     account_names.clear();
     Utils::get_account_from_bitmap(account_names, "BM::EFFECTIVE_NEW_USERS", "new_effective_right.txt");
     account_names.clear();
     Utils::get_account_from_bitmap(account_names, "BM::EFFECTIVE_RETURN_USERS", "return_effective_right.txt");
     account_names.clear();
     Utils::get_account_from_bitmap(account_names, "BM::EFFECTIVE_STAY_USERS", "stay_effective_right.txt");
     */
  // write to tbEffectiveUserDetail
  Redis::bitop_comm("OR", BM::EFFECTIVE_USERS, 
      BM::EFFECTIVE_NEW_USERS + " " + 
      BM::EFFECTIVE_STAY_USERS + " " + 
      BM::EFFECTIVE_RETURN_USERS);

  vector<string> account_names;
  account_names.clear();
  Utils::get_account_from_bitmap(account_names, BM::EFFECTIVE_USERS);
  // create new thread to write to result db
  AuditThread::get_instance().increase_count_thread();
  boost::thread t1(&PoolsManager::write_to_tbEffectiveUserDetail,
      &PoolsManager::get_instance(), str_iso_date, account_names);
  LOG("\t\t write to tbEffectiveUserDetail done.");
  Utils::loadbar(95);

  // delete bitmap
  Redis::del_append_comm(BM::EFFECTIVE_USERS);
  Redis::del_append_comm(BM::EFFECTIVE_NEW_USERS);
  Redis::del_append_comm(BM::EFFECTIVE_RETURN_USERS);
  Redis::del_append_comm(BM::EFFECTIVE_LAST_MONTH);
  Redis::del_append_comm(BM::EFFECTIVE_LOST_USERS);
  Redis::del_append_comm(BM::EFFECTIVE_STAY_USERS);
  Redis::free_replies();

  LOG("\t<done user effective>");
  Utils::loadbar(100);
  cout << endl;
}

//================================= statistic level distributed ======================

void User::statistic_active_by_level_distributed(
    std::vector<std::string>& info,
    ResultTable<TableUserLoginLvDis>::Type& result_table,
    FieldsOfUserLoginLvDis field) {
  unsigned server, level;

  result_table.insert(
      std::pair<Keys, TableUserLoginLvDis>(Keys(0, 0), TableUserLoginLvDis()));

  vector<unsigned> store;
  store.clear();
  for (unsigned i = 0; i < info.size(); ++i) {
    Utils::split(info[i], store);
    server = store[0];
    level = store[1];
    //loginTime = store[2];
    store.clear();

    if (result_table.find(Keys(server, level)) == result_table.end()) {
      result_table.insert(
          pair<Keys, TableUserLoginLvDis>(Keys(server, level),
            TableUserLoginLvDis()));
    }
    if (result_table.find(Keys(0, level)) == result_table.end()) {
      result_table.insert(
          pair<Keys, TableUserLoginLvDis>(Keys(0, level),
            TableUserLoginLvDis()));
    }
    result_table[Keys(server, 0)].increase(field);
    result_table[Keys(0, 0)].increase(field);
    result_table[Keys(server, level)].increase(field);
    result_table[Keys(0, level)].increase(field);
  }
}

void User::statistic_effective_by_level_distributed(
    std::vector<std::string>& info,
    ResultTable<TableEffecUserLvDis>::Type& result_table,
    FieldsOfEffectiveUserLvDis field) {
  unsigned server, level;

  result_table.insert(
      std::pair<Keys, TableEffecUserLvDis>(Keys(0, 0), TableEffecUserLvDis()));

  vector<unsigned> store;
  store.clear();
  for (unsigned i = 0; i < info.size(); ++i) {
    Utils::split(info[i], store);
    server = store[0];
    level = store[1];
    //loginTime = store[2];
    store.clear();

    if (result_table.find(Keys(server, level)) == result_table.end()) {
      result_table.insert(
          pair<Keys, TableEffecUserLvDis>(Keys(server, level),
            TableEffecUserLvDis()));
    }
    if (result_table.find(Keys(0, level)) == result_table.end()) {
      result_table.insert(
          pair<Keys, TableEffecUserLvDis>(Keys(0, level),
            TableEffecUserLvDis()));
    }
    result_table[Keys(server, 0)].increase(field);
    result_table[Keys(0, 0)].increase(field);
    result_table[Keys(server, level)].increase(field);
    result_table[Keys(0, level)].increase(field);
  }
}

void User::statistic_frequency_active(
    vector<string>& info,
    ResultTableUseCerberus<TableActivityScaleLvDis>::Type& result_table) {
  unsigned server, level, days;

  vector<unsigned> column;
  column.clear();
  for (unsigned i = 0; i < info.size(); ++i) {
    Utils::split(info[i], column);
    server = column[0];
    level = column[1];
    days = column[column.size() - 1];
    column.clear();

    if (result_table.find(Cerberus(server, level, days))
        == result_table.end()) {
      result_table.insert(
          pair<Cerberus, TableActivityScaleLvDis>(Cerberus(server, level, days),
            TableActivityScaleLvDis()));
    }
    if (result_table.find(Cerberus(0, level, days)) == result_table.end()) {
      result_table.insert(
          pair<Cerberus, TableActivityScaleLvDis>(Cerberus(0, level, days),
            TableActivityScaleLvDis()));
    }
    result_table[Cerberus(server, level, days)].increase();
    result_table[Cerberus(0, level, days)].increase();
  }
}

void User::fill_tbDayRegRegionDis(const string& bm_reg_in_date,
    const string& str_iso_date) {
  unsigned bm_size = 0;
  vector<unsigned> ids;
  vector<unsigned> ips;
  timestamp_t t0 = get_timestamp();

  // get ids user register in statis date
  Redis::strlen_comm(bm_reg_in_date);
  bm_size = Redis::reply->integer;
  Redis::free_reply();

  Redis::get_comm(bm_reg_in_date);
  Utils::get_ids_from_bitmap(ids, Redis::reply->str, bm_size);
  Redis::free_reply();

  Utils::get_ips_from_ids(INDEX::USER, ids, ips, str_iso_date);

  vector<string> store_replies;
  for (unsigned i = 0; i < ips.size(); ++i) {
    Redis::zrangebyscore_append_comm(CONSTANT_KEY::IP2LOC,
        Utils::convert<unsigned, string>(ips[i]),
        "+inf", "limit 0 1");
  }
  Redis::get_and_free_replies(store_replies);

  ResultTable<TableDayRegRegionDis>::Type result_table;
  vector<unsigned> store;
  unsigned province;
  for (unsigned i = 0; i < store_replies.size(); ++i) {
    Utils::split(store_replies[i], store);
    province = store[1];
    store.clear();

    if (result_table.find(Keys(0, province)) == result_table.end()) {
      result_table.insert(
          pair<Keys, TableDayRegRegionDis>(Keys(0, province),
            TableDayRegRegionDis()));
    }
    result_table[Keys(0, province)].increase();
  }

  LOG("\t\t+ day register region dis: ", (get_timestamp() - t0) / 1000000.0L,
      "s.");

  // create new thread to write to result db
  AuditThread::get_instance().increase_count_thread();
  boost::thread t(&PoolsManager::write_to_tbDayRegRegionDis,
      &PoolsManager::get_instance(), str_iso_date, result_table);
}

void User::fill_tbDayLoginRegionDis(const string& str_iso_date) {
  LOG("\t<day login region dis>");
  Utils::loadbar(1);
  //timestamp_t t0 = get_timestamp();

  vector<unsigned> ids;
  vector<unsigned> ips;
  ResultTable<TableDayLoginRegionDis>::Type result_table;
  string bm_login_in_date = Utils::build_key(DATA_TYPE::BITMAP, INDEX::USER,
      str_iso_date);

  vector<string> info;
  Utils::get_info_from_bitmap(bm_login_in_date, info, str_iso_date,
      str_iso_date, INDEX::USER);

  Utils::loadbar(10);
  unsigned server, ip;
  vector<unsigned> store;
  store.clear();
  for (unsigned i = 0; i < info.size(); ++i) {
    Utils::split(info[i], store);
    server = store[0];
    ip = store[3];
    store.clear();

    Redis::zrangebyscore_comm(CONSTANT_KEY::IP2LOC,
        Utils::convert<unsigned, string>(ip), "+inf",
        "limit 0 1");
    if (Redis::reply->elements > 0) {
      Utils::split(Redis::reply->element[0]->str, store);
      Redis::free_reply();

      if (result_table.find(Keys(server, store[1])) == result_table.end()) {
        result_table.insert(
            pair<Keys, TableDayLoginRegionDis>(Keys(server, store[1]),
              TableDayLoginRegionDis()));
      }

      if (result_table.find(Keys(0, store[1])) == result_table.end()) {
        result_table.insert(
            pair<Keys, TableDayLoginRegionDis>(Keys(0, store[1]),
              TableDayLoginRegionDis()));
      }
      result_table[Keys(server, store[1])].increase();
      result_table[Keys(0, store[1])].increase();

      store.clear();
    } else
      Redis::free_reply();
  }
  Utils::loadbar(50);

  // create new thread to write to result db
  AuditThread::get_instance().increase_count_thread();
  boost::thread t(&PoolsManager::write_to_tbDayLoginRegionDis,
      &PoolsManager::get_instance(), str_iso_date, result_table);

  LOG("\t<done day login region dis>");
  Utils::loadbar(100);
  cout << endl;
}

void User::fill_tbEffectActivity(const string& str_iso_date) {
  LOG("\t<effect activity>");
  Utils::loadbar(1);
  timestamp_t t0 = get_timestamp();

  date date_i1, date_i2;
  string key_main = "tm_key_main";
  string key_t1 = "tm_key_t1";
  string bm_name1, bm_name2;

  if (from_undelimited_string(str_iso_date) == key_point_date.day_begin_month0)
    return;

  for (date_i1 = key_point_date.day_begin_month0;
      date_i1 <= key_point_date.statis_day - days(1); date_i1 += days(1)) {
    bm_name1 = Utils::build_key(DATA_TYPE::BITMAP, INDEX::USER,
        to_iso_string(date_i1));
    for (date_i2 = date_i1 + days(1); date_i2 <= key_point_date.statis_day;
        date_i2 += days(1)) {
      bm_name2 = Utils::build_key(DATA_TYPE::BITMAP, INDEX::USER,
          to_iso_string(date_i2));
      Redis::bitop_append_comm("AND", key_t1, bm_name1 + " " + bm_name2);
      Redis::bitop_append_comm("OR", key_main, key_main + " " + key_t1);
    }
  }
  Redis::free_replies();
  Utils::loadbar(35);

  vector<string> info;
  Utils::get_info_from_bitmap(key_main, info,
      to_iso_string(key_point_date.day_begin_month0),
      str_iso_date, INDEX::USER);

  Redis::del_append_comm(key_t1);
  Redis::del_append_comm(key_main);
  Redis::free_replies();
  Utils::loadbar(40);

  ResultTable<TableInEffectActivity>::Type result_table;
  vector<unsigned> store;
  store.clear();
  unsigned server, numdays;
  for (unsigned i = 0; i < info.size(); ++i) {
    Utils::split(info[i], store);
    server = store[0];
    //level = store[1];
    //loginTime = store[2];
    numdays = store[5];
    store.clear();

    if (result_table.find(Keys(server, numdays)) == result_table.end()) {
      result_table.insert(
          pair<Keys, TableInEffectActivity>(Keys(server, numdays),
            TableInEffectActivity()));
    }
    if (result_table.find(Keys(0, numdays)) == result_table.end()) {
      result_table.insert(
          pair<Keys, TableInEffectActivity>(Keys(0, numdays),
            TableInEffectActivity()));
    }
    result_table[Keys(server, numdays)].increase();
    result_table[Keys(0, numdays)].increase();
  }
  info.clear();
  LOG("\t\t+ in effect activity in: ", (get_timestamp() - t0) / 1000000.0L,
      "s.");
  Utils::loadbar(85);

  // create new thread to write to result db
  AuditThread::get_instance().increase_count_thread();
  boost::thread t(&PoolsManager::write_to_tbEffectActivity,
      &PoolsManager::get_instance(), str_iso_date, result_table);

  LOG("\t<done in effect activity>");
  Utils::loadbar(100);
  cout << endl;
}

void User::fill_tbRecoveredUser(const string& str_iso_date) {
  LOG("\t<export recovered user>");
  Utils::loadbar(1);
  vector<string> info;

  // iDayActivityNum
  timestamp_t t0 = get_timestamp();
  ResultTable<TableUserLoginLvDis>::Type result;
  ResultTableUseCerberus<TableActivityScaleLvDis>::Type tbWeekScaleLvDis;

  // iWeekActivityNum
  t0 = get_timestamp();
  string login_w1 = "tm_w1";
  find_active_users(
      login_w1,
      to_iso_string(
        from_undelimited_string(str_iso_date) - days(TIME_CONST::WEEK)),
      str_iso_date, eWeekActivityNum, result, tbWeekScaleLvDis);
  LOG("\t\t+ week activity user: ", (get_timestamp() - t0) / 1000000.0L, "s.");
  Utils::loadbar(10);

  // iWeekLostNum
  t0 = get_timestamp();
  string login_w2 = "tm_w2";
  string lost_w2 = "tm_lost_w2";
  find_lost_active_users(
      login_w1,
      login_w2,
      lost_w2,
      to_iso_string(
        from_undelimited_string(str_iso_date) - days(TIME_CONST::BIWEEK)),
      to_iso_string(
        from_undelimited_string(str_iso_date) - days(TIME_CONST::WEEK + 1)),
      eWeekLostNum, result);
  LOG("\t\t+ week lost user: ", (get_timestamp() - t0) / 1000000.0L, "s.");
  Utils::loadbar(15);

  // iWeekBackNum
  t0 = get_timestamp();
  string login_w3 = "tm_w3";
  string back_w3 = "tm_back_w3";
  find_back_active_users(
      login_w1,
      login_w2,
      login_w3,
      back_w3,
      to_iso_string(
        from_undelimited_string(str_iso_date) - days(TIME_CONST::THWEEK)),
      to_iso_string(
        from_undelimited_string(str_iso_date) - days(TIME_CONST::BIWEEK + 1)),
      str_iso_date, eWeekBackNum, result);
  LOG("\t\t+ week back user: ", (get_timestamp() - t0) / 1000000.0L, "s.");
  Utils::loadbar(20);

  //-------------------------------------------------------------------
  // get ids user register in statis date
  t0 = get_timestamp();
  unsigned bm_size = 0;
  vector<string> account_names;
  Redis::strlen_comm(back_w3);
  bm_size = Redis::reply->integer;
  Redis::free_reply();

  Redis::get_comm(back_w3);
  Utils::get_account_from_bitmap(account_names, Redis::reply->str, bm_size, "abc");
  LOG("\t\t+ find detail back user: ", (get_timestamp() - t0) / 1000000.0L, "s.");
  Utils::loadbar(70);
  // create new thread to write to result db
  /*
     AuditThread::get_instance().increase_count_thread();
     boost::thread t5(&PoolsManager::write_to_tbRecoveredUser,
     &PoolsManager::get_instance(), str_iso_date, account_names);
     LOG("\t\t write to RecoveredUser done.");
     Utils::loadbar(95);
     */
  //---------------------------------------------------------------------

  Redis::del_append_comm(login_w1);
  Redis::del_append_comm(login_w2);
  Redis::del_append_comm(lost_w2);
  Redis::del_append_comm(login_w3);
  Redis::del_append_comm(back_w3);
  Redis::free_replies();

  LOG("\t<done export recovered user>");
  Utils::loadbar(100);
  cout << endl;
}

void User::merge_server(const string& src, const string& dest,
    const string& max_src_id) {
  string key_server_src_template = "hash:" + src + ":*";
  vector<string> list_hash_server_src;

  Redis::search_keys_comm(key_server_src_template);
  //Redis::store_and_free_reply(list_hash_server_src);
  for (size_t i = 0; i < Redis::reply->elements; ++i) {
    list_hash_server_src.push_back(
        Utils::convert<char*, string>(Redis::reply->element[i]->str));
  }
  Redis::free_reply();
  ResultTable<string>::Type list_user;

  for (unsigned i = 0; i < list_hash_server_src.size(); ++i) {
    LOG(list_hash_server_src[i]);
    Redis::hgetall_append_comm(list_hash_server_src[i]);
  }
  Redis::get_and_free_merge_server(list_user);

  unsigned NUMBER_OF_HASH = 0;
  ConfigReader::get_number_of_hash_server(NUMBER_OF_HASH);

  string id = "";
  string role = "";
  string server_hash = "";

  LOG("list user: ", list_user.size());
  for (ResultTable<string>::Type::iterator roleid_uuid_it = list_user.begin();
      roleid_uuid_it != list_user.end(); ++roleid_uuid_it) {

    LOG("before: ", roleid_uuid_it->first.sv_, " ", roleid_uuid_it->first.lv_);
    role = Utils::convert<unsigned, string>(
        roleid_uuid_it->first.sv_
        + Utils::convert<string, unsigned>(max_src_id));
    server_hash = dest + ":"
      + Utils::convert<unsigned, string>(
          Utils::murmur2a(role) % NUMBER_OF_HASH);

    std::string key = Utils::build_key(DATA_TYPE::HASH, server_hash);
    Redis::hset_append_comm(
        key, role, Utils::convert<unsigned, string>(roleid_uuid_it->first.lv_));
    //LOG("after - key:", key, " role: ", role, " oldid: ", roleid_uuid_it->first.lv_, " max: ", Utils::convert<string, unsigned>(max_src_id));
    LOG("after: ", key, " ", role, " ", roleid_uuid_it->first.lv_);
  }
  Redis::free_replies();
}

void User::clear(const std::string& str_iso_stat_date) {
  string bm_user = Utils::build_key(DATA_TYPE::BITMAP, INDEX::USER,
      str_iso_stat_date);
  string bm_deposit = Utils::build_key(DATA_TYPE::BITMAP, INDEX::DEPOSIT,
      str_iso_stat_date);

  // clear user
  unsigned bm_size = 0;
  vector<unsigned> ids;
  // take size
  Redis::strlen_comm(bm_user);
  bm_size = Redis::reply->integer;
  Redis::free_reply();

  Redis::get_comm(bm_user);
  Utils::get_ids_from_bitmap(ids, Redis::reply->str, bm_size);
  Redis::free_reply();
  for (unsigned i = 0; i < ids.size(); ++i) {
    Redis::zremrangebyscore_append_comm(
        INDEX::USER + ":" + Utils::convert<unsigned, string>(ids[i]),
        str_iso_stat_date, str_iso_stat_date);
  }
  Redis::free_replies();

  // clear deposit
  bm_size = 0;
  ids.clear();
  // take size
  Redis::strlen_comm(bm_deposit);
  bm_size = Redis::reply->integer;
  Redis::free_reply();

  Redis::get_comm(bm_deposit);
  Utils::get_ids_from_bitmap(ids, Redis::reply->str, bm_size);
  Redis::free_reply();
  for (unsigned i = 0; i < ids.size(); ++i) {
    Redis::zremrangebyscore_append_comm(
        INDEX::DEPOSIT + ":" + Utils::convert<unsigned, string>(ids[i]),
        str_iso_stat_date, str_iso_stat_date);
  }
  Redis::free_replies();

  Redis::del_append_comm(bm_user);
  Redis::del_append_comm(bm_deposit);
  Redis::free_replies();
}

void User::find_new_effective_users_wrong(
    const string& isoDateStat) {

  if (key_point_date.statis_day - days(config_effective_user.continuous_)
      < key_point_date.day_begin_month0) {
    return;
  }

  date date_iterator0, date_iterator1, date_iterator2, date_iterator3;

  string r2d1 = "tm_r2d1";
  string r2d0 = "tm_r2d0";
  string r_in_d0 = "tm_r_in_d0";
  string r_in_month = "tm_r_in_month";

  vector<unsigned> ids_candidate;
  vector<string> info;
  ids_candidate.clear();
  info.clear();

  string local_candidate = "tm_local_candidate";
  string keys = "";

  Utils::create_bm_of_period(INDEX::USER, "-inf",
      to_iso_string(key_point_date.day_end_month1),
      r2d1);

  Redis::bitop_comm("OR", r2d0, r2d1);
  Redis::free_reply();

  // loop from begin month0 to statis date
  // for each day:
  //    case1: find numbers of user had reg in this day and continuous login 4 day next
  //    case2: find numbers of user had reg in this day and from 6th day after this day
  //            to statistic day, he login at least 2 days.
  for (date_iterator0 = key_point_date.day_begin_month0;
      date_iterator0
      <= key_point_date.statis_day - days(config_effective_user.continuous_);
      date_iterator0 += days(1)) {
    Redis::bitop_append_comm(
        "OR",
        r2d0,
        r2d1 + " "
        + Utils::build_key(DATA_TYPE::BITMAP, INDEX::USER,
          to_iso_string(date_iterator0)));

    Redis::bitop_append_comm("XOR", r_in_d0, r2d1 + " " + r2d0);
    Redis::bitop_append_comm("OR", r_in_month, r_in_month + " " + r_in_d0);

    //----------------  case 1
    // if reg in d0 and login continuous 4 days:
    // reg_d0 AND log_d0 AND log_d1 AND log_d2 AND log_d3 AND log_d4
    keys = "";
    for (unsigned i = 0; i <= config_effective_user.continuous_; ++i) {
      keys += Utils::build_key(DATA_TYPE::BITMAP, INDEX::USER,
          to_iso_string(date_iterator0 + days(i))) + " ";
    }
    keys += r_in_d0;

    Redis::bitop_append_comm("AND", local_candidate, keys);
    Redis::bitop_append_comm("OR", BM::EFFECTIVE_NEW_USERS,
        BM::EFFECTIVE_NEW_USERS + " " + local_candidate);

    Redis::del_append_comm(local_candidate);
    //------------------ case 2
    // from 6th day (day + 5) duyet toi statis day:
    // if from 6th day to statis day have at least 2 day login and reg in this month is effective
    date_iterator1 = date_iterator0 + days(config_effective_user.from_day_n_);
    for (date_iterator2 = date_iterator1 + days(1);
        date_iterator2 <= key_point_date.statis_day;
        date_iterator2 += days(1)) {

      // login_d1 AND login_d2 AND reg_in_month
      keys = Utils::build_key(DATA_TYPE::BITMAP, INDEX::USER,
          to_iso_string(date_iterator1)) + " "
        + Utils::build_key(DATA_TYPE::BITMAP, INDEX::USER,
            to_iso_string(date_iterator2)) + " " + r_in_month;

      Redis::bitop_append_comm("AND", local_candidate, keys);
      Redis::bitop_append_comm("OR", BM::EFFECTIVE_NEW_USERS,
          BM::EFFECTIVE_NEW_USERS + " " + local_candidate);

      Redis::del_append_comm(local_candidate);
    }

    Redis::bitop_append_comm(
        "OR",
        r2d1,
        r2d1 + " "
        + Utils::build_key(DATA_TYPE::BITMAP, INDEX::USER,
          to_iso_string(date_iterator0)));
    Redis::del_append_comm(r_in_d0);
  }
  Redis::free_replies();


  //-------------------------------------------------------------------
  // export effective user
  timestamp_t t0 = get_timestamp();
  unsigned bm_size = 0;
  vector<string> account_names;
  Redis::strlen_comm(BM::EFFECTIVE_NEW_USERS);
  bm_size = Redis::reply->integer;
  Redis::free_reply();

  Redis::get_comm(BM::EFFECTIVE_NEW_USERS);
  Utils::get_account_from_bitmap(account_names, Redis::reply->str, bm_size, "effective_user_wrong.txt");
  LOG("\t\t+ find wrong detail effective user: ", (get_timestamp() - t0) / 1000000.0L, "s.");
  Utils::loadbar(70);
  // create new thread to write to result db
  /*
     AuditThread::get_instance().increase_count_thread();
     boost::thread t5(&PoolsManager::write_to_tbRecoveredUser,
     &PoolsManager::get_instance(), str_iso_date, account_names);
     LOG("\t\t write to RecoveredUser done.");
     Utils::loadbar(95);
     */
  //---------------------------------------------------------------------

  Redis::del_append_comm(r2d1);
  Redis::del_append_comm(r2d0);
  Redis::del_append_comm(r_in_d0);
  //Redis::del_append_comm(r_in_month);
  Redis::del_append_comm(local_candidate);
  Redis::del_append_comm(BM::EFFECTIVE_NEW_USERS);
  Redis::free_replies();
}

void User::find_user_have_state_effective(
    const std::string& str_iso_date, 
    const std::string& dest_key, 
    const bool& is_stat_month) {

  date stat_date = from_undelimited_string(str_iso_date);
  date begin_stat;
  date end_stat;
  unsigned stat_month = stat_date.month();

  begin_stat = date(stat_date.year(), stat_date.month(), 1);
  if (!is_stat_month) {
    end_stat = stat_date;
  } else {
    end_stat = stat_date.end_of_month() + days(4);
  }

  map<unsigned, LoginBitmap> active_user;
  string key_redis;
  unsigned nth;
  for (date date_iter = begin_stat; date_iter <= end_stat; date_iter += days(1)) {
    key_redis = Utils::build_key(DATA_TYPE::BITMAP, INDEX::USER, to_iso_string(date_iter));
    nth = date_iter.day();
    if (date_iter.month() != stat_month) {
      nth += stat_date.end_of_month().day();    
    }
    Utils::set_login_to_user_bitmap(active_user, key_redis, nth);
  }

  // regex
  //regex e1("0*11111.*....");
  //regex e2("0*1.....*1.*1.*");
  regex e1, e2;
  string pattern1, pattern2;
  if (!is_stat_month) {
    pattern1 = "0*11111.*";
    pattern2 = "0*1.....*1.*1.*";
  } else {
    pattern1 = "0*11111.*";
    pattern2 = "0*1.....*1.*1.*";
    for (unsigned i = stat_date.end_of_month().day() + 1; i <= 35; ++i) {
      pattern2 += "0";
    }
  }
  e1.assign(pattern1);
  e2.assign(pattern2);
  vector<unsigned> ids;
  string bm_login;
  for (map<unsigned, LoginBitmap>::iterator iter = active_user.begin(); iter != active_user.end(); ++iter) {
    bm_login = (*iter).second.bitmap.to_string();
    //reverse(bm_login.begin(), bm_login.end());
    if (regex_match(bm_login.begin(), bm_login.end(), e1) || regex_match(bm_login.begin(), bm_login.end(), e2)) {
      ids.push_back((*iter).first);
    }
  }
  Utils::create_bitmap_by_list(ids, dest_key);
}

void User::find_new_and_return_effective_users_new(
    const std::string& new_effective_user_key,
    const std::string& return_effective_user_key,
    const std::string& str_iso_stat_date, 
    const bool& is_stat_month) {

  date stat_date = from_undelimited_string(str_iso_stat_date);
  date begin_stat = date(stat_date.year(), stat_date.month(), 1);
  date end_stat = stat_date;

  if (!is_stat_month) {
    end_stat = stat_date;
  } else {
    end_stat = stat_date.end_of_month();
  }

  // new effective user
  string bm_reg_in_month = "bm_reg_in_monthweoihfg";
  Utils::create_bm_reg_in(to_iso_string(begin_stat), to_iso_string(end_stat), bm_reg_in_month);

  string bm_candidate_effective = "bm_candidate_effectivehewhncxkv";
  User::find_user_have_state_effective(str_iso_stat_date, bm_candidate_effective, is_stat_month);

  Redis::bitop_comm("AND", new_effective_user_key, bm_candidate_effective + " " + bm_reg_in_month);
  Redis::free_reply();

  // return effective user
  date end_l2m = (stat_date - months(2)).end_of_month();
  date begin_lm = end_l2m + days(1);
  date end_lm = begin_lm.end_of_month();
  string bm_reg_to_l2m = "bm_reg_to_l2mkhew34u";

  Utils::create_bm_of_period(INDEX::USER, "-inf", to_iso_string(end_l2m), bm_reg_to_l2m);
  string bm_login_in_last_month = "bm_login_in_last_monthkjewhrwhi43u";
  Utils::create_bm_of_period(INDEX::USER, to_iso_string(begin_lm), to_iso_string(end_lm), bm_login_in_last_month);
  string bm_not_login_in_last_month = "bm_not_login_in_last_monthwketroi32o";

  Redis::bitop_append_comm("NOT", bm_not_login_in_last_month, bm_login_in_last_month);
  Redis::bitop_append_comm("AND", return_effective_user_key, bm_candidate_effective + " " + bm_reg_to_l2m + " " + bm_not_login_in_last_month);
  Redis::free_replies();

  Redis::del_append_comm(bm_reg_in_month);
  Redis::del_append_comm(bm_candidate_effective);

  Redis::del_append_comm(bm_reg_to_l2m);
  Redis::del_append_comm(bm_login_in_last_month);
  Redis::del_append_comm(bm_not_login_in_last_month);

  Redis::free_replies();
}

void User::find_stay_and_lost_and_last_month_effective_users(
    const std::string& stay_effective_user_key,
    const std::string& lost_effective_user_key,
    const std::string& last_month_effective_user_key,
    const std::string& str_iso_stat_date) {

  date date_open_game = from_simple_string(game_open_date);  // from open date of game
  date stat_date = from_undelimited_string(str_iso_stat_date);
  date begin_stat = date(stat_date.year(), stat_date.month(), 1);
  date end_stat = stat_date;

  string bm_effective_user_of_last_month = Utils::build_key(
      BM::EFFECTIVE_MONTH, 
      to_iso_string(date((stat_date - months(1)).year(), 
          (stat_date - months(1)).month(), 
          date_open_game.day())).c_str());

  if (!Redis::exists(bm_effective_user_of_last_month)) {
    string new_effective_user_last_month_key = "new_effective_user_last_month_keyewhi8w";
    string return_effective_user_last_month_key = "return_effective_user_last_month_keyshiew32";
    string stay_effective_user_last_month_key = "stay_effective_user_last_month_keyaskthewo32";

    date begin_lm = date((stat_date - months(1)).year(), (stat_date - months(1)).month(), 1);
    date end_lm = begin_lm.end_of_month();

    find_new_and_return_effective_users_new(
        new_effective_user_last_month_key,
        return_effective_user_last_month_key,
        to_iso_string((stat_date - months(1)).end_of_month()),
        true);

    string bm_effective_user_of_last_last_month = Utils::build_key(
        BM::EFFECTIVE_MONTH, 
        to_iso_string(date((stat_date - months(2)).year(), 
            (stat_date - months(2)).month(), 
            date_open_game.day())).c_str());

    string bm_login_in_last_month = "bm_login_in_last_montheiu39ri3hj";
    Utils::create_bm_of_period(INDEX::USER, to_iso_string(begin_lm), to_iso_string(end_lm), bm_login_in_last_month);
    Redis::bitop_append_comm("AND", 
        stay_effective_user_last_month_key, 
        bm_effective_user_of_last_last_month + " " + bm_login_in_last_month); 

    Redis::bitop_append_comm("OR", 
        bm_effective_user_of_last_month, 
        new_effective_user_last_month_key + " " + 
        return_effective_user_last_month_key + " " +
        stay_effective_user_last_month_key);

    Redis::del_append_comm(new_effective_user_last_month_key);
    Redis::del_append_comm(return_effective_user_last_month_key);
    Redis::del_append_comm(stay_effective_user_last_month_key);
    Redis::del_append_comm(bm_login_in_last_month);
    Redis::free_replies();
  }

  string bm_login_in_month = "bm_login_in_monthoiewu83h";
  Utils::create_bm_of_period(INDEX::USER, to_iso_string(begin_stat), to_iso_string(end_stat), bm_login_in_month);
  string bm_not_login_in_month = "bm_not_login_in_monthiwuyr928";
  Redis::bitop_comm("NOT", bm_not_login_in_month, bm_login_in_month);
  Redis::free_reply();

  Redis::bitop_append_comm("AND", stay_effective_user_key, bm_effective_user_of_last_month + " " + bm_login_in_month);
  Redis::bitop_append_comm("OR", last_month_effective_user_key, bm_effective_user_of_last_month);
  Redis::bitop_append_comm("AND", lost_effective_user_key, bm_effective_user_of_last_month + " " + bm_not_login_in_month);
  Redis::free_replies();

  Redis::del_append_comm(bm_login_in_month);
  Redis::del_append_comm(bm_not_login_in_month);
  if (stat_date.day() < 5) {
    Redis::del_append_comm(bm_effective_user_of_last_month);
  }
  Redis::free_replies();
}

void User::get_level_info_of_effective_user(const string& key_name, 
    const string& str_iso_date_start, 
    const string& str_iso_date_end,
    vector<string>& info) {

  // have bitmap new effective user, find detail
  unsigned bm_size = 0;

  Redis::strlen_comm(key_name);
  bm_size = Redis::reply->integer;
  Redis::free_reply();

  vector<unsigned> ids_candidate;
  Redis::get_comm(key_name);
  Utils::get_ids_from_bitmap(ids_candidate, Redis::reply->str, bm_size);
  Redis::free_reply();

  for (unsigned i = 0; i < ids_candidate.size(); ++i) {
    Redis::zrevrangebyscore_append_comm(
        INDEX::USER + ":" + Utils::convert<unsigned, string>(ids_candidate[i]),
        str_iso_date_start, str_iso_date_end,
        "limit 0 1");
  }

  Redis::get_and_free_replies(info);
}
