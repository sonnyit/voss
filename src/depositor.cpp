/*
 * depositor.cpp
 *
 *  Created on: May 13, 2013
 *      Author: minhnh3
 */

#ifndef LOGGING_LEVEL_1
#define LOGGING_LEVEL_1
#endif

#include "depositor.h"
#include "utils.h"
#include "pools_manager.h"
#include "redis.h"
#include "config_reader.h"
#include "logger.h"
#include <boost/shared_ptr.hpp>
#include <boost/thread.hpp>
#include "audit_thread.h"

using namespace std;
using namespace boost::gregorian;

/*!\file depositor.cpp
 * \brief definition all method of class depositor
 *
 * \warning need handle exception (may have bug if be terminate when runtime)
 */

Depositor::Depositor() {
}

Depositor::~Depositor() {
}

void Depositor::get_data(const string& str_iso_date, const string& index) {
  Base::get_data(str_iso_date, index);
}

void Depositor::statistic(const string& str_iso_date) {
  LOG("--------------------------- <DEPOSIT INDEX> ++ <", str_iso_date,
      "> ------------------------");
  cout << "<DEPOSIT INDEX> <" << str_iso_date << ">:\n";

  cout << "get data:\n";
  get_data(str_iso_date);

  cout << "depositor:\n";
  fill_tbDepositors(str_iso_date);

  cout << "new depositor:\n";
  fill_tbNewDepositors(str_iso_date);

  cout << "store game point:\n";
  fill_tbStoreGamePoints(str_iso_date);

  // check if any thread not done
  while (AuditThread::get_instance().get_count_thread() != 0)
  {
    if (!AuditThread::get_instance().wait(1))
      break;
  }
  AuditThread::get_instance().reset_wait_time();

  LOG("-----------------------------<DONE DEPOSIT INDEX> -----------------------------");
  cout << "<DONE DEPOSIT INDEX>\n";
}

/*!\copydoc Depositor::find_depositors()
 *
 * find depositor by using bitmap in redis database
 * on this bitmap, get offset of all bit on (offset is depositor id)
 * get value from sorted set corresponding and find
 * which depositor in which server/level
 */
void Depositor::find_depositors(const std::string& dest_key,
                                const std::string& iso_date_statistic,
                                const unsigned& observation,
                                ResultTable<TableDepositors>::Type& result,
                                FieldsOfDepositors field) {
  vector<string> info;

  Utils::create_bm_of_period(
      INDEX::DEPOSIT,
      Utils::shift_date(iso_date_statistic, observation - 1, true),
      iso_date_statistic, dest_key);
  Utils::get_info_from_bitmap(
      dest_key, info,
      Utils::shift_date(iso_date_statistic, observation - 1, true),
      iso_date_statistic, INDEX::DEPOSIT);

  depositors_by_level_distributed(info, result, field);
}

/*!\copydoc Depositor::find_new_depositors()
 *
 * find new depositor by using bitmap in redis database
 * on this bitmap, get offset of all bit on (offset is depositor id)
 * get value from sorted set corresponding and find
 * which depositor in which server/level
 */
void Depositor::find_new_depositors(
    const std::string& dest_key, const std::string& iso_date_statistic,
    const unsigned& observation, ResultTable<TableNewDepositors>::Type& result,
    FieldsOfNewDepositors field) {
  vector<string> info;

  string all_depositors = "tm_all_depositors_jhsdif32u";
  Utils::create_bm_of_period(INDEX::DEPOSIT, "-inf", iso_date_statistic,
                             all_depositors);

  string depositors_last = "tm_depositors_last_jasofi0ewe";
  Utils::create_bm_of_period(
      INDEX::DEPOSIT, "-inf",
      Utils::shift_date(iso_date_statistic, observation, true),
      depositors_last);

  Redis::bitop_comm("XOR", dest_key, all_depositors + " " + depositors_last);
  Redis::free_reply();

  Utils::get_info_from_bitmap(
      dest_key, info, Utils::shift_date(iso_date_statistic, observation, true),
      iso_date_statistic, INDEX::DEPOSIT);

  new_depositors_by_level_distributed(info, result, field);

  Redis::del_append_comm(all_depositors);
  Redis::del_append_comm(depositors_last);
  Redis::free_replies();
}

/*!\copydoc Depositor::find_depositors_lost()
 *
 * \TODO need update
 */
void Depositor::find_depositors_lost(const std::string& dest_key,
                                     const std::string& iso_date_statistic,
                                     const unsigned& observation,
                                     ResultTable<TableDepositors>::Type& result,
                                     FieldsOfDepositors field) {

  using namespace Utils;
  vector<string> info;

  string depositors_circle_2 = "tm_depositors_circle_2_walrjelknnhwvka";
  string depositors_circle_1 = "tm_depositors_circle_1_walrjelknnhwvka";

  unsigned half = observation / 2;
  create_bm_of_period(
      INDEX::DEPOSIT,
      Utils::shift_date(iso_date_statistic, observation - 1, true),
      Utils::shift_date(iso_date_statistic, half, true), depositors_circle_2);

  create_bm_of_period(INDEX::DEPOSIT,
                      Utils::shift_date(iso_date_statistic, half - 1, true),
                      iso_date_statistic, depositors_circle_1);

  string depositors_both_circle = "tm_depositors_both_circle_lfjewoiew3";
  Redis::bitop_append_comm("AND", depositors_both_circle,
                           depositors_circle_1 + " " + depositors_circle_2);
  Redis::bitop_append_comm("XOR", dest_key,
                           depositors_both_circle + " " + depositors_circle_2);
  Redis::free_replies();

  Utils::get_info_from_bitmap(
      dest_key, info, shift_date(iso_date_statistic, observation - 1, true),
      iso_date_statistic, INDEX::DEPOSIT);

  depositors_by_level_distributed(info, result, field);

  Redis::del_append_comm(depositors_circle_2);
  Redis::del_append_comm(depositors_circle_1);
  Redis::del_append_comm(depositors_both_circle);
  Redis::free_replies();
}

/*!\copydoc Depositor::find_depositors_back()
 *
 * \TODO need update
 */
void Depositor::find_depositors_back(const std::string& dest_key,
                                     const std::string& iso_date_statistic,
                                     const unsigned& observation,
                                     ResultTable<TableDepositors>::Type& result,
                                     FieldsOfDepositors field) {
  using namespace Utils;
  vector<string> info;

  string depositors_circle_3 = "tm_depositors_circle_2_cxlkjdew32";
  string depositors_circle_2 = "tm_depositors_circle_2_dsajofew98";
  string depositors_circle_1 = "tm_depositors_circle_1_xzdssf9832";

  create_bm_of_period(
      INDEX::DEPOSIT,
      Utils::shift_date(iso_date_statistic, observation - 1, true),
      Utils::shift_date(iso_date_statistic, (observation * 2 / 3), true),
      depositors_circle_3);

  create_bm_of_period(
      INDEX::DEPOSIT,
      Utils::shift_date(iso_date_statistic, (observation * 2 / 3) - 1, true),
      Utils::shift_date(iso_date_statistic, (observation / 3), true),
      depositors_circle_2);

  create_bm_of_period(
      INDEX::DEPOSIT,
      Utils::shift_date(iso_date_statistic, (observation / 3) - 1, true),
      iso_date_statistic, depositors_circle_1);

  string temp1 = "tm_kdsalfjlsdjf234sdf";
  string temp2 = "tm_salkfjufoiewjroi32";

  Redis::bitop_append_comm("AND", temp1,
                           depositors_circle_2 + " " + depositors_circle_3);
  Redis::bitop_append_comm("XOR", temp2, temp1 + " " + depositors_circle_3);
  Redis::bitop_append_comm("AND", dest_key, temp2 + " " + depositors_circle_1);
  Redis::free_replies();

  Utils::get_info_from_bitmap(
      dest_key, info, shift_date(iso_date_statistic, observation - 1, true),
      iso_date_statistic, INDEX::DEPOSIT);
  depositors_by_level_distributed(info, result, field);

  Redis::del_append_comm(depositors_circle_3);
  Redis::del_append_comm(depositors_circle_2);
  Redis::del_append_comm(depositors_circle_1);
  Redis::del_append_comm(temp1);
  Redis::del_append_comm(temp2);
  Redis::free_replies();
}

/*!\copydoc Depositor::depositors_by_level_distributed()
 *
 * \TODO need update
 */
void Depositor::depositors_by_level_distributed(
    std::vector<std::string>& info,
    ResultTable<TableDepositors>::Type& result_table,
    FieldsOfDepositors field) {
  unsigned server, level;

  result_table.insert(
      std::pair<Keys, TableDepositors>(Keys(0, 0), TableDepositors()));

  vector<unsigned> store;
  store.clear();
  for (unsigned i = 0; i < info.size(); ++i) {
    Utils::split(info[i], store);
    //money = store[0];
    server = store[1];
    level = store[2];
    //loginTime = store[3];
    store.clear();

    if (result_table.find(Keys(server, level)) == result_table.end()) {
      result_table.insert(
          pair<Keys, TableDepositors>(Keys(server, level), TableDepositors()));
    }
    if (result_table.find(Keys(0, level)) == result_table.end()) {
      result_table.insert(
          pair<Keys, TableDepositors>(Keys(0, level), TableDepositors()));
    }
    result_table[Keys(server, 0)].increase(field);
    result_table[Keys(0, 0)].increase(field);
    result_table[Keys(server, level)].increase(field);
    result_table[Keys(0, level)].increase(field);
  }
}

/*!\copydoc Depositor::new_depositors_by_level_distributed()
 *
 * \TODO need update
 */
void Depositor::new_depositors_by_level_distributed(
    std::vector<std::string>& info,
    ResultTable<TableNewDepositors>::Type& result_table,
    FieldsOfNewDepositors field) {
  unsigned server, level;

  result_table.insert(
      std::pair<Keys, TableNewDepositors>(Keys(0, 0), TableNewDepositors()));

  vector<unsigned> store;
  store.clear();
  for (unsigned i = 0; i < info.size(); ++i) {
    Utils::split(info[i], store);
    //money = store[0];
    server = store[1];
    level = store[2];
    //loginTime = store[3];
    store.clear();

    if (result_table.find(Keys(server, level)) == result_table.end()) {
      result_table.insert(
          pair<Keys, TableNewDepositors>(Keys(server, level),
                                         TableNewDepositors()));
    }
    if (result_table.find(Keys(0, level)) == result_table.end()) {
      result_table.insert(
          pair<Keys, TableNewDepositors>(Keys(0, level), TableNewDepositors()));
    }
    result_table[Keys(server, 0)].increase(field);
    result_table[Keys(0, 0)].increase(field);
    result_table[Keys(server, level)].increase(field);
    result_table[Keys(0, level)].increase(field);
  }
}

/*!\copydoc Depositor::fill_tbDepositors(const string&)
 *
 * \TODO need update
 */
void Depositor::fill_tbDepositors(const string& str_iso_date) {
  LOG("\t<depositor>");

  ResultTable<TableDepositors>::Type result;
  TableDepositors tbDepositors;
  string all_depositor_num = "tm_all_depositor_num_dskf32";
  string day_depositor_num = "tm_day_depositor_num_3432kjh";
  string week_depositor_num = "tm_week_depositor_num_32jekwn";
  string dweek_depositor_num = "tm_dweek_depositor_num_3o2lkjdsk";
  string month_depositor_num = "tm_month_depositor_num_ldsjfoiw2";
  string dmonth_depositor_num = "tm_dmonth_depositor_num_lkdsjfa";
  string week_lost_num = "tm_week_lost_num_lksdjo3";
  string dweek_lost_num = "tm_dweek_lost_num_slkjfoi30";
  string month_lost_num = "tm_month_lost_num_jjofds";
  string week_back_num = "tm_week_back_num_odsfjowe0";
  string dweek_back_num = "tm_dweek_back_num_oijweoifwoi3";
  string month_back_num = "tm_month_back_num_sdiupo39";

  timestamp_t t0 = get_timestamp();
  find_depositors(all_depositor_num, str_iso_date, 10000, result,
                  eAllDepositorNum);
  LOG("\t\t+ all depositor: ", (get_timestamp() - t0) / 1000000.0L, "s.");
  Utils::loadbar(12);

  t0 = get_timestamp();
  find_depositors(day_depositor_num, str_iso_date, 1, result, eDayDepositorNum);
  LOG("\t\t+ day depositor: ", (get_timestamp() - t0) / 1000000.0L, "s.");
  Utils::loadbar(15);

  t0 = get_timestamp();
  find_depositors(week_depositor_num, str_iso_date, 7, result,
                  eWeekDepositorNum);
  LOG("\t\t+ week depositor: ", (get_timestamp() - t0) / 1000000.0L, "s.");
  Utils::loadbar(21);

  t0 = get_timestamp();
  find_depositors(dweek_depositor_num, str_iso_date, 14, result,
                  eDWeekDepositorNum);
  LOG("\t\t+ double week depositor: ", (get_timestamp() - t0) / 1000000.0L,
      "s.");
  Utils::loadbar(30);

  t0 = get_timestamp();
  find_depositors(month_depositor_num, str_iso_date, 30, result,
                  eMonthDepositorNum);
  LOG("\t\t+ month depositor: ", (get_timestamp() - t0) / 1000000.0L, "s.");
  Utils::loadbar(43);

  t0 = get_timestamp();
  find_depositors(dmonth_depositor_num, str_iso_date, 60, result,
                  eDMonthDepositorNum);
  LOG("\t\t+ double month depositor: ", (get_timestamp() - t0) / 1000000.0L,
      "s.");
  Utils::loadbar(57);

  t0 = get_timestamp();
  find_depositors_lost(week_lost_num, str_iso_date, 14, result,
                       eWeekDepositorLostNum);
  LOG("\t\t+ week lost depositor: ", (get_timestamp() - t0) / 1000000.0L, "s.");
  Utils::loadbar(60);

  t0 = get_timestamp();
  find_depositors_lost(dweek_lost_num, str_iso_date, 28, result,
                       eDWeekDepositorLostNum);
  LOG("\t\t+ double week lost depositor: ", (get_timestamp() - t0) / 1000000.0L,
      "s.");
  Utils::loadbar(65);

  t0 = get_timestamp();
  find_depositors_lost(month_lost_num, str_iso_date, 60, result,
                       eMonthDepositorLostNum);
  LOG("\t\t+ month lost depositor: ", (get_timestamp() - t0) / 1000000.0L,
      "s.");
  Utils::loadbar(70);

  t0 = get_timestamp();
  find_depositors_back(week_back_num, str_iso_date, 21, result,
                       eWeekDepositorBackNum);
  LOG("\t\t+ week back depositor: ", (get_timestamp() - t0) / 1000000.0L, "s.");
  Utils::loadbar(74);

  t0 = get_timestamp();
  find_depositors_back(dweek_back_num, str_iso_date, 42, result,
                       eDWeekDepositorBackNum);
  LOG("\t\t+ double week back depositor: ", (get_timestamp() - t0) / 1000000.0L,
      "s.");
  Utils::loadbar(80);

  t0 = get_timestamp();
  find_depositors_back(month_back_num, str_iso_date, 90, result,
                       eMonthDepositorBackNum);
  LOG("\t\t+ month back depositor: ", (get_timestamp() - t0) / 1000000.0L,
      "s.");
  Utils::loadbar(88);

  //---------------------- delelte bitmap temp -----------------------------------

  Redis::del_append_comm(all_depositor_num);
  Redis::del_append_comm(day_depositor_num);
  Redis::del_append_comm(week_depositor_num);
  Redis::del_append_comm(dweek_depositor_num);
  Redis::del_append_comm(month_depositor_num);
  Redis::del_append_comm(dmonth_depositor_num);
  Redis::del_append_comm(week_lost_num);
  Redis::del_append_comm(dweek_lost_num);
  Redis::del_append_comm(month_lost_num);
  Redis::del_append_comm(week_back_num);
  Redis::del_append_comm(dweek_back_num);
  Redis::del_append_comm(month_back_num);
  Redis::free_replies();

  // create new thread to write to result db
  AuditThread::get_instance().increase_count_thread();
  boost::thread t(&PoolsManager::write_to_tbDepositors,
                  &PoolsManager::get_instance(), str_iso_date, result);

  LOG("\t<done depositor>");
  Utils::loadbar(100);
  cout << endl;
}

/*!\copydoc Depositor::fill_tbNewDepositors(const string&)
 *
 * \TODO need update
 */
void Depositor::fill_tbNewDepositors(const string& str_iso_date) {
  LOG("\t<new depositor>");
  Utils::loadbar(1);

  ResultTable<TableNewDepositors>::Type result;
  TableNewDepositors tbNewDepositors;
  string day_new_depositor_num = "tm_day_new_depositor_num_dskf32";
  string week_new_depositor_num = "tm_week_new_depositor_num_3432kjh";
  string dweek_new_depositor_num = "tm_dweek_new_depositor_num_32jekwn";
  string month_new_depositor_num = "tm_month_new_depositor_num_3o2lkjdsk";

  timestamp_t t0 = get_timestamp();
  find_new_depositors(day_new_depositor_num, str_iso_date, 1, result,
                      eDayNewDepositorNum);
  LOG("\t\t+ day new depositor: ", (get_timestamp() - t0) / 1000000.0L, "s.");
  Utils::loadbar(15);

  t0 = get_timestamp();
  find_new_depositors(week_new_depositor_num, str_iso_date, 7, result,
                      eWeekNewDepositorNum);
  LOG("\t\t+ week new depositor: ", (get_timestamp() - t0) / 1000000.0L, "s.");
  Utils::loadbar(31);

  t0 = get_timestamp();
  find_new_depositors(dweek_new_depositor_num, str_iso_date, 14, result,
                      eDWeekNewDepositorNum);
  LOG("\t\t+ double week new depositor: ", (get_timestamp() - t0) / 1000000.0L,
      "s.");
  Utils::loadbar(49);

  t0 = get_timestamp();
  find_new_depositors(month_new_depositor_num, str_iso_date, 30, result,
                      eMonthNewDepositorNum);
  LOG("\t\t+ month new depositor: ", (get_timestamp() - t0) / 1000000.0L, "s.");
  Utils::loadbar(70);

  Redis::del_append_comm(day_new_depositor_num);
  Redis::del_append_comm(week_new_depositor_num);
  Redis::del_append_comm(dweek_new_depositor_num);
  Redis::del_append_comm(month_new_depositor_num);
  Redis::free_replies();

  // create new thread to write to result db
  AuditThread::get_instance().increase_count_thread();
  boost::thread t(&PoolsManager::write_to_tbNewDepositors,
                  &PoolsManager::get_instance(), str_iso_date, result);

  LOG("\t<done new depositor>");
  Utils::loadbar(100);
  cout << endl;
}

//================================ store game point ====================================

void Depositor::fill_tbStoreGamePoints(const string& str_iso_date) {
  LOG("\t<store game points>");
  Utils::loadbar(1);

  string bitmap_name = Utils::build_key(DATA_TYPE::BITMAP, INDEX::DEPOSIT,
                                        str_iso_date);
  vector<string> info;
  ResultTable<TableStoreGamePoints>::Type result_table;
  ResultTable<TableStoreGamePoints>::Type gamepoint_seg_table;

  vector<unsigned> segment_table;
  string str_segment = "";
  ConfigReader::get_gamepoint_seg_conf(str_segment);
  Utils::split(str_segment, segment_table);

  Utils::get_info_from_bitmap(bitmap_name, info, str_iso_date, str_iso_date,
                              INDEX::DEPOSIT);
  Utils::loadbar(30);

  vector<unsigned> column;
  unsigned seg = 0;
  for (unsigned i = 0; i < info.size(); ++i) {
    Utils::split(info[i], column);
    seg = Utils::find_gamepoint_seg(segment_table, column[0]);

    if (result_table.find(Keys(column[1], 0)) == result_table.end()) {
      result_table.insert(
          pair<Keys, TableStoreGamePoints>(Keys(column[1], 0),
                                           TableStoreGamePoints()));
    }

    if (result_table.find(Keys(0, 0)) == result_table.end()) {
      result_table.insert(
          pair<Keys, TableStoreGamePoints>(Keys(0, 0), TableStoreGamePoints()));
    }

    if (gamepoint_seg_table.find(Keys(column[1], seg))
        == gamepoint_seg_table.end()) {
      gamepoint_seg_table.insert(
          pair<Keys, TableStoreGamePoints>(Keys(column[1], seg),
                                           TableStoreGamePoints()));
    }

    if (gamepoint_seg_table.find(Keys(0, seg)) == gamepoint_seg_table.end()) {
      gamepoint_seg_table.insert(
          pair<Keys, TableStoreGamePoints>(Keys(0, seg),
                                           TableStoreGamePoints()));
    }

    result_table[Keys(column[1], 0)].increaseUserNum();
    result_table[Keys(0, 0)].increaseUserNum();

    result_table[Keys(column[1], 0)].addGamePoints(column[0]);
    result_table[Keys(0, 0)].addGamePoints(column[0]);

    result_table[Keys(column[1], 0)].addStoreTimes(column[3]);
    result_table[Keys(0, 0)].addStoreTimes(column[3]);

    gamepoint_seg_table[Keys(column[1], seg)].increaseUserNum();
    gamepoint_seg_table[Keys(0, seg)].increaseUserNum();

    gamepoint_seg_table[Keys(column[1], seg)].addGamePoints(column[0]);
    gamepoint_seg_table[Keys(0, seg)].addGamePoints(column[0]);

    column.clear();
  }
  Utils::loadbar(70);

  // create new thread to write to result db
  AuditThread::get_instance().increase_count_thread();
  boost::thread t1(&PoolsManager::write_to_tbStoreGamePoints,
                   &PoolsManager::get_instance(), str_iso_date, result_table);

  AuditThread::get_instance().increase_count_thread();
  boost::thread t2(&PoolsManager::write_to_tbStoreGamePointsUserDis,
                   &PoolsManager::get_instance(), str_iso_date,
                   gamepoint_seg_table);

  LOG("\t<done store game point>");
  Utils::loadbar(100);
  cout << endl;
}

