/*
 * pools_manager.cpp
 *
 *  Created on: May 3, 2013
 *      Author: minhnh3
 */

#ifndef LOGGING_LEVEL_1
#define LOGGING_LEVEL_1
#endif

#include "pools_manager.h"
#include "config_reader.h"
#include "logger.h"
#include "audit_thread.h"
#include "utils.h"

#include <string>

using namespace std;

bool PoolsManager::destroyed_ = false;

PoolsManager::PoolsManager() {
  LOG("Constructor of pool connection manager");

  string url_src_db, url_result_db, user_name_src_db, user_name_result_db,
      password_src_db, password_result_db, db_name_src, db_name_result;
  ConfigReader::get_db_config(url_src_db, url_result_db, user_name_src_db,
                              user_name_result_db, password_src_db,
                              password_result_db, db_name_src, db_name_result);
  url_src_db_ = url_src_db;
  url_result_db_ = url_result_db;
  user_name_src_db_ = user_name_src_db;
  user_name_result_db_ = user_name_result_db;
  password_src_db_ = password_src_db;
  password_result_db_ = password_result_db;
  db_name_src_db_ = db_name_src;
  db_name_result_db_ = db_name_result;
  max_size_pool_src_db_ = 4;
  max_size_pool_result_db_ = 10;

  source_db_connections_.reset(
      new ConnectionPool(url_src_db_, user_name_src_db_, password_src_db_,
                         max_size_pool_src_db_));
  result_db_connections_.reset(
      new ConnectionPool(url_result_db_, user_name_result_db_,
                         password_result_db_, max_size_pool_result_db_));
}

PoolsManager::~PoolsManager() {
  LOG("Destructor of pool connection manager");
  destroyed_ = true;
}

PoolsManager& PoolsManager::get_instance() {
  static PoolsManager instance;
  if (destroyed_)
    throw std::runtime_error("MySQLConnection: Dead Reference Access");
  else
    return instance;
}

Connection* PoolsManager::get_src_db_connection() {
  return source_db_connections_->get_connection();
}

void PoolsManager::release_src_db_connection(Connection* connection) {
  source_db_connections_->release_connection(connection);
}

Connection* PoolsManager::get_result_db_connection() {
  return result_db_connections_->get_connection();
}

void PoolsManager::release_result_db_connection(Connection* connection) {
  result_db_connections_->release_connection(connection);
}

void PoolsManager::write_to_tbUserRegister(string str_iso_date,
                                           TableUserRegister user_register) {
  // replace to result database
  timestamp_t t0 = get_timestamp();
  Connection* conn = result_db_connections_->get_connection();
  boost::shared_ptr<sql::Statement> stmt(conn->createStatement());
  string db_result = "";
  ConfigReader::get_db_result_name(db_result);
  stmt->execute("use " + db_result);
  stmt->execute("START TRANSACTION;");

  boost::shared_ptr<sql::PreparedStatement> prep_stmt(
      conn->prepareStatement(
          "replace into tbUserRegister values(?, ?, ?, ?, ?, ?, ?)"));

  prep_stmt->setDateTime(1, str_iso_date);
  prep_stmt->setUInt(2, 0);
  prep_stmt->setUInt(3, user_register.iAllRegNum);
  prep_stmt->setUInt(4, user_register.iDayRegNum);
  prep_stmt->setUInt(5, user_register.iWeekRegNum);
  prep_stmt->setUInt(6, user_register.iDWeekRegNum);
  prep_stmt->setUInt(7, user_register.iMonthRegNum);
  prep_stmt->execute();

  stmt->execute("COMMIT;");

  LOG("### write to tbUserRegister: ", (get_timestamp() - t0) / 1000000.0L,
      "s.");

  result_db_connections_->release_connection(conn);
  AuditThread::get_instance().decrease_count_thread();
}

void PoolsManager::write_to_tbUserLoginLvDis(
    string str_iso_date, ResultTable<TableUserLoginLvDis>::Type user_login) {
  // write to db
  timestamp_t t0 = get_timestamp();
  Connection* conn = result_db_connections_->get_connection();

  boost::shared_ptr<sql::Statement> stmt(conn->createStatement());
  string db_result = "";
  ConfigReader::get_db_result_name(db_result);
  stmt->execute("use " + db_result);
  stmt->execute("START TRANSACTION;");

  boost::shared_ptr<sql::PreparedStatement> prep_stmt(
      conn->prepareStatement(
          "replace into tbUserLoginLvDis values(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"));
  boost::shared_ptr<sql::PreparedStatement> prep_stmt2(
      conn->prepareStatement(
          "replace into tbUserLogin values(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"));

  for (ResultTable<TableUserLoginLvDis>::Type::iterator sv_lv_it = user_login
      .begin(); sv_lv_it != user_login.end(); ++sv_lv_it) {
    if ((*sv_lv_it).first.lv_ != 0) {
      prep_stmt->setDateTime(1, str_iso_date);
      prep_stmt->setUInt(2, sv_lv_it->first.sv_);
      prep_stmt->setUInt(3, sv_lv_it->first.lv_);
      prep_stmt->setUInt(4, sv_lv_it->second.iDayActivityNum);
      prep_stmt->setUInt(5, sv_lv_it->second.iWeekActivityNum);
      prep_stmt->setUInt(6, sv_lv_it->second.iDWeekActivityNum);
      prep_stmt->setUInt(7, sv_lv_it->second.iMonthActivityNum);
      prep_stmt->setUInt(8, sv_lv_it->second.iDMonthActivityNum);
      prep_stmt->setUInt(9, sv_lv_it->second.iWeekLostNum);
      prep_stmt->setUInt(10, sv_lv_it->second.iDWeekLostNum);
      prep_stmt->setUInt(11, sv_lv_it->second.iMonthLostNum);
      prep_stmt->setUInt(12, sv_lv_it->second.iSilenceNum);
      prep_stmt->setUInt(13, sv_lv_it->second.iWeekBackNum);
      prep_stmt->setUInt(14, sv_lv_it->second.iDWeekBackNum);
      prep_stmt->setUInt(15, sv_lv_it->second.iMonthBackNum);
      prep_stmt->execute();
    } else {
      prep_stmt2->setDateTime(1, str_iso_date);
      prep_stmt2->setUInt(2, sv_lv_it->first.sv_);
      prep_stmt2->setUInt(3, sv_lv_it->second.iDayActivityNum);
      prep_stmt2->setUInt(4, sv_lv_it->second.iWeekActivityNum);
      prep_stmt2->setUInt(5, sv_lv_it->second.iDWeekActivityNum);
      prep_stmt2->setUInt(6, sv_lv_it->second.iMonthActivityNum);
      prep_stmt2->setUInt(7, sv_lv_it->second.iDMonthActivityNum);
      prep_stmt2->setUInt(8, sv_lv_it->second.iWeekLostNum);
      prep_stmt2->setUInt(9, sv_lv_it->second.iDWeekLostNum);
      prep_stmt2->setUInt(10, sv_lv_it->second.iMonthLostNum);
      prep_stmt2->setUInt(11, sv_lv_it->second.iSilenceNum);
      prep_stmt2->setUInt(12, sv_lv_it->second.iWeekBackNum);
      prep_stmt2->setUInt(13, sv_lv_it->second.iDWeekBackNum);
      prep_stmt2->setUInt(14, sv_lv_it->second.iMonthBackNum);
      prep_stmt2->execute();
    }
  }
  stmt->execute("COMMIT;");

  LOG("### write to tbUserLogin & tbUserLoginLvDis: ",
      (get_timestamp() - t0) / 1000000.0L, "s.");

  result_db_connections_->release_connection(conn);
  AuditThread::get_instance().decrease_count_thread();
}

void PoolsManager::write_to_tbWeekActivityScaleLvDis(
    std::string str_iso_date,
    ResultTableUseCerberus<TableActivityScaleLvDis>::Type weekScaleLvDis) {
  // write to db
  timestamp_t t0 = get_timestamp();
  Connection* conn = result_db_connections_->get_connection();

  boost::shared_ptr<sql::Statement> stmt(conn->createStatement());
  string db_result = "";
  ConfigReader::get_db_result_name(db_result);
  stmt->execute("use " + db_result);
  stmt->execute("START TRANSACTION;");

  boost::shared_ptr<sql::PreparedStatement> prep_stmt(
      conn->prepareStatement(
          "replace into tbWeekActivityScaleLvDis values(?, ?, ?, ?, ?)"));

  for (ResultTableUseCerberus<TableActivityScaleLvDis>::Type::iterator sv_lv_days_it =
      weekScaleLvDis.begin(); sv_lv_days_it != weekScaleLvDis.end();
      ++sv_lv_days_it) {
    prep_stmt->setDateTime(1, str_iso_date);
    prep_stmt->setUInt(2, sv_lv_days_it->first.sv_);
    prep_stmt->setUInt(3, sv_lv_days_it->first.lv_);
    prep_stmt->setUInt(4, sv_lv_days_it->first.days_);
    prep_stmt->setUInt(5, sv_lv_days_it->second.iActivityNum);
    prep_stmt->execute();
  }

  stmt->execute("COMMIT;");

  LOG("### write to tbWeekActivityScaleLvDis: ",
      (get_timestamp() - t0) / 1000000.0L, "s.");

  result_db_connections_->release_connection(conn);
  AuditThread::get_instance().decrease_count_thread();
}

void PoolsManager::write_to_tbDWeekActivityScaleLvDis(
    std::string str_iso_date,
    ResultTableUseCerberus<TableActivityScaleLvDis>::Type dWeekScaleLvDis) {
  // write to db
  timestamp_t t0 = get_timestamp();
  Connection* conn = result_db_connections_->get_connection();

  boost::shared_ptr<sql::Statement> stmt(conn->createStatement());
  string db_result = "";
  ConfigReader::get_db_result_name(db_result);
  stmt->execute("use " + db_result);
  stmt->execute("START TRANSACTION;");

  boost::shared_ptr<sql::PreparedStatement> prep_stmt(
      conn->prepareStatement(
          "replace into tbDWeekActivityScaleLvDis values(?, ?, ?, ?, ?)"));

  for (ResultTableUseCerberus<TableActivityScaleLvDis>::Type::iterator sv_lv_days_it =
      dWeekScaleLvDis.begin(); sv_lv_days_it != dWeekScaleLvDis.end();
      ++sv_lv_days_it) {
    prep_stmt->setDateTime(1, str_iso_date);
    prep_stmt->setUInt(2, sv_lv_days_it->first.sv_);
    prep_stmt->setUInt(3, sv_lv_days_it->first.lv_);
    prep_stmt->setUInt(4, sv_lv_days_it->first.days_);
    prep_stmt->setUInt(5, sv_lv_days_it->second.iActivityNum);
    prep_stmt->execute();
  }

  stmt->execute("COMMIT;");

  LOG("### write to tbDWeekActivityScaleLvDis: ",
      (get_timestamp() - t0) / 1000000.0L, "s.");

  result_db_connections_->release_connection(conn);
  AuditThread::get_instance().decrease_count_thread();
}

void PoolsManager::write_to_tbMonthActivityScaleLvDis(
    std::string str_iso_date,
    ResultTableUseCerberus<TableActivityScaleLvDis>::Type monthScaleLvDis) {
  timestamp_t t0 = get_timestamp();
  Connection* conn = result_db_connections_->get_connection();

  boost::shared_ptr<sql::Statement> stmt(conn->createStatement());
  string db_result = "";
  ConfigReader::get_db_result_name(db_result);
  stmt->execute("use " + db_result);
  stmt->execute("START TRANSACTION;");

  boost::shared_ptr<sql::PreparedStatement> prep_stmt(
      conn->prepareStatement(
          "replace into tbMonthActivityScaleLvDis values(?, ?, ?, ?, ?)"));

  for (ResultTableUseCerberus<TableActivityScaleLvDis>::Type::iterator sv_lv_days_it =
      monthScaleLvDis.begin(); sv_lv_days_it != monthScaleLvDis.end();
      ++sv_lv_days_it) {
    prep_stmt->setDateTime(1, str_iso_date);
    prep_stmt->setUInt(2, sv_lv_days_it->first.sv_);
    prep_stmt->setUInt(3, sv_lv_days_it->first.lv_);
    prep_stmt->setUInt(4, sv_lv_days_it->first.days_);
    prep_stmt->setUInt(5, sv_lv_days_it->second.iActivityNum);
    prep_stmt->execute();
  }

  stmt->execute("COMMIT;");

  LOG("### write to tbMonthActivityScaleLvDis: ",
      (get_timestamp() - t0) / 1000000.0L, "s.");

  result_db_connections_->release_connection(conn);
  AuditThread::get_instance().decrease_count_thread();
}

void PoolsManager::write_to_tbRoleLoginTimesDis(
    std::string str_iso_date,
    ResultTable<TableRoleLoginTimesDis>::Type role_loginTimes) {
  timestamp_t t0 = get_timestamp();
  Connection* conn = result_db_connections_->get_connection();

  boost::shared_ptr<sql::Statement> stmt(conn->createStatement());
  string db_result = "";
  ConfigReader::get_db_result_name(db_result);
  stmt->execute("use " + db_result);
  stmt->execute("START TRANSACTION;");

  boost::shared_ptr<sql::PreparedStatement> prep_stmt(
      conn->prepareStatement(
          "replace into tbRoleLoginTimesDis values(?, ?, ?, ?, ?)"));

  boost::shared_ptr<sql::PreparedStatement> prep_stmt2(
      conn->prepareStatement(
          "replace into tbRoleLoginTimes values(?, ?, ?, ?)"));

  for (ResultTable<TableRoleLoginTimesDis>::Type::iterator sv_loginTimes_it =
      role_loginTimes.begin(); sv_loginTimes_it != role_loginTimes.end();
      ++sv_loginTimes_it) {

    if (sv_loginTimes_it->first.lv_ != 0) {
      prep_stmt->setDateTime(1, str_iso_date);
      prep_stmt->setUInt(2, sv_loginTimes_it->first.sv_);
      prep_stmt->setUInt(3, 0);
      prep_stmt->setUInt(4, sv_loginTimes_it->first.lv_);
      prep_stmt->setUInt(5, sv_loginTimes_it->second.iRoleNum);
      prep_stmt->execute();
    } else {
      prep_stmt2->setDateTime(1, str_iso_date);
      prep_stmt2->setUInt(2, sv_loginTimes_it->first.sv_);
      prep_stmt2->setUInt(3, 0);
      prep_stmt2->setUInt(4, sv_loginTimes_it->second.iRoleNum);
      prep_stmt2->execute();
    }
  }

  stmt->execute("COMMIT;");

  LOG("### write to tbRoleLoginTimes & tbRoleLoginTimesDis: ",
      (get_timestamp() - t0) / 1000000.0L, "s.");

  result_db_connections_->release_connection(conn);
  AuditThread::get_instance().decrease_count_thread();
}

void PoolsManager::write_to_tbResidentUser(
    std::string str_iso_date,
    ResultTableUseCerberus<TableResidentUserLvDis>::Type resident_user) {
  timestamp_t t0 = get_timestamp();
  Connection* conn = result_db_connections_->get_connection();

  boost::shared_ptr<sql::Statement> stmt(conn->createStatement());
  string db_result = "";
  ConfigReader::get_db_result_name(db_result);
  stmt->execute("use " + db_result);
  stmt->execute("START TRANSACTION;");

  boost::shared_ptr<sql::PreparedStatement> prep_stmt(
      conn->prepareStatement(
          "replace into tbResidentUser values(?, ?, ?, ?, ?)"));

  boost::shared_ptr<sql::PreparedStatement> prep_stmt2(
      conn->prepareStatement(
          "replace into tbResidentUserLvDis values(?, ?, ?, ?, ?, ?)"));

  for (ResultTableUseCerberus<TableResidentUserLvDis>::Type::iterator it =
      resident_user.begin(); it != resident_user.end(); ++it) {
    if (it->first.days_ != 0) {
      prep_stmt2->setDateTime(1, it->second.dtRegDate);
      prep_stmt2->setDateTime(2, it->second.dtStatDate);
      prep_stmt2->setUInt(3, it->second.iDayNum);
      prep_stmt2->setUInt(4, it->first.days_);
      prep_stmt2->setUInt(5, it->second.iUserNum);
      prep_stmt2->setUInt(6, it->second.iCumulateUserNum);
      prep_stmt2->execute();
    } else {
      prep_stmt->setDateTime(1, it->second.dtRegDate);
      prep_stmt->setDateTime(2, it->second.dtStatDate);
      prep_stmt->setUInt(3, it->second.iDayNum);
      prep_stmt->setUInt(4, it->second.iUserNum);
      prep_stmt->setUInt(5, it->second.iCumulateUserNum);
      prep_stmt->execute();
    }
  }

  stmt->execute("COMMIT;");

  LOG("### write to tbResidentUser & tbResidentUserLvDis: ",
      (get_timestamp() - t0) / 1000000.0L, "s.");

  result_db_connections_->release_connection(conn);
  AuditThread::get_instance().decrease_count_thread();
}

void PoolsManager::write_to_tbEffectiveUser(
    std::string str_iso_date,
    ResultTable<TableEffecUserLvDis>::Type effective_user) {
  timestamp_t t0 = get_timestamp();
  Connection* conn = result_db_connections_->get_connection();

  boost::shared_ptr<sql::Statement> stmt(conn->createStatement());
  string db_result = "";
  ConfigReader::get_db_result_name(db_result);
  stmt->execute("use " + db_result);
  stmt->execute("START TRANSACTION;");

  boost::shared_ptr<sql::PreparedStatement> prep_stmt(
      conn->prepareStatement(
          "replace into tbEffectiveUserLvDis values(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"));
  boost::shared_ptr<sql::PreparedStatement> prep_stmt2(
      conn->prepareStatement(
          "replace into tbEffectiveUser values(?, ?, ?, ?, ?, ?, ?, ?, ?)"));

  for (ResultTable<TableEffecUserLvDis>::Type::iterator sv_lv_it =
      effective_user.begin(); sv_lv_it != effective_user.end(); ++sv_lv_it) {
    if ((*sv_lv_it).first.lv_ != 0) {
      prep_stmt->setDateTime(1, str_iso_date);
      prep_stmt->setUInt(2, sv_lv_it->first.sv_);
      prep_stmt->setUInt(3, sv_lv_it->first.lv_);
      prep_stmt->setUInt(4, sv_lv_it->second.iEffectiveNum);
      prep_stmt->setUInt(5, sv_lv_it->second.iNewEffectiveNum);
      prep_stmt->setUInt(6, sv_lv_it->second.iOldEffectiveNum);
      prep_stmt->setUInt(7, sv_lv_it->second.iBackEffectiveNum);
      prep_stmt->setUInt(8, sv_lv_it->second.iLostEffectiveNum);
      prep_stmt->setUInt(9, sv_lv_it->second.iLastMonthEffectiveNum);
      prep_stmt->setUInt(10, sv_lv_it->second.iNatureMonthRegisterNum);
      prep_stmt->execute();
    } else {
      prep_stmt2->setDateTime(1, str_iso_date);
      prep_stmt2->setUInt(2, sv_lv_it->first.sv_);
      prep_stmt2->setUInt(3, sv_lv_it->second.iEffectiveNum);
      prep_stmt2->setUInt(4, sv_lv_it->second.iNewEffectiveNum);
      prep_stmt2->setUInt(5, sv_lv_it->second.iOldEffectiveNum);
      prep_stmt2->setUInt(6, sv_lv_it->second.iBackEffectiveNum);
      prep_stmt2->setUInt(7, sv_lv_it->second.iLostEffectiveNum);
      prep_stmt2->setUInt(8, sv_lv_it->second.iLastMonthEffectiveNum);
      prep_stmt2->setUInt(9, sv_lv_it->second.iNatureMonthRegisterNum);
      prep_stmt2->execute();
    }
  }

  stmt->execute("COMMIT;");

  LOG("### write to tbEffectiveUser & tbEffectiveUserLvDis: ",
      (get_timestamp() - t0) / 1000000.0L, "s.");

  result_db_connections_->release_connection(conn);
  AuditThread::get_instance().decrease_count_thread();
}

void PoolsManager::write_to_tbDayRegRegionDis(
    std::string str_iso_date,
    ResultTable<TableDayRegRegionDis>::Type day_reg_region) {
  timestamp_t t0 = get_timestamp();
  Connection* conn = result_db_connections_->get_connection();

  boost::shared_ptr<sql::Statement> stmt(conn->createStatement());
  string db_result = "";
  ConfigReader::get_db_result_name(db_result);
  stmt->execute("use " + db_result);
  stmt->execute("START TRANSACTION;");

  boost::shared_ptr<sql::PreparedStatement> prep_stmt(
      conn->prepareStatement("replace into tbDayRegRegionDis values(?, ?, ?)"));

  for (ResultTable<TableDayRegRegionDis>::Type::iterator sv_province_it =
      day_reg_region.begin(); sv_province_it != day_reg_region.end();
      ++sv_province_it) {
    prep_stmt->setDateTime(1, str_iso_date);
    prep_stmt->setUInt(2, sv_province_it->first.lv_);
    prep_stmt->setUInt(3, sv_province_it->second.iUserNum);
    prep_stmt->execute();
  }

  stmt->execute("COMMIT;");

  LOG("### write to tbDayRegRegionDis: ", (get_timestamp() - t0) / 1000000.0L,
      "s.");

  result_db_connections_->release_connection(conn);
  AuditThread::get_instance().decrease_count_thread();
}

void PoolsManager::write_to_tbDayLoginRegionDis(
    std::string str_iso_date,
    ResultTable<TableDayLoginRegionDis>::Type day_login_region) {
  timestamp_t t0 = get_timestamp();
  Connection* conn = result_db_connections_->get_connection();

  boost::shared_ptr<sql::Statement> stmt(conn->createStatement());
  string db_result = "";
  ConfigReader::get_db_result_name(db_result);
  stmt->execute("use " + db_result);
  stmt->execute("START TRANSACTION;");

  boost::shared_ptr<sql::PreparedStatement> prep_stmt(
      conn->prepareStatement(
          "replace into tbDayLoginRegionDis values(?, ?, ?, ?)"));

  for (ResultTable<TableDayLoginRegionDis>::Type::iterator sv_province_it =
      day_login_region.begin(); sv_province_it != day_login_region.end();
      ++sv_province_it) {
    prep_stmt->setDateTime(1, str_iso_date);
    prep_stmt->setUInt(2, sv_province_it->first.sv_);
    prep_stmt->setUInt(3, sv_province_it->first.lv_);
    prep_stmt->setUInt(4, sv_province_it->second.iUserNum);
    prep_stmt->execute();
  }

  stmt->execute("COMMIT;");

  LOG("### write to tbDayLoginRegionDis: ", (get_timestamp() - t0) / 1000000.0L,
      "s.");

  result_db_connections_->release_connection(conn);
  AuditThread::get_instance().decrease_count_thread();
}

void PoolsManager::write_to_tbEffectActivity(
    std::string str_iso_date,
    ResultTable<TableInEffectActivity>::Type in_effect_activity) {
  timestamp_t t0 = get_timestamp();
  Connection* conn = result_db_connections_->get_connection();

  boost::shared_ptr<sql::Statement> stmt(conn->createStatement());
  string db_result = "";
  ConfigReader::get_db_result_name(db_result);
  stmt->execute("use " + db_result);
  stmt->execute("START TRANSACTION;");

  boost::shared_ptr<sql::PreparedStatement> prep_stmt(
      conn->prepareStatement(
          "replace into tbInEffectActivity values(?, ?, ?, ?)"));

  for (ResultTable<TableInEffectActivity>::Type::iterator sv_numday_it =
      in_effect_activity.begin(); sv_numday_it != in_effect_activity.end();
      ++sv_numday_it) {
    prep_stmt->setDateTime(1, str_iso_date);
    prep_stmt->setUInt(2, sv_numday_it->first.sv_);
    prep_stmt->setUInt(3, sv_numday_it->first.lv_);
    prep_stmt->setUInt(4, sv_numday_it->second.iActivityNum);
    prep_stmt->execute();
  }

  stmt->execute("COMMIT;");

  LOG("### write to tbInEffectActivity: ", (get_timestamp() - t0) / 1000000.0L,
      "s.");

  result_db_connections_->release_connection(conn);
  AuditThread::get_instance().decrease_count_thread();
}

void PoolsManager::write_to_tbStoreGamePoints(
    std::string str_iso_date,
    ResultTable<TableStoreGamePoints>::Type store_game_points) {
  timestamp_t t0 = get_timestamp();
  Connection* conn = result_db_connections_->get_connection();

  boost::shared_ptr<sql::Statement> stmt(conn->createStatement());
  string db_result = "";
  ConfigReader::get_db_result_name(db_result);
  stmt->execute("use " + db_result);
  stmt->execute("START TRANSACTION;");

  boost::shared_ptr<sql::PreparedStatement> prep_stmt(
      conn->prepareStatement(
          "replace into tbStoreGamePoints values(?, ?, ?, ?, ?)"));

  for (ResultTable<TableStoreGamePoints>::Type::iterator sv_none_it =
      store_game_points.begin(); sv_none_it != store_game_points.end();
      ++sv_none_it) {
    prep_stmt->setDateTime(1, str_iso_date);
    prep_stmt->setUInt(2, sv_none_it->first.sv_);
    prep_stmt->setUInt(3, sv_none_it->second.iUserNum);
    prep_stmt->setUInt(4, sv_none_it->second.iStoreTimes);
    prep_stmt->setUInt(5, sv_none_it->second.iStore);
    prep_stmt->execute();
  }

  stmt->execute("COMMIT;");

  LOG("### write to tbStoreGamePoints: ", (get_timestamp() - t0) / 1000000.0L,
      "s.");

  result_db_connections_->release_connection(conn);
  AuditThread::get_instance().decrease_count_thread();
}

void PoolsManager::write_to_tbStoreGamePointsUserDis(
    std::string str_iso_date,
    ResultTable<TableStoreGamePoints>::Type store_game_points_user_dis) {
  timestamp_t t0 = get_timestamp();
  Connection* conn = result_db_connections_->get_connection();

  boost::shared_ptr<sql::Statement> stmt(conn->createStatement());
  string db_result = "";
  ConfigReader::get_db_result_name(db_result);
  stmt->execute("use " + db_result);
  stmt->execute("START TRANSACTION;");

  boost::shared_ptr<sql::PreparedStatement> prep_stmt(
      conn->prepareStatement(
          "replace into tbStoreGamePointsUserDis values(?, ?, ?, ?, ?)"));

  for (ResultTable<TableStoreGamePoints>::Type::iterator sv_seg_it =
      store_game_points_user_dis.begin();
      sv_seg_it != store_game_points_user_dis.end(); ++sv_seg_it) {
    prep_stmt->setDateTime(1, str_iso_date);
    prep_stmt->setUInt(2, sv_seg_it->first.sv_);
    prep_stmt->setUInt(3, sv_seg_it->first.lv_);
    prep_stmt->setUInt(4, sv_seg_it->second.iUserNum);
    prep_stmt->setUInt(5, sv_seg_it->second.iStore);
    prep_stmt->execute();
  }

  stmt->execute("COMMIT;");

  LOG("### write to tbStoreGamePointsUserDis: ",
      (get_timestamp() - t0) / 1000000.0L, "s.");

  result_db_connections_->release_connection(conn);
  AuditThread::get_instance().decrease_count_thread();
}

void PoolsManager::write_to_tbDepositors(
    std::string str_iso_date,
    ResultTable<TableDepositors>::Type depositors) {
  timestamp_t t0 = get_timestamp();
  Connection* conn = result_db_connections_->get_connection();

  boost::shared_ptr<sql::Statement> stmt(conn->createStatement());
  string db_result = "";
  ConfigReader::get_db_result_name(db_result);
  stmt->execute("use " + db_result);
  stmt->execute("START TRANSACTION;");

  boost::shared_ptr<sql::PreparedStatement> prep_stmt(
      conn->prepareStatement(
          "replace into tbDepositors values(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"));

  for (ResultTable<TableDepositors>::Type::iterator sv_lv_it =
      depositors.begin(); sv_lv_it != depositors.end(); ++sv_lv_it) {
    if ((*sv_lv_it).first.lv_ == 0) {
      prep_stmt->setDateTime(1, str_iso_date);
      prep_stmt->setUInt(2, sv_lv_it->first.sv_);
      prep_stmt->setUInt(3, sv_lv_it->second.iAllDepositorNum);
      prep_stmt->setUInt(4, sv_lv_it->second.iDayDepositorNum);
      prep_stmt->setUInt(5, sv_lv_it->second.iWeekDepositorNum);
      prep_stmt->setUInt(6, sv_lv_it->second.iDWeekDepositorNum);
      prep_stmt->setUInt(7, sv_lv_it->second.iMonthDepositorNum);
      prep_stmt->setUInt(8, sv_lv_it->second.iDMonthDepositorNum);
      prep_stmt->setUInt(9, sv_lv_it->second.iWeekLostNum);
      prep_stmt->setUInt(10, sv_lv_it->second.iDWeekLostNum);
      prep_stmt->setUInt(11, sv_lv_it->second.iMonthLostNum);
      prep_stmt->setUInt(12, sv_lv_it->second.iSilenceNum);
      prep_stmt->setUInt(13, sv_lv_it->second.iWeekBackNum);
      prep_stmt->setUInt(14, sv_lv_it->second.iDWeekBackNum);
      prep_stmt->setUInt(15, sv_lv_it->second.iMonthBackNum);
      prep_stmt->execute();
    }
  }

  stmt->execute("COMMIT;");

  LOG("### write to tbDepositors: ", (get_timestamp() - t0) / 1000000.0L, "s.");

  result_db_connections_->release_connection(conn);
  AuditThread::get_instance().decrease_count_thread();
}

void PoolsManager::write_to_tbNewDepositors(
    std::string str_iso_date,
    ResultTable<TableNewDepositors>::Type new_depositors) {
  timestamp_t t0 = get_timestamp();
  Connection* conn = result_db_connections_->get_connection();

  boost::shared_ptr<sql::Statement> stmt(conn->createStatement());
  string db_result = "";
  ConfigReader::get_db_result_name(db_result);
  stmt->execute("use " + db_result);
  stmt->execute("START TRANSACTION;");

  boost::shared_ptr<sql::PreparedStatement> prep_stmt(
      conn->prepareStatement(
          "replace into tbNewDepositors values(?, ?, ?, ?, ?, ?)"));

  for (ResultTable<TableNewDepositors>::Type::iterator sv_lv_it = new_depositors
      .begin(); sv_lv_it != new_depositors.end(); ++sv_lv_it) {
    if ((*sv_lv_it).first.lv_ == 0) {
      prep_stmt->setDateTime(1, str_iso_date);
      prep_stmt->setUInt(2, sv_lv_it->first.sv_);
      prep_stmt->setUInt(3, sv_lv_it->second.iDayNewDepositorNum);
      prep_stmt->setUInt(4, sv_lv_it->second.iWeekNewDepositorNum);
      prep_stmt->setUInt(5, sv_lv_it->second.iDWeekNewDepositorNum);
      prep_stmt->setUInt(6, sv_lv_it->second.iMonthNewDepositorNum);
      prep_stmt->execute();
    }
  }

  stmt->execute("COMMIT;");

  LOG("### write to tbNewDepositors: ", (get_timestamp() - t0) / 1000000.0L,
      "s.");

  result_db_connections_->release_connection(conn);
  AuditThread::get_instance().decrease_count_thread();
}

void PoolsManager::write_to_tbAccess(
    std::string str_iso_date,
    vector< vector<string> > rows) {
  timestamp_t t0 = get_timestamp();
  Connection* conn = result_db_connections_->get_connection();

  boost::shared_ptr<sql::Statement> stmt(conn->createStatement());
  string db_result = "";
  ConfigReader::get_db_result_name(db_result);
  stmt->execute("use " + db_result);
  stmt->execute("START TRANSACTION;");

  boost::shared_ptr<sql::PreparedStatement> prep_stmt(
      conn->prepareStatement(
          "insert into tbUserAccess2 values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) on duplicate key update iServer = ?, iLevel = ?, iDayAccess = if(month(?)>month(dtFirstAccessDate), 1, if(datediff(?, dtFirstAccessDate)>=5, iDayAccess + 1, iDayAccess)), iContinuousDay = if(datediff(?, dtFirstAccessDate)=iContinuousDay, iContinuousDay + 1, 0), dtFirstAccessDate = if(month(?)>month(dtFirstAccessDate), ?, dtFirstAccessDate), iContinuousDay = if(datediff(?, dtFirstAccessDate)=iContinuousDay, iContinuousDay + 1, 0), iIsEffectiveCurMonth = if((iIsEffectiveLastMonth=true and iDayAccess>=1) or (iContinuousDay >= 5) or (iDayAccess>2), true, false), dtLastSecondAccess = if (dtLastAccess < ?, dtLastAccess, dtLastSecondAccess), dtLastAccess = if (dtLastAccess < ?, ?, dtLastAccess);"));
  
  for (unsigned i = 0; i < rows.size(); ++i) {
      prep_stmt->setDateTime(1, str_iso_date);
      prep_stmt->setString(2, rows[i][0]); // vAccount
      prep_stmt->setString(3, rows[i][1]); // iServer
      prep_stmt->setString(4, rows[i][2]); // iLevel
      prep_stmt->setUInt(5, 1); // iContinuousDay
      prep_stmt->setDateTime(6, str_iso_date); // dtFirstAccessDate
      prep_stmt->setUInt(7, 1); // iDayAccess
      prep_stmt->setBoolean(8, false);
      prep_stmt->setBoolean(9, false);
      prep_stmt->setDateTime(10, str_iso_date);
      prep_stmt->setDateTime(11, str_iso_date);
      prep_stmt->setString(12, rows[i][1]);
      prep_stmt->setString(13, rows[i][2]);
      prep_stmt->setDateTime(14, str_iso_date);
      prep_stmt->setDateTime(15, str_iso_date);
      prep_stmt->setDateTime(16, str_iso_date);
      prep_stmt->setDateTime(17, str_iso_date);
      prep_stmt->setDateTime(18, str_iso_date);
      prep_stmt->setDateTime(19, str_iso_date);
      prep_stmt->setDateTime(20, str_iso_date);
      prep_stmt->setDateTime(21, str_iso_date);
      prep_stmt->setDateTime(22, str_iso_date);
      prep_stmt->execute();
  }

  stmt->execute("COMMIT;");

  LOG("### write to tbAccess: ", (get_timestamp() - t0) / 1000000.0L,
      "s.");

  result_db_connections_->release_connection(conn);
  AuditThread::get_instance().decrease_count_thread();
}

void PoolsManager::write_to_tbRecoveredUser(
      std::string str_iso_date,
      std::vector<std::string> rows) {
  timestamp_t t0 = get_timestamp();
  Connection* conn = result_db_connections_->get_connection();

  boost::shared_ptr<sql::Statement> stmt(conn->createStatement());
  string db_result = "";
  ConfigReader::get_db_result_name(db_result);
  stmt->execute("use " + db_result);
  stmt->execute("START TRANSACTION;");

  boost::shared_ptr<sql::PreparedStatement> prep_stmt(
      conn->prepareStatement(
          "replace into tbRecoveredUser values (?, ?, ?)"));
  
  string account_name = "";
  string id_generation_type = "";
  ConfigReader::get_id_generation_type(id_generation_type);
  
  if (id_generation_type.compare("ROLE") != 0) {
    for (unsigned i = 0; i < rows.size(); ++i) {
      account_name = rows[i];
      prep_stmt->setDateTime(1, str_iso_date);
      prep_stmt->setString(2, account_name); // vAccount
      prep_stmt->setUInt(3, 0); // iServer
      prep_stmt->execute();
    }
  } else {
    vector<string> result;
    for (unsigned i = 0; i < rows.size(); ++i) {
      result.clear();
      Utils::split_by_delimiter(rows[i], ":", result);
      prep_stmt->setDateTime(1, str_iso_date);
      prep_stmt->setString(2, result[0]); // vAccount
      prep_stmt->setUInt(3, Utils::convert<string, unsigned>(result[1])); // iServer
      prep_stmt->execute();
    }
  }

  stmt->execute("COMMIT;");

  LOG("### write to tbRecoveredUser: ", (get_timestamp() - t0) / 1000000.0L,
      "s.");

  result_db_connections_->release_connection(conn);
  AuditThread::get_instance().decrease_count_thread();     
}

void PoolsManager::write_to_tbEffectiveUserDetail(
      std::string str_iso_date,
      std::vector<std::string> rows) {

  timestamp_t t0 = get_timestamp();
  Connection* conn = result_db_connections_->get_connection();

  boost::shared_ptr<sql::Statement> stmt(conn->createStatement());
  string db_result = "";
  ConfigReader::get_db_result_name(db_result);
  stmt->execute("use " + db_result);
  stmt->execute("START TRANSACTION;");

  boost::shared_ptr<sql::PreparedStatement> prep_stmt(
      conn->prepareStatement(
          "replace into tbEffectiveUserDetail values (?, ?, ?)"));
  
  string account_name = "";
  string id_generation_type = "";
  ConfigReader::get_id_generation_type(id_generation_type);
  
  if (id_generation_type.compare("ROLE") != 0) {
    for (unsigned i = 0; i < rows.size(); ++i) {
      account_name = rows[i];
      prep_stmt->setDateTime(1, str_iso_date);
      prep_stmt->setString(2, account_name); // vAccount
      prep_stmt->setUInt(3, 0); // iServer
      prep_stmt->execute();
    }
  } else {
    vector<string> result;
    for (unsigned i = 0; i < rows.size(); ++i) {
      result.clear();
      Utils::split_by_delimiter(rows[i], ":", result);
      prep_stmt->setDateTime(1, str_iso_date);
      prep_stmt->setString(2, result[0]); // vAccount
      prep_stmt->setUInt(3, Utils::convert<string, unsigned>(result[1])); // iServer
      prep_stmt->execute();
    }
  }

  stmt->execute("COMMIT;");

  LOG("### write to tbEffectiveUserDetail: ", (get_timestamp() - t0) / 1000000.0L,
      "s.");

  result_db_connections_->release_connection(conn);
  AuditThread::get_instance().decrease_count_thread();     
}


















