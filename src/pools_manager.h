/*
 * pools_manager.h
 *
 *  Created on: May 1, 2013
 *      Author: minhnh3
 */

#ifndef MYSQL_CONNECTION_H_
#define MYSQL_CONNECTION_H_

#include "def.h"
#include "connection_pool.h"
#include <boost/shared_ptr.hpp>
#include <vector>

/*!\class PoolsManager
 * \brief Manage connection for two pools: one for source db, and one for dest db.
 */
class PoolsManager {
 private:
  static bool destroyed_;
  PoolsManager();
  //! don't implement copy constructor.
  PoolsManager(const PoolsManager&);
  //! don't implement assignment constructor.
  PoolsManager& operator=(const PoolsManager&);
  ~PoolsManager();

  boost::shared_ptr<ConnectionPool> source_db_connections_;
  boost::shared_ptr<ConnectionPool> result_db_connections_;

  std::string url_src_db_, url_result_db_;
  std::string user_name_src_db_, user_name_result_db_;
  std::string password_src_db_, password_result_db_;
  std::string db_name_src_db_, db_name_result_db_;
  unsigned max_size_pool_src_db_, max_size_pool_result_db_;

 public:
  static PoolsManager& get_instance();
  //!\brief Get a connection from a pool, which contain connections to source database.
  Connection* get_src_db_connection();
  //!\brief Get a connection from a pool, which contain connections to result database.
  Connection* get_result_db_connection();
  //!\brief release connection back to to pool connection with source database after using.
  void release_src_db_connection(Connection* connection);
  //!\brief release connection back to to pool connection with result database after using.
  void release_result_db_connection(Connection* connection);

  void write_to_tbUserRegister(std::string str_iso_date,
                               TableUserRegister user_register);

  void write_to_tbUserLoginLvDis(
      std::string str_iso_date,
      ResultTable<TableUserLoginLvDis>::Type user_login);
  void write_to_tbWeekActivityScaleLvDis(
      std::string str_iso_date,
      ResultTableUseCerberus<TableActivityScaleLvDis>::Type weekScaleLvDis);
  void write_to_tbDWeekActivityScaleLvDis(
      std::string str_iso_date,
      ResultTableUseCerberus<TableActivityScaleLvDis>::Type dWeekScaleLvDis);
  void write_to_tbMonthActivityScaleLvDis(
      std::string str_iso_date,
      ResultTableUseCerberus<TableActivityScaleLvDis>::Type monthScaleLvDis);

  void write_to_tbRoleLoginTimesDis(
      std::string str_iso_date,
      ResultTable<TableRoleLoginTimesDis>::Type role_loginTimes);

  void write_to_tbResidentUser(
      std::string str_iso_date,
      ResultTableUseCerberus<TableResidentUserLvDis>::Type resident_user);

  void write_to_tbEffectiveUser(
      std::string str_iso_date,
      ResultTable<TableEffecUserLvDis>::Type effective_user);

  void write_to_tbDayRegRegionDis(
      std::string str_iso_date,
      ResultTable<TableDayRegRegionDis>::Type day_reg_region);

  void write_to_tbDayLoginRegionDis(
      std::string str_iso_date,
      ResultTable<TableDayLoginRegionDis>::Type day_login_region);

  void write_to_tbEffectActivity(
      std::string str_iso_date,
      ResultTable<TableInEffectActivity>::Type in_effect_activity);

  // deposit index
  void write_to_tbStoreGamePoints(
      std::string str_iso_date,
      ResultTable<TableStoreGamePoints>::Type store_game_points);
  void write_to_tbStoreGamePointsUserDis(
      std::string str_iso_date,
      ResultTable<TableStoreGamePoints>::Type store_game_points_user_dis);

  void write_to_tbDepositors(
      std::string str_iso_date,
      ResultTable<TableDepositors>::Type depositors);

  void write_to_tbNewDepositors(
      std::string str_iso_date,
      ResultTable<TableNewDepositors>::Type new_depositors);
      
  void write_to_tbAccess(
      std::string str_iso_date,
      std::vector< std::vector<std::string> > rows);
      
  void write_to_tbRecoveredUser(
      std::string str_iso_date,
      std::vector<std::string> rows);

  void write_to_tbEffectiveUserDetail(
      std::string str_iso_date,
      std::vector<std::string> rows);
};

#endif /* MYSQL_CONNECTION_H_ */
