/*
 * user.h
 *
 *  Created on: May 9, 2013
 *      Author: minhnh3
 */

#ifndef USER_H_
#define USER_H_

#include <string>
#include "base.h"
#include "def.h"
#include "utils.h"

#include <boost/date_time/gregorian/gregorian.hpp>

/*!\class User
 * \brief manage user index
 *
 * derived from Base
 */
class User : public Base {
 private:
  struct ConfigEffectiveUser {
    unsigned continuous_;
    unsigned at_least_;
    unsigned from_day_n_;

    ConfigEffectiveUser()
        : continuous_(4),
          at_least_(2),
          from_day_n_(5) {
    }
  };

  struct KeyPointDate {
    boost::gregorian::date statis_day;
    boost::gregorian::date day_begin_month0;
    boost::gregorian::date day_begin_month1;
    boost::gregorian::date day_begin_month2;
    boost::gregorian::date day_begin_month3;
    boost::gregorian::date day_end_month0;
    boost::gregorian::date day_end_month1;
    boost::gregorian::date day_end_month2;
    boost::gregorian::date day_end_month3;
  };

  static ConfigEffectiveUser config_effective_user;
  static KeyPointDate key_point_date;
  static std::string game_open_date;

 public:
  User();
  ~User();

  void get_data(const std::string& str_iso_date, const std::string& index =
                    INDEX::USER);
  void import_registered(const std::string& str_iso_date,
                         const std::string& index = INDEX::USER);
  void config_before_statistic(const std::string& str_iso_date);
  void statistic(const std::string& str_iso_date);

  //================================ statistic level distributed =====================
  void fill_tbDayRegRegionDis(const std::string& bm_reg_in_date,
                              const std::string& str_iso_date);

  void fill_tbDayLoginRegionDis(const std::string& str_iso_date);
  //================================ statistic level distributed =====================
  void statistic_active_by_level_distributed(
      std::vector<std::string>& info,
      ResultTable<TableUserLoginLvDis>::Type& result_table,
      FieldsOfUserLoginLvDis field);
  void statistic_effective_by_level_distributed(
      std::vector<std::string>& info,
      ResultTable<TableEffecUserLvDis>::Type& result_table,
      FieldsOfEffectiveUserLvDis field);

  void statistic_frequency_active(
      std::vector<std::string>& info,
      ResultTableUseCerberus<TableActivityScaleLvDis>::Type& result_table);

  //================================= tbUserRegister ===================================
  void fill_tbUserRegister(const std::string& str_iso_date);

  //=============================== tbUserLogin, tbUserLoginLvDis =======================
  void find_active_users(
      const std::string& dest_key,
      const std::string& str_iso_date_begin,
      const std::string& str_iso_date_end,
      FieldsOfUserLoginLvDis field,
      ResultTable<TableUserLoginLvDis>::Type& tbUserLoginLvDis,
      ResultTableUseCerberus<TableActivityScaleLvDis>::Type& tbActivityScaleLvDis);
  void find_lost_active_users(const std::string& circle1,
                              const std::string& circle2,
                              const std::string& dest_key,
                              const std::string& str_iso_date_begin,
                              const std::string& str_iso_date_end,
                              FieldsOfUserLoginLvDis field,
                              ResultTable<TableUserLoginLvDis>::Type& result);
  void find_back_active_users(const std::string& circle1,
                              const std::string& circle2,
                              const std::string& circle3,
                              const std::string& dest_key,
                              const std::string& str_iso_start_circle3,
                              const std::string& str_iso_end_circle3,
                              const std::string& std_iso_stat_date,
                              FieldsOfUserLoginLvDis field,
                              ResultTable<TableUserLoginLvDis>::Type& result);
  void fill_tbUserLoginLvDis(const std::string& str_iso_date);

  void fill_RoleLoginTimesDis(const std::string& str_iso_date);
  //================================ tbResidentUser ====================================

  //!\brief find date last login of new register
  void find_last_date_new_reg_login(
      const std::string& bm_reg_in_statis_date,
      const std::string& str_iso_reg_date, const std::string& str_iso_stat_date,
      ResultTableUseCerberus<TableResidentUserLvDis>::Type& result_table);
  void fill_tbResidentUser(const std::string& str_iso_date);

  //======================== tbEffectiveUser & tbEffectiveUserLvDis =====================

  //!\brief find numbers of registers in natural month.
  void find_num_reg_of_natural_month(
      const std::string& str_iso_stat_date,
      ResultTable<TableEffecUserLvDis>::Type& uomResult);

  /*! registered in this natural month and:
   * login 4 days continuous after registered day
   * from the 6th day of the registered day to statis day have at least 2 days login
   */

  /*! registered before last natural month, not login in last natural month
   * and in this month:
   * login 5 days continuous or
   * from the 6th day of the first login to statis day have at least 2 days login
   */

  /*! effective user from last natural month and in this natural month login
   * at least 1 days.
   */

  void fill_tbEffectiveUser(const std::string& isoDateStat);

  void fill_tbEffectActivity(const std::string& isoDateStat);

  void fill_tbRecoveredUser(const std::string& str_iso_date);

  void export_EffectiveUser(const std::string& str_iso_date);

  void find_user_have_state_effective(const std::string& str_iso_date, const std::string& dest_key, const bool& is_stat_month = false);

  void find_new_and_return_effective_users(
      const std::string& new_effective_user_key,
      const std::string& return_effective_user_key,
      const std::string& str_iso_stat_date,
      const bool& is_stat_month = false);

  void find_stay_and_lost_and_last_month_effective_users(
      const std::string& stay_effective_user_key,
      const std::string& lost_effective_user_key,
      const std::string& last_month_effective_user_key,
      const std::string& str_iso_stat_date);

  void get_level_info_of_effective_user(
      const std::string& key_name, 
      const std::string& str_iso_date_start, 
      const std::string& str_iso_date_end, 
      std::vector<std::string>& info);

  //======================= utils =============================

  //!\brief merge server
  void merge_server(const std::string& src, const std::string& dest,
                    const std::string& max_src_id);

  //!\brief clear date
  void clear(const std::string& str_iso_stat_date);
  
  //!\brief for export user info of recovered_user and effective user
  void user_export(const std::string& str_iso_date);
};

#endif /* USER_H_ */
