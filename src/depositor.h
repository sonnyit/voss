/*
 * deposit.h
 *
 *  Created on: May 13, 2013
 *      Author: minhnh3
 */

#ifndef DEPOSITOR_H_
#define DEPOSITOR_H_

#include "base.h"
#include "def.h"
#include "utils.h"

#include <boost/date_time/gregorian/gregorian.hpp>
#include <string>

/*!\class Depositor
 * \brief manage deposit index
 *
 * derived from Base
 */
class Depositor : public Base {
 private:
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

  static KeyPointDate key_point_date;

 public:
  Depositor();
  ~Depositor();

  //!\brief derived from base class, get data for index deposit
  void get_data(const std::string& str_iso_date, const std::string& index =
                    INDEX::DEPOSIT);

  void config_before_statistic(const std::string& str_iso_date);
  void statistic(const std::string& str_iso_date);

  //=============================== depositor =================================

  //!\brief find numbers of depositor in an observation.
  void find_depositors(const std::string& dest_key,
                       const std::string& str_iso_date_statistic,
                       const unsigned& observation,
                       ResultTable<TableDepositors>::Type& result,
                       FieldsOfDepositors field);

  //!\brief find numbers of lost depositor in an observation.
  void find_depositors_lost(const std::string& dest_key,
                            const std::string& iso_date_statistic,
                            const unsigned& observation,
                            ResultTable<TableDepositors>::Type& result,
                            FieldsOfDepositors field);

  //!\brief find numbers of back depositor in an observation.
  void find_depositors_back(const std::string& dest_key,
                            const std::string& str_iso_date_statistic,
                            const unsigned& observation,
                            ResultTable<TableDepositors>::Type& result,
                            FieldsOfDepositors field);

  //!\brief fill table tbDepositors
  void fill_tbDepositors(const std::string& str_iso_date);

  //!\brief fill table tbNewDepositors
  void fill_tbNewDepositors(const std::string& str_iso_date);

  //================================ new depositor ======================================

  //!\brief find numbers of new depositor in an observation.
  void find_new_depositors(const std::string& dest_key,
                           const std::string& str_iso_date_statistic,
                           const unsigned& observation,
                           ResultTable<TableNewDepositors>::Type& result,
                           FieldsOfNewDepositors field);

  //================================ store game point ====================================
  void fill_tbStoreGamePoints(const std::string& str_iso_date);

  //================================ statistic level distributed =========================

  //!\brief help detail numbers of depositors by server, level
  void depositors_by_level_distributed(
      std::vector<std::string>& info,
      ResultTable<TableDepositors>::Type& result_table,
      FieldsOfDepositors field);

  //!\brief help detail numbers of new depositors by server, level
  void new_depositors_by_level_distributed(
      std::vector<std::string>& info,
      ResultTable<TableNewDepositors>::Type& result_table,
      FieldsOfNewDepositors field);

  //!\brief help detail numbers of effective depositors by server, level
  void statistic_effective_by_level_distributed(
      std::vector<std::string>& info,
      ResultTable<TableEffecUserLvDis>::Type& result_table,
      FieldsOfEffectiveUserLvDis field);
};

#endif /* DEPOSITOR_H_ */
