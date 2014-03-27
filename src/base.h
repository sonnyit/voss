/*
 * base.h
 *
 *  Created on: May 9, 2013
 *      Author: root
 */

#ifndef BASE_H_
#define BASE_H_

#include <string>

/*!\class Base
 * \brief The base, where all class for each index inheritance from it.
 *
 * \detail virtual class.
 */
class Base {
 public:
  Base(void) {
  }
  ;
  virtual ~Base(void) {
  }
  ;

  /*!\brief Get data from source db
   *
   * \param str_iso_date is date we need get data, have type as undelimited iso string date.
   * \param index which index we want to get data, as: USER, DEPOSITOR.
   * \index_redis_database redis database corresponding.
   */
  virtual void get_data(const std::string& str_iso_date,
                        const std::string& index, bool force = false);

  //!\brief pure virtual function, derived class will implement it.
  virtual void statistic(const std::string& str_iso_date) = 0;
};

#endif /* BASE_H_ */
