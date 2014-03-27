/*
 * database.h
 *
 *  Created on: Mar 17, 2013
 *      Author: root
 */

#ifndef DATABASE_H_
#define DATABASE_H_

#include <string>
#include <iostream>
#include "result_set.h"

template<class T>
class DataBase {
 public:
  DataBase()
      : connected(false) {
  }

  virtual ~DataBase() {
    if (connected) {
      databaseEngine.close();
    }
  }

  void connect(const std::string& server, const std::string& user,
               const std::string& passwd, const std::string& db) {
    databaseEngine.connect(server, user, passwd, db);
    connected = true;
  }

  DataBase& operator<<(const std::string& sql) {
    databaseEngine.execute(sql);
    return *this;
  }

  DataBase& operator,(ResultSet& rs) {
    databaseEngine.populate(rs);
    return *this;
  }

  void close() {
    databaseEngine.close();
  }

 private:
  T databaseEngine;
  bool connected;
};

#endif /* DATABASE_H_ */
