/*
 * MySql.h
 *
 *  Created on: Mar 17, 2013
 *      Author: root
 */

#ifndef MYSQL_H_
#define MYSQL_H_

#include "exception.h"
#include "result_set.h"
#include "database.h"
#include "iostream"
#include <mysql/mysql.h>

class MySql {
  friend class DataBase<MySql> ;
 public:
  MySql() {
    pConnectionHandler = NULL;
  }

  ~MySql() {
  }
  ;

  void connect(const std::string& server, const std::string& user,
               const std::string& passwd, const std::string& db) {
    pConnectionHandler = mysql_init(NULL);

    if (NULL
        == mysql_real_connect(pConnectionHandler, server.c_str(), user.c_str(),
                              passwd.c_str(), db.c_str(), 0, NULL, 0)) {
      throw DataBaseError(
          "Failed to connect to database: Error: "
              + std::string(mysql_error(pConnectionHandler)));
    }
  }

  void execute(const std::string& sql) {
    //std::cout << sql << std::endl;

    if (!mysql_query(pConnectionHandler, sql.c_str()) == 0) {
      throw DataBaseError(
          "Failed to execute sql: Error: "
              + std::string(mysql_error(pConnectionHandler)));
    }
  }

  void populate(ResultSet& rs) {
    MYSQL_RES* result = NULL;

    result = mysql_use_result(pConnectionHandler);

    MYSQL_ROW row;
    unsigned int num_fields;
    unsigned int i;
    num_fields = mysql_num_fields(result);

    // get rows
    while ((row = mysql_fetch_row(result))) {
      std::vector<std::string> myRow;

      // get fields of row
      for (i = 0; i < num_fields; ++i) {
        if (row[i] == NULL) {
          myRow.push_back("NULL");
        } else {
          myRow.push_back(row[i]);
        }
      }
      rs.addRow(myRow);
    }

    mysql_free_result(result);
  }

 protected:
  void close() {
    std::cout << "Close connection" << std::endl;
    if (pConnectionHandler != NULL)
      mysql_close(pConnectionHandler);
  }

 private:
  MYSQL* pConnectionHandler;
};

#endif /* MYSQL_H_ */
