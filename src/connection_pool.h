/*
 * connection_pool.h
 *
 *  Created on: May 1, 2013
 *      Author: minhnh3
 */

#ifndef CONNECTION_POOL_H_
#define CONNECTION_POOL_H_

#include <iostream>
#include <mysql_connection.h>
#include <mysql_driver.h>

#include <cppconn/driver.h>
#include <cppconn/exception.h>
#include <cppconn/resultset.h>
#include <cppconn/statement.h>
#include <cppconn/prepared_statement.h>

#include <boost/thread.hpp>
#include <list>

using namespace sql;

#define MySQLPool ConnectionPool::GetInstance()

/*!\class ConnectionPool
 * \brief hold a number of connection to a database (mysql server)
 */
class ConnectionPool {
 private:
  std::string url_;
  std::string user_name_;
  std::string password_;
  unsigned max_size_;

  std::list<Connection*> connection_list_;
  boost::mutex mutex_;
  Driver* driver_;

 private:
  Connection* create_connection();
  //!\brief Initialize a pool with the number of connection (size).
  void init_connection(unsigned init_size);
  //!\brief Terminate a connection and push it back to it's pool.
  void terminate_connection(Connection* connection);
  //!\brief Terminate a pool and release all connection it contain.
  void terminate_connection_pool();

 public:
  ConnectionPool(std::string url, std::string user_name, std::string password,
                 unsigned max_size);
  ~ConnectionPool();

  //!\brief Pop a connection still available from a pool for using.
  Connection* get_connection();
  //!\brief After using connection, push it back to it's pool.
  void release_connection(Connection* connection);
  //!\brief Get current available connection still in pool.
  unsigned get_current_size();
};

#endif /* CONNECTION_POOL_H_ */
