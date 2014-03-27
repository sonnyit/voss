/*
 * connection_pool.cpp
 *
 *  Created on: May 1, 2013
 *      Author: minhnh3
 */

#ifndef LOGGING_LEVEL_1
#define LOGGING_LEVEL_1
#endif

#include "connection_pool.h"
#include "logger.h"
#include <exception>

using namespace std;

ConnectionPool::ConnectionPool(std::string url, std::string user_name,
                               std::string password, unsigned max_size)
    : url_(url),
      user_name_(user_name),
      password_(password),
      max_size_(max_size) {
  try {
    driver_ = get_driver_instance();
  } catch (sql::SQLException &e) {
    LOG_ERR("Can not get_driver_instance() - exit");
    exit(1);
  }
  this->init_connection(max_size_ / 2);
  LOG("\t+ create connection pool to ", url_);
}

ConnectionPool::~ConnectionPool() {
  this->terminate_connection_pool();
  LOG("\t+ delete connection pool to ", url_);
}

Connection* ConnectionPool::create_connection() {
  Connection* connection;
  try {
    connection = driver_->connect(this->url_, this->user_name_,
                                  this->password_);
    return connection;
  } catch (sql::SQLException &e) {
    LOG_ERR("Can not create_connection() - return NULL");
    return NULL;
  }
}

void ConnectionPool::init_connection(unsigned init_size) {
  Connection* connection;
  mutex_.lock();
  for (unsigned i = 0; i < init_size; ++i) {
    connection = this->create_connection();
    if (connection) {
      connection_list_.push_back(connection);
    } else {
      LOG_ERR("init connection fail");
    }
  }
  mutex_.unlock();
}

Connection* ConnectionPool::get_connection() {
  Connection* connection;
  mutex_.lock();

  if (connection_list_.size() > 0) {
    connection = connection_list_.front();
    connection_list_.pop_front();

    if (connection->isClosed()) {
      delete connection;
      connection = this->create_connection();
    }
    mutex_.unlock();
    return connection;
  } else {
    if (connection_list_.size() < max_size_) {
      connection = this->create_connection();

      if (connection) {
        mutex_.unlock();
        return connection;
      } else {
        LOG("over half pool, but create addition connection fail");
        mutex_.unlock();
        return NULL;
      }
    } else {
      LOG("reach max connection of pools, can not create more");
      mutex_.unlock();
      return NULL;
    }
  }
  return NULL;
}

void ConnectionPool::release_connection(Connection* connection) {
  if (connection) {
    mutex_.lock();
    connection_list_.push_back(connection);
    mutex_.unlock();
  }
}

void ConnectionPool::terminate_connection(Connection* connection) {
  if (connection) {
    try {
      connection->close();
    } catch (sql::SQLException &e) {
      LOG_ERR("Close connection have error");
    }
  }
  delete connection;
}

void ConnectionPool::terminate_connection_pool() {
  list<Connection*>::iterator connection_it;
  mutex_.lock();
  for (connection_it = connection_list_.begin();
      connection_it != connection_list_.end(); ++connection_it) {
    this->terminate_connection(*connection_it);
  }
  connection_list_.clear();
  mutex_.unlock();
}

unsigned ConnectionPool::get_current_size() {
  return connection_list_.size();
}

