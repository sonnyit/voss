/*
 * audit_thread.cpp
 *
 *  Created on: Sep 17, 2013
 *      Author: root
 */

#ifndef LOGGING_LEVEL_1
#define LOGGING_LEVEL_1
#endif

#include "audit_thread.h"
#include "logger.h"

using namespace std;

bool AuditThread::destroyed_ = false;
int AuditThread::count_thread = 0;
int AuditThread::wait_time = 0;

AuditThread::AuditThread()
{
  // wait maximum 120s
  max_wait_time = 120;
}

AuditThread::~AuditThread()
{
  destroyed_ = true;
}

AuditThread& AuditThread::get_instance()
{
  static AuditThread instance;
  if (destroyed_)
    throw std::runtime_error( "AuditThread: Dead Reference Access");
  else
    return instance;
}

bool AuditThread::wait(int seconds)
{
  this->wait_time += seconds;

  if (this->wait_time < this->max_wait_time)
  {
    LOG_WARN("AUDIT THREAD: wait ", seconds, " seconds");
    boost::this_thread::sleep(boost::posix_time::seconds(seconds));
    return true;
  }
  else
  {
    LOG_ERR("AUDIT THREAD: wait over maximum time. Cannot wait more !!!");
    return false;
  }
}

void AuditThread::increase_count_thread()
{
  this->mutex_.lock();
  ++this->count_thread;
  this->mutex_.unlock();
}

void AuditThread::decrease_count_thread()
{
  this->mutex_.lock();
  --this->count_thread;
  this->mutex_.unlock();
}

int AuditThread::get_count_thread()
{
  return this->count_thread;
}

void AuditThread::reset_wait_time()
{
  this->mutex_.lock();
  this->wait_time = 0;
  this->mutex_.unlock();
}
