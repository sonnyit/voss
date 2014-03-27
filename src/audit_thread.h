/*
 * audit_thread.h
 *
 *  Created on: Sep 17, 2013
 *      Author: root
 */

#ifndef AUDIT_THREAD_H_
#define AUDIT_THREAD_H_

#include <iostream>
#include <boost/thread.hpp>

class AuditThread
{
 private:
  static bool destroyed_;
  static int count_thread;
  static int wait_time;
  int max_wait_time;
  boost::mutex mutex_;

  AuditThread();
  //! don't implement copy constructor
  AuditThread(const AuditThread&);
  //! don't implement copy constructor
  AuditThread& operator=(const AuditThread&);

  ~AuditThread();

 public:
  static AuditThread& get_instance();
  bool wait(int seconds);
  void increase_count_thread();
  void decrease_count_thread();
  int get_count_thread();
  void reset_wait_time();
};


#endif /* AUDIT_THREAD_H_ */
