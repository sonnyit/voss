/*
 * logger.h
 *
 *  Created on: May 24, 2013
 *      Author: minhnh3
 */

#ifndef LOGGER_H_
#define LOGGER_H_

#include "log.h"

static logging::logger<logging::file_log_policy> log_inst("log/oss_user_deposit.log");

#ifdef LOGGING_LEVEL_1

#define LOG log_inst.print<logging::severity_type::debug>
#define LOG_ERR log_inst.print<logging::severity_type::error>
#define LOG_WARN log_inst.print<logging::severity_type::warning>

#else

#define LOG(...)
#define LOG_ERR(...)
#define LOG_WARN(...)

#endif

#endif /* LOGGER_H_ */
