/*
 * def.h
 *
 *  Created on: Mar 19, 2013
 *      Author: minhnh3
 */

#ifndef KEY_NAME_H_
#define KEY_NAME_H_

#include <boost/unordered/unordered_map.hpp>

#include <sys/time.h>
#include <bitset>
#include <string>

struct LoginBitmap {
  std::bitset<35> bitmap;

  LoginBitmap(const unsigned& nth) {
    bitmap.set(nth);
  }

  LoginBitmap() {
    bitmap.reset();
  }

  void operator=(const LoginBitmap& another) {
    bitmap = another.bitmap;
  }

  void add_dayth(const unsigned& nth) {
    bitmap.set(34 - (nth - 1)); // start at 0
  }
};

/*!\file
 * \brief hold constant or define value
 *
 * \warning messy, need recode again :)
 */

template<typename T>
struct Map1level {
  typedef std::map<T, T> type;
};

template<typename T, typename U>
struct Mmap2level {
  typedef std::multimap<T, U> sub_type;
  typedef std::multimap<T, sub_type> type;
};

template<typename T, typename U>
struct UMap2level {
  typedef boost::unordered_map<T, U> sub_type;
  typedef boost::unordered_map<T, sub_type> type;
};

//!\brief get current time (milliseconds)
typedef unsigned long long timestamp_t;
timestamp_t get_timestamp();

template<typename T, typename U>
struct Map2level {
  typedef std::map<T, U> sub_type;
  typedef std::map<T, sub_type> type;
};

struct Keys {
  unsigned sv_;
  unsigned lv_;
  Keys()
      : sv_(0),
        lv_(0) {
  }
  ;
  Keys(const unsigned& sv, const unsigned& lv)
      : sv_(sv),
        lv_(lv) {
  }
  ;
  bool operator<(const Keys& k) const {
    return (sv_ < k.sv_ || (sv_ == k.sv_ && lv_ < k.lv_));
  }
};

struct Cerberus {
  unsigned sv_;
  unsigned lv_;
  unsigned days_;
  Cerberus()
      : sv_(0),
        lv_(0),
        days_(0) {
  }
  ;
  Cerberus(const unsigned& sv, const unsigned& lv, const unsigned& days)
      : sv_(sv),
        lv_(lv),
        days_(days) {
  }
  ;
  bool operator<(const Cerberus& k) const {
    return (sv_ < k.sv_ || (sv_ == k.sv_ && lv_ < k.lv_)
        || (sv_ == k.sv_ && lv_ == k.lv_ && days_ < k.days_));
  }
};

template<typename T>
struct ResultTable {
  typedef std::map<Keys, T> Type;
};

template<typename T>
struct ResultTableUseCerberus {
  typedef std::map<Cerberus, T> Type;
};

namespace TIME_CONST {
const unsigned DAY = 0;
const unsigned WEEK = 6;
const unsigned BIWEEK = 13;
const unsigned THWEEK = 20;
const unsigned FOURWEEK = 27;
const unsigned SIXWEEK = 41;
const unsigned MONTH = 29;
const unsigned BIMONTH = 59;
const unsigned THMONTH = 89;
}

namespace DATA_TYPE {
const std::string BITMAP = "bm";
const std::string HASH = "hash";
}

namespace CONSTANT_KEY {
const std::string UUID = "UUID";
const std::string IP2LOC = "IP2LOC";
const std::string RHASH = "rhash:";
}

namespace INDEX {
const std::string REGISTERED = "registered";
const std::string USER = "user";
const std::string DEPOSIT = "deposit";
const std::string RECOVERED_USER = "recovered_user";
}

namespace BM {
const std::string LOGIN = "bm:login:";
const std::string REG = "bm_reg:";

const std::string DEPOSIT = "bm:deposit:";

const std::string EFFECTIVE_USERS = "bm_effective_userswoeihoxzas";
const std::string EFFECTIVE_NEW_USERS = "bm_effective_new_userswrq3ws";
const std::string EFFECTIVE_RETURN_USERS = "bm_effective_return_userssadvhhj";
const std::string EFFECTIVE_STAY_USERS = "bm_effective_stay_usersweresa";
const std::string EFFECTIVE_LOST_USERS = "bm_effective_lost_usersoewjieo";
const std::string EFFECTIVE_LAST_MONTH = "bm_effective_last_monthwoer32";
const std::string EFFECTIVE_MONTH = "bm_effective_month";
}

namespace ZSET {
const std::string ZS_TOTAL_LOG = "zs_log:";
const std::string ZS_STORE_ZS_LOG_KEYS = "zs_store_zs_log_k:";

const std::string STORE_HASHES_LOGIN_KEYNAMES = "zset_store_h_login_kn:";
const std::string STORE_BM_LOGIN_KEYNAMES = "zset_store_bm_login_kn";

const std::string STORE_BM_DEPOSIT_KEYNAMES = "zset_store_bm_deposit_kn";

const std::string STORE_BITMAP = "zset_store_bm_";
}

namespace SET {
const std::string STORE_SERVERS_LOGIN_KN = "s_store_servers_login_kn";
}

namespace HASHES {
const std::string LOGIN = "h_login:";
const std::string STORE_SERVERS_LOGIN = "h_store_servers_login:";
}

struct TableUserRegister {
  unsigned iWorldId;
  unsigned iAllRegNum;
  unsigned iDayRegNum;
  unsigned iWeekRegNum;
  unsigned iDWeekRegNum;
  unsigned iMonthRegNum;
};

struct TableDayRegRegionDis {
  unsigned iUserNum;

  TableDayRegRegionDis() {
    iUserNum = 0;
  }

  void increase() {
    ++iUserNum;
  }
};

struct TableDayLoginRegionDis {
  unsigned iUserNum;

  TableDayLoginRegionDis() {
    iUserNum = 0;
  }

  void increase() {
    ++iUserNum;
  }
};

struct TableUserLogin {
  std::string iDayActivityNum;
  std::string iWeekActivityNum;
  std::string iDWeekActivityNum;
  std::string iMonthActivityNum;
  std::string iDMonthActivityNum;
  std::string iWeekLostNum;
  std::string iDWeekLostNum;
  std::string iMonthLostNum;
  std::string iSilenceNum;
  std::string iWeekBackNum;
  std::string iDWeekBackNum;
  std::string iMonthBackNum;
};

enum FieldsOfUserLoginLvDis {
  eDayActivityNum,
  eWeekActivityNum,
  eDWeekActivityNum,
  eMonthActivityNum,
  eDMonthActivityNum,
  eWeekLostNum,
  eDWeekLostNum,
  eMonthLostNum,
  eSilenceNum,
  eWeekBackNum,
  eDWeekBackNum,
  eMonthBackNum,

  eActivityNum,
};

struct TableUserLoginLvDis {
  unsigned iWorldId;
  unsigned iLevel;
  unsigned iDayActivityNum;			// first: iWorldId, second: number
  unsigned iWeekActivityNum;
  unsigned iDWeekActivityNum;
  unsigned iMonthActivityNum;
  unsigned iDMonthActivityNum;
  unsigned iWeekLostNum;
  unsigned iDWeekLostNum;
  unsigned iMonthLostNum;
  unsigned iSilenceNum;
  unsigned iWeekBackNum;
  unsigned iDWeekBackNum;
  unsigned iMonthBackNum;

  TableUserLoginLvDis() {
    iWorldId = iLevel = iDayActivityNum = iWeekActivityNum = iDWeekActivityNum =
        iMonthActivityNum = iDMonthActivityNum = iWeekLostNum = iDWeekLostNum =
            iMonthLostNum = iSilenceNum = iWeekBackNum = iDWeekBackNum =
                iMonthBackNum = 0;
  }

  void increase(FieldsOfUserLoginLvDis field) {
    switch (field) {
      case eDayActivityNum: {
        ++iDayActivityNum;
        break;
      }
      case eWeekActivityNum: {
        ++iWeekActivityNum;
        break;
      }
      case eDWeekActivityNum: {
        ++iDWeekActivityNum;
        break;
      }
      case eMonthActivityNum: {
        ++iMonthActivityNum;
        break;
      }
      case eDMonthActivityNum: {
        ++iDMonthActivityNum;
        break;
      }
      case eWeekLostNum: {
        ++iWeekLostNum;
        break;
      }
      case eDWeekLostNum: {
        ++iDWeekLostNum;
        break;
      }
      case eMonthLostNum: {
        ++iMonthLostNum;
        break;
      }
      case eSilenceNum: {
        ++iSilenceNum;
        break;
      }
      case eWeekBackNum: {
        ++iWeekBackNum;
        break;
      }
      case eDWeekBackNum: {
        ++iDWeekBackNum;
        break;
      }
      case eMonthBackNum: {
        ++iMonthBackNum;
        break;
      }
      default:
        break;
    }
  }
};

struct TableActivityScaleLvDis {
  unsigned iActivityNum;

  TableActivityScaleLvDis() {
    iActivityNum = 0;
  }

  void increase() {
    ++iActivityNum;
  }
};

struct TableRoleLoginTimesDis {
  unsigned iRoleNum;

  TableRoleLoginTimesDis() {
    iRoleNum = 0;
  }

  void increase() {
    ++iRoleNum;
  }

  void increase(unsigned v) {
    iRoleNum += v;
  }
};

enum FieldsResidentUser {
  eResidentUserUserNum,
  eResidentUserCumulateUserNum,
};

struct TableResidentUser {
  std::string dtRegDate;
  std::string dtStatDate;
  unsigned iDayNum;
  unsigned iUserNum;
  unsigned iCumulateUserNum;

  TableResidentUser() {
    dtRegDate = dtStatDate = "";
    iDayNum = iUserNum = iCumulateUserNum = 0;
  }
  TableResidentUser(std::string regDate, std::string statDate, unsigned dayNum,
                    unsigned userNum, unsigned cumulateUserNum) {
    dtRegDate = regDate;
    dtStatDate = statDate;
    iDayNum = dayNum;
    iUserNum = userNum;
    iCumulateUserNum = cumulateUserNum;
  }

  void increase(FieldsResidentUser field) {
    switch (field) {
      case eResidentUserUserNum: {
        ++iUserNum;
        break;
      }
      default:
        break;
    }
  }
};

struct TableResidentUserLvDis {
  std::string dtRegDate;
  std::string dtStatDate;
  unsigned iDayNum;
  unsigned iLevel;
  unsigned iUserNum;
  unsigned iCumulateUserNum;

  TableResidentUserLvDis() {
    dtRegDate = dtStatDate = "";
    iDayNum = iLevel = iUserNum = iCumulateUserNum = 0;
  }

  TableResidentUserLvDis(std::string regDate, std::string statDate,
                         unsigned dayNum, unsigned level, unsigned userNum,
                         unsigned cumulateUserNum) {
    dtRegDate = regDate;
    dtStatDate = statDate;
    iDayNum = dayNum;
    iLevel = level;
    iUserNum = userNum;
    iCumulateUserNum = cumulateUserNum;
  }

  void increase() {
    ++iUserNum;
  }
};

struct TableEffectiveUser {
  std::string dtStatDate;
  unsigned iWorldId;
  unsigned iEffectiveNum;
  unsigned iNewEffectiveNum;
  unsigned iOldEffectiveNum;
  unsigned iBackEffectiveNum;
  unsigned iLostEffectiveNum;
  unsigned iLastMonthEffectiveNum;
  unsigned iNatureMonthRegisterNum;

  TableEffectiveUser() {
    dtStatDate = "";
    iWorldId = iEffectiveNum = iNewEffectiveNum = iOldEffectiveNum =
        iBackEffectiveNum = iLostEffectiveNum = iLastMonthEffectiveNum =
            iNatureMonthRegisterNum = 0;
  }
};

enum FieldsOfEffectiveUserLvDis {
  eEffectiveNum,
  eNewEffectiveNum,
  eOldEffectiveNum,
  eBackEffectiveNum,
  eLostEffectiveNum,
  eLastMonthEffectiveNum,
  eNatureMonthRegisterNum,
};

struct TableEffecUserLvDis {
  std::string dtStatDate;
  unsigned iWorldId;
  unsigned iLevel;
  unsigned iEffectiveNum;
  unsigned iNewEffectiveNum;
  unsigned iOldEffectiveNum;
  unsigned iBackEffectiveNum;
  unsigned iLostEffectiveNum;
  unsigned iLastMonthEffectiveNum;
  unsigned iNatureMonthRegisterNum;

  TableEffecUserLvDis() {
    dtStatDate = "";
    iWorldId = iEffectiveNum = iNewEffectiveNum = iOldEffectiveNum = iLevel =
        iBackEffectiveNum = iLostEffectiveNum = iLastMonthEffectiveNum =
            iNatureMonthRegisterNum = 0;
  }

  void increase(FieldsOfEffectiveUserLvDis field) {
    switch (field) {
      case eEffectiveNum: {
        ++iEffectiveNum;
        break;
      }
      case eNewEffectiveNum: {
        ++iNewEffectiveNum;
        break;
      }
      case eOldEffectiveNum: {
        ++iOldEffectiveNum;
        break;
      }
      case eBackEffectiveNum: {
        ++iBackEffectiveNum;
        break;
      }
      case eLostEffectiveNum: {
        ++iLostEffectiveNum;
        break;
      }
      case eLastMonthEffectiveNum: {
        ++iLastMonthEffectiveNum;
        break;
      }
      case eNatureMonthRegisterNum: {
        ++iNatureMonthRegisterNum;
        break;
      }
    }
  }
};

struct TableInEffectActivity {
  unsigned iActivityNum;

  TableInEffectActivity()
      : iActivityNum(0) {
  }

  void increase() {
    ++iActivityNum;
  }
};

enum FieldsOfDepositors {
  eAllDepositorNum,
  eDayDepositorNum,
  eWeekDepositorNum,
  eDWeekDepositorNum,
  eMonthDepositorNum,
  eDMonthDepositorNum,
  eWeekDepositorLostNum,
  eDWeekDepositorLostNum,
  eMonthDepositorLostNum,
  eSilenceDepositorNum,
  eWeekDepositorBackNum,
  eDWeekDepositorBackNum,
  eMonthDepositorBackNum,
};

struct TableDepositors {
  unsigned iAllDepositorNum;
  unsigned iDayDepositorNum;
  unsigned iWeekDepositorNum;
  unsigned iDWeekDepositorNum;
  unsigned iMonthDepositorNum;
  unsigned iDMonthDepositorNum;
  unsigned iWeekLostNum;
  unsigned iDWeekLostNum;
  unsigned iMonthLostNum;
  unsigned iSilenceNum;
  unsigned iWeekBackNum;
  unsigned iDWeekBackNum;
  unsigned iMonthBackNum;

  TableDepositors() {
    iAllDepositorNum = iDayDepositorNum = iWeekDepositorNum =
        iDWeekDepositorNum = iMonthDepositorNum = iDMonthDepositorNum =
            iWeekLostNum = iDWeekLostNum = iMonthLostNum = iSilenceNum =
                iWeekBackNum = iDWeekBackNum = iMonthBackNum = 0;
  }

  void increase(FieldsOfDepositors field) {
    switch (field) {
      case eAllDepositorNum: {
        ++iAllDepositorNum;
        break;
      }
      case eDayDepositorNum: {
        ++iDayDepositorNum;
        break;
      }
      case eWeekDepositorNum: {
        ++iWeekDepositorNum;
        break;
      }
      case eDWeekDepositorNum: {
        ++iDWeekDepositorNum;
        break;
      }
      case eMonthDepositorNum: {
        ++iMonthDepositorNum;
        break;
      }
      case eDMonthDepositorNum: {
        ++iDMonthDepositorNum;
        break;
      }
      case eWeekDepositorLostNum: {
        ++iWeekLostNum;
        break;
      }
      case eDWeekDepositorLostNum: {
        ++iDWeekLostNum;
        break;
      }
      case eMonthDepositorLostNum: {
        ++iMonthLostNum;
        break;
      }
      case eSilenceDepositorNum: {
        ++iSilenceNum;
        break;
      }
      case eWeekDepositorBackNum: {
        ++iWeekBackNum;
        break;
      }
      case eDWeekDepositorBackNum: {
        ++iDWeekBackNum;
        break;
      }
      case eMonthDepositorBackNum: {
        ++iMonthBackNum;
        break;
      }
    }
  }
};

enum FieldsOfNewDepositors {
  eDayNewDepositorNum,
  eWeekNewDepositorNum,
  eDWeekNewDepositorNum,
  eMonthNewDepositorNum,
};

struct TableNewDepositors {
  unsigned iDayNewDepositorNum;
  unsigned iWeekNewDepositorNum;
  unsigned iDWeekNewDepositorNum;
  unsigned iMonthNewDepositorNum;

  TableNewDepositors() {
    iDayNewDepositorNum = iWeekNewDepositorNum = iDWeekNewDepositorNum =
        iMonthNewDepositorNum = 0;
  }

  void increase(FieldsOfNewDepositors field) {
    switch (field) {
      case eDayNewDepositorNum: {
        ++iDayNewDepositorNum;
        break;
      }
      case eWeekNewDepositorNum: {
        ++iWeekNewDepositorNum;
        break;
      }
      case eDWeekNewDepositorNum: {
        ++iDWeekNewDepositorNum;
        break;
      }
      case eMonthNewDepositorNum: {
        ++iMonthNewDepositorNum;
        break;
      }
    }
  }
};

struct TableStoreGamePoints {
  unsigned iUserNum;
  unsigned iStoreTimes;
  unsigned iStore;

  TableStoreGamePoints() {
    iUserNum = iStoreTimes = iStore = 0;
  }

  void increaseUserNum() {
    ++iUserNum;
  }

  void addStoreTimes(unsigned amount) {
    iStoreTimes += amount;
  }

  void addGamePoints(unsigned amount) {
    iStore += amount;
  }
};

#endif /* KEY_NAME_H_ */
