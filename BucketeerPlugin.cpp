#include <MarkLogic.h>
#include <algorithm>
#include <chrono>
#include <ctime>
#include <map>
#include <regex.h>
#include <sstream>
#include <string>

#ifdef _MSC_VER
#define PLUGIN_DLL __declspec(dllexport)
#else // !_MSC_VER
#define PLUGIN_DLL
#endif

using namespace marklogic;

////////////////////////////////////////////////////////////////////////////////
template <class T>
class Bucketeer : public AggregateUDF
{
public:
  int key_count;
  std::multimap<String, T> buckets;

public:
  void close();

  virtual void start(Sequence &, Reporter &) = 0;
  void finish(OutputSequence &os, Reporter &reporter);

  virtual void map(TupleIterator &values, Reporter &reporter) = 0;
  void reduce(const AggregateUDF *_o, Reporter &reporter);

  void encode(Encoder &e, Reporter &reporter);
  void decode(Decoder &d, Reporter &reporter);
};

template <class T>
void Bucketeer<T>::
    close()
{
  buckets.clear();
  delete this;
}

template <class T>
void Bucketeer<T>::
    finish(OutputSequence &os, Reporter &reporter)
{
  // start the MarkLogic map
  os.startMap();
  // interate over the unique keys
  // note the use of buckets.upper_bound() to ensure we get a key only once
  for (typename std::multimap<String, T>::iterator it = buckets.begin(); it != buckets.end(); it = buckets.upper_bound(it->first))
  {
    std::pair<typename std::multimap<String, T>::iterator, typename std::multimap<String, T>::iterator> ret;
    // get the range for the key values
    ret = buckets.equal_range(it->first);
    // write the map key
    os.writeMapKey(it->first);
    // iterate over the values for current key
    for (typename std::multimap<String, T>::iterator it2 = ret.first; it2 != ret.second; ++it2)
    {
      // write the value
      os.writeValue(it2->second);
    }
  }
  os.endMap();
}

template <class T>
void Bucketeer<T>::
    reduce(const AggregateUDF *_o, Reporter &reporter)
{
  /* Merge matches found */
  const Bucketeer<T> *o = (const Bucketeer<T> *)_o;
  typename std::multimap<String, T> o_buckets = o->buckets;
  for (typename std::multimap<String, T>::iterator it = o_buckets.begin(); it != o_buckets.end(); it++)
  {
    buckets.insert(typename std::pair<String, T>(it->first, it->second));
  }
}

/*
 * Encode the map in a flattened state.
 */
template <class T>
void Bucketeer<T>::
    encode(Encoder &e, Reporter &reporter)
{
  std::map<String, int> keys_w_count;
  int key_count = 0;
  // determine key count and collect unique keys
  for (
      typename std::multimap<String, T>::iterator it = buckets.begin();
      it != buckets.end();
      // set the iterator to the upperbound of the current key
      it = buckets.upper_bound(it->first))
  {
    // store key with count of entries for that key
    keys_w_count.insert(std::pair<String, int>(it->first, buckets.count(it->first)));
    // increment key count
    key_count++;
  }
  // encode key count
  e.encode(key_count);
  for (std::map<String, int>::iterator it = keys_w_count.begin(); it != keys_w_count.end(); ++it)
  {
    // encode key value
    e.encode((it->first));
    // encode count of unique values associated with key
    e.encode(it->second);
    std::pair<typename std::multimap<String, T>::iterator, typename std::multimap<String, T>::iterator> ret;
    // get iterator for key range
    ret = buckets.equal_range(it->first);
    // encode each value for key
    for (typename std::multimap<String, T>::iterator it2 = ret.first; it2 != ret.second; ++it2)
    {
      e.encode(it2->second);
    }
  }
}

template <class T>
void Bucketeer<T>::
    decode(Decoder &d, Reporter &reporter)
{
  int key_count;
  // decode key count
  d.decode(key_count);
  for (int i = 0; i < key_count; i++)
  {
    String key;
    int key_size;
    // decode key
    d.decode(key);
    // decode count of key values
    d.decode(key_size);
    for (int j = 0; j < key_size; j++)
    {
      T value;
      // decode value for key
      d.decode(value);
      // insert key/value pair into our multimap
      buckets.insert(typename std::pair<String, T>(key, value));
    }
  }
}

////////////////////////////////////////////////////////////////////////////////

class BucketeerRegex : public Bucketeer<String>
{
public:
  std::string overflow_bucket;
  String regex;
  regex_t regex_compiled;
  bool case_insensitive;
  bool capture_overflow;

public:
  AggregateUDF *clone() const { return new BucketeerRegex(*this); }
  void start(Sequence &, Reporter &);
  void map(TupleIterator &values, Reporter &reporter);
};

void BucketeerRegex::
    start(Sequence &arg, Reporter &reporter)
{
  overflow_bucket = "bucketeer:overflow";
  case_insensitive = false;
  capture_overflow = false;
  int reti;
  int case_sensitive = 0;
  int extended = 0;
  arg.value(regex);
  arg.next();
  while (!arg.done())
  {
    String arg_value;
    arg.value(arg_value);
    const char *val = arg_value.get();
    if (strcmp(val, "case-insensitive") == 0)
    {
      case_sensitive = REG_ICASE;
      case_insensitive = true;
    }
    else if (strcmp(val, "extended") == 0)
    {
      extended = REG_EXTENDED;
    }
    else if (strcmp(val, "capture-overflow") == 0)
    {
      capture_overflow = true;
    }
    arg.next();
  }
  /* Compile regular expression */
  reti = regcomp(&regex_compiled, regex.get(), case_sensitive | extended);
}

void BucketeerRegex::
    map(TupleIterator &values, Reporter &reporter)
{
  int reti = 0;
  for (; !values.done(); values.next())
  {
    if (!values.null(0))
    {
      String cur;
      values.value(0, cur);
      /* Execute regular expression */
      regmatch_t pmatch[1];
      reti = regexec(&regex_compiled, cur.get(), 1, pmatch, 0);
      if (!reti)
      {
        // add a place for the trailing NULL
        int match_length = (pmatch[0].rm_eo - (pmatch[0].rm_so)) + 1;
        std::vector<char> match_str(match_length);
        /* If matches then create a copy of the string */
        size_t str_length = strlen(cur.get());
        std::vector<char> cp_str(str_length);
        cp_str.assign(cur.get(), cur.get() + str_length);
        // don't set the last value since it needs to be NULL
        for (int i = 0; i < (match_length - 1); i++)
        {
          int str_pos = pmatch[0].rm_so + i;
          match_str[i] = cp_str[str_pos];
        }
        // add trailing NULL to match
        match_str.insert(match_str.begin() + match_length - 1, '\0');
        String *match;
        // make everything lowercase if case_insensitive option is passed
        if (case_insensitive)
        {
          std::string tmp_str = std::string(match_str.data());
          std::transform(tmp_str.begin(), tmp_str.end(), tmp_str.begin(), ::tolower);
          match = new String(tmp_str.c_str(), cur.collation());
        }
        else
        {
          match = new String(match_str.data(), cur.collation());
        }
        /* Store the pointer to the marklogic::String for output later */
        buckets.insert(std::pair<String, String>(*(match), *(new String(cp_str.data(), cur.collation()))));
      }
      else if (capture_overflow)
      {
        size_t str_length = strlen(cur.get());
        std::vector<char> cp_str(str_length);
        cp_str.assign(cur.get(), cur.get() + str_length);
        buckets.insert(std::pair<String, String>(*(new String(overflow_bucket.c_str(), cur.collation())), *(new String(cp_str.data(), cur.collation()))));
      }
    }
  }
}

////////////////////////////////////////////////////////////////////////////////

class BucketeerDateTime : public Bucketeer<DateTime>
{
public:
  std::string collation;
  bool year;
  bool month;
  bool day_of_year;
  bool day_of_month;
  bool day_of_week;
  bool hour;
  bool minute;
  bool second;

public:
  AggregateUDF *clone() const { return new BucketeerDateTime(*this); }
  void start(Sequence &, Reporter &);
  void map(TupleIterator &values, Reporter &reporter);
};

void BucketeerDateTime::
    start(Sequence &arg, Reporter &reporter)
{
  year = false;
  month = false;
  day_of_year = false;
  day_of_month = false;
  day_of_week = false;
  hour = false;
  minute = false;
  second = false;
  collation = "http://marklogic.com/collation/codepoint";
  if (arg.done())
  {
    year = true;
    month = true;
  }
  else
  {
    while (!arg.done())
    {
      String arg_value;
      arg.value(arg_value);
      const char *val = arg_value.get();
      if (strcmp(val, "year") == 0)
      {
        year = true;
      }
      else if (strcmp(val, "month") == 0)
      {
        month = true;
      }
      else if (strcmp(val, "day-of-year") == 0)
      {
        day_of_year = true;
      }
      else if (strcmp(val, "day-of-month") == 0)
      {
        day_of_month = true;
      }
      else if (strcmp(val, "day-of-week") == 0)
      {
        day_of_week = true;
      }
      else if (strcmp(val, "hour") == 0)
      {
        hour = true;
      }
      else if (strcmp(val, "minute") == 0)
      {
        minute = true;
      }
      else if (strcmp(val, "second") == 0)
      {
        second = true;
      }
      arg.next();
    }
  }
}

void BucketeerDateTime::
    map(TupleIterator &values, Reporter &reporter)
{
  int64_t divisor = 1000000;
  for (; !values.done(); values.next())
  {
    if (!values.null(0))
    {
      marklogic::DateTime v;
      std::stringstream ss;
      std::string joiner = "";
      std::chrono::system_clock::time_point tp;
      values.value(0, v);
      // convert FILETIME to UNIX timestamp (See http://www.frenk.com/2009/12/convert-filetime-to-unix-timestamp/)
      tp += std::chrono::seconds((int64_t)(v - (11644473600000 * 10000)) / 10000000);
      std::time_t iVal = std::chrono::system_clock::to_time_t(tp);
      struct tm timeval = *localtime(&iVal);
      if (year)
      {
        ss << joiner;
        ss << (timeval.tm_year + 1900);
        joiner = "-";
      }
      if (month)
      {
        ss << joiner;
        ss << (timeval.tm_mon + 1);
        joiner = "-";
      }
      if (day_of_year)
      {
        ss << joiner;
        ss << timeval.tm_yday;
        joiner = "-";
      }
      if (day_of_month)
      {
        ss << joiner;
        ss << timeval.tm_mday;
        joiner = "-";
      }
      if (day_of_week)
      {
        ss << joiner;
        ss << timeval.tm_wday;
        joiner = "-";
      }
      if (hour)
      {
        ss << joiner;
        ss << timeval.tm_hour;
        joiner = "-";
      }
      if (minute)
      {
        ss << joiner;
        ss << timeval.tm_min;
        joiner = "-";
      }
      if (second)
      {
        ss << joiner;
        ss << timeval.tm_sec;
      }
      buckets.insert(std::pair<String, DateTime>(*(new String(ss.str().c_str(), collation.c_str())), v));
    }
  }
}

////////////////////////////////////////////////////////////////////////////////

extern "C" PLUGIN_DLL void
marklogicPlugin(Registry &r)
{
  r.version();
  r.registerAggregate<BucketeerRegex>("regex");
  r.registerAggregate<BucketeerDateTime>("dateTime");
}
