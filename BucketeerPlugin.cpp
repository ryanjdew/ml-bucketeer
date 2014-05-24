#include <MarkLogic.h>
#include <regex.h> 
#include <string.h>
#include <ctime>
#include <map>
#include <vector>
#include <sstream>
#include <chrono>

#ifdef _MSC_VER
#define PLUGIN_DLL __declspec(dllexport)
#else // !_MSC_VER
#define PLUGIN_DLL
#endif

using namespace marklogic;

////////////////////////////////////////////////////////////////////////////////
template<class T>
class Bucketeer : public AggregateUDF
{
public:
  int key_count;
  std::multimap<String, T> buckets;
public:
  void close();

  virtual void start(Sequence&, Reporter&) = 0;
  void finish(OutputSequence& os, Reporter& reporter);

  virtual void map(TupleIterator& values, Reporter& reporter) = 0;
  void reduce(const AggregateUDF* _o, Reporter& reporter);

  void encode(Encoder& e, Reporter& reporter);
  void decode(Decoder& d, Reporter& reporter);

};

template<class T>
void Bucketeer<T>::
close() { 
  buckets.clear();
  delete this;
}

template<class T>
void Bucketeer<T>::
finish(OutputSequence& os, Reporter& reporter)
{
  std::vector<String> keys;
  for (typename std::multimap<String, T>::iterator it = buckets.begin(); it != buckets.end(); it = buckets.upper_bound(it->first)) {
    keys.push_back(it->first);
  }
  os.startMap();
  for (std::vector<String>::iterator it = keys.begin(); it != keys.end(); ++it) {
    std::pair <typename std::multimap<String,T>::iterator, typename std::multimap<String,T>::iterator> ret;
    ret = buckets.equal_range(*it);
    os.writeMapKey((*it));
    for (typename std::multimap<String, T>::iterator it2 = ret.first; it2 != ret.second; ++it2) {
      os.writeValue(it2->second);
    }
  }
  os.endMap();
}

template<class T>
void Bucketeer<T>::
reduce(const AggregateUDF* _o, Reporter& reporter)
{
  /* Merge matches found */
  const Bucketeer<T> *o = (const Bucketeer<T>*)_o;
  typename std::multimap<String, T> o_buckets = o->buckets;
  for (typename std::multimap<String, T>::iterator it = o_buckets.begin(); it != o_buckets.end(); it++) {
    buckets.insert(typename std::pair <String, T> (it->first, it->second));
  }
}

/* 
 * Encode the map in a flattened state.  
 */
template<class T>
void Bucketeer<T>::
encode(Encoder& e, Reporter& reporter)
{
  std::map<String, int> keys_w_count;
  int key_count = 0;
  // determine key count and collect unique keys
  for (
    typename std::multimap<String, T>::iterator it = buckets.begin(); 
    it != buckets.end(); 
    // set the iterator to the upperbound of the current key
    it = buckets.upper_bound(it->first)
  ) {
    // store key with count of entries for that key
    keys_w_count.insert(std::pair <String, int>(it->first,buckets.count(it->first)));
    // increment key count
    key_count++;
  }
  // encode key count
  e.encode(key_count);
  for (std::map<String, int>::iterator it = keys_w_count.begin(); it != keys_w_count.end(); ++it) {
    // encode key value
    e.encode((it->first));
    // encode count of unique values associated with key
    e.encode(it->second);
    std::pair <typename std::multimap<String,T>::iterator, typename std::multimap<String,T>::iterator> ret;
    // get iterator for key range
    ret = buckets.equal_range(it->first);
    // encode each value for key
    for (typename std::multimap<String, T>::iterator it2 = ret.first; it2 != ret.second; ++it2) {
      e.encode(it2->second);
    }
  }
}

template<class T>
void Bucketeer<T>::
decode(Decoder& d, Reporter& reporter)
{
  int key_count;
  // decode key count
  d.decode(key_count);
  for (int i = 0; i < key_count;i++) {
    String key;
    int key_size;
    // decode key
    d.decode(key);
    // decode count of key values
    d.decode(key_size);
    for (int j = 0; j < key_size; j++) {
      T value;
      // decode value for key
      d.decode(value);
      // insert key/value pair into our multimap
      buckets.insert(typename std::pair <String, T> (key, value));
    }
  }
}

////////////////////////////////////////////////////////////////////////////////

class BucketeerRegex : public Bucketeer<String>
{
public:
  String regex;
  regex_t regex_compiled;
public:
  AggregateUDF* clone() const { return new BucketeerRegex(*this); }
  void start(Sequence&, Reporter&);
  void map(TupleIterator& values, Reporter& reporter);
};

void BucketeerRegex::
start(Sequence& arg, Reporter& reporter)
{
  int reti;
  int case_sensitive = 0;
  int extended = 0;
  arg.value(regex);
  arg.next();
  while(!arg.done()) {
    String arg_value;
    arg.value(arg_value);
    const char* val = arg_value.get();
    if (strcmp(val,"case-insensitive") == 0) {
      case_sensitive = REG_ICASE;
    } else if (strcmp(val,"extended") == 0) {
      extended = REG_EXTENDED;
    }
    arg.next();
  }
  /* Compile regular expression */
  reti = regcomp(&regex_compiled, regex.get(), case_sensitive|extended);
}

void BucketeerRegex::
map(TupleIterator& values, Reporter& reporter)
{
  int reti = 0;
  for(; !values.done(); values.next()) {
    if(!values.null(0)) {
      String cur; 
      values.value(0,cur);
	    /* Execute regular expression */
      regmatch_t pmatch[1];
	    reti = regexec(&regex_compiled, cur.get(), 1, pmatch, 0);
	    if( !reti ){
        // add a place for the trailing NULL
        int match_length = (pmatch[0].rm_eo - (pmatch[0].rm_so)) + 1;
        char match_str[match_length];
		    /* If matches then create a copy of the string */
		    size_t str_length = strlen(cur.get());
		    char cp_str[str_length];
		    strcpy(cp_str,cur.get());
        // don't set the last value since it needs to be NULL
        for (int i = 0; i < (match_length - 1); i++) {
          int str_pos = pmatch[0].rm_so + i;
          match_str[i] = cp_str[str_pos];
        }
        // add trailing NULL to match
        match_str[match_length - 1] = '\0';
        String* match = new String(match_str,cur.collation());
		    /* Store the pointer to the marklogic::String for output later */
        buckets.insert(std::pair<String, String>(*(match),*(new String(cp_str,cur.collation()))));
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
  AggregateUDF* clone() const { return new BucketeerDateTime(*this); }
  void start(Sequence&, Reporter&);
  void map(TupleIterator& values, Reporter& reporter);
};

void BucketeerDateTime::
start(Sequence& arg, Reporter& reporter)
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
  if (arg.done()) {
    year = true;
    month = true;
  } else {
    while(!arg.done()) {
      String arg_value;
      arg.value(arg_value);
      const char* val = arg_value.get();
      if (strcmp(val,"year") == 0) {
        year = true;
      } else if (strcmp(val,"month") == 0) {
        month = true;
      } else if (strcmp(val,"day-of-year") == 0) {
        day_of_year = true;
      } else if (strcmp(val,"day-of-month") == 0) {
        day_of_month = true;
      } else if (strcmp(val,"day-of-week") == 0) {
        day_of_week = true;
      } else if (strcmp(val,"hour") == 0) {
        hour = true;
      } else if (strcmp(val,"minute") == 0) {
        minute = true;
      } else if (strcmp(val,"second") == 0) {
        second = true;
      }
      arg.next();
    }
  }
}

void BucketeerDateTime::
map(TupleIterator& values, Reporter& reporter)
{
  int64_t divisor = 1000000;
  for(; !values.done(); values.next()) {
    if(!values.null(0)) {
      marklogic::DateTime v;
      std::time_t iVal;
      struct tm timeval;
      std::stringstream ss;
      std::string joiner = "";
      std::chrono::system_clock::time_point tp;
      values.value(0, v);
      // convert FILETIME to UNIX timestamp (See http://www.frenk.com/2009/12/convert-filetime-to-unix-timestamp/)
      tp += std::chrono::seconds((int64_t)(v - (11644473600000 * 10000))/10000000);
      iVal = std::chrono::system_clock::to_time_t(tp);
      localtime_r(&iVal, &timeval);
      if (year) {
        ss << joiner;
        ss << (timeval.tm_year + 1900);
        joiner = "-";
      } 
      if (month) {
        ss << joiner;
        ss << (timeval.tm_mon + 1);
        joiner = "-";
      } 
      if (day_of_year) {
        ss << joiner;
        ss << timeval.tm_yday;
        joiner = "-";
      }
      if (day_of_month) {
        ss << joiner;
        ss << timeval.tm_mday;
        joiner = "-";
      }
      if (day_of_week) {
        ss << joiner;
        ss << timeval.tm_wday;
        joiner = "-";
      }
      if (hour) {
        ss << joiner;
        ss << timeval.tm_hour;
        joiner = "-";
      }
      if (minute) {
        ss << joiner;
        ss << timeval.tm_min;
        joiner = "-";
      }
      if (second) {
        ss << joiner;
        ss << timeval.tm_sec;
      }
      buckets.insert(std::pair<String, DateTime>(*(new String(ss.str().c_str(),collation.c_str())),v));
    }
  }
}

////////////////////////////////////////////////////////////////////////////////

extern "C" PLUGIN_DLL void
marklogicPlugin(Registry& r)
{
  r.version();
  r.registerAggregate<BucketeerRegex>("regex");
  r.registerAggregate<BucketeerDateTime>("dateTime");
}
