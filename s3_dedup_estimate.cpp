#include <aws/core/Aws.h>
#include <aws/s3/S3Client.h>
#include <aws/s3/S3ServiceClientModel.h>
#include <aws/core/auth/AWSCredentialsProvider.h>
#include <aws/core/auth/AWSCredentialsProviderChain.h>
#include <aws/core/utils/HashingUtils.h>
#include <aws/s3/model/GetObjectRequest.h>
#include <aws/s3/model/GetObjectResult.h>
#include <aws/s3/model/GetBucketVersioningRequest.h>
#include <aws/s3/model/ObjectVersion.h>
#include <aws/s3/model/ListObjectsRequest.h>
#include <aws/s3/model/ListObjectsResult.h>
#include <aws/s3/model/ListObjectVersionsRequest.h>
#include <aws/s3/model/ListObjectVersionsResult.h>

#include <algorithm>
#include <iostream>
#include <fstream>
#include <string>
#include <string_view>
#include <cstring>
#include <cstdlib>
#include <cassert>
#include <vector>
#include <unordered_map>
#include <mutex>
#include <thread>


using namespace Aws;
using namespace Aws::Auth;
using namespace Aws::Utils;
using namespace Aws::Client;
using namespace Aws::S3;
using namespace Aws::S3::Model;

using namespace std;

//#define EXPLICIT_CREDENTIALS
//Aws::Auth::AWSCredentials credentials("0555b35654ad1656d804", "h7GhxuBLTrlhVUyxSPUKUV8r/2EI4ngqJxD7iBdBYLhwluN30JaT3Q==");
//Aws::Auth::AWSCredentials credentials("KF7KMXHXU9R2DHG1CKSL", "9gA16gTPyM2ZsBsGzDVkNsaiVH5egYBqFc9PAPLB");

//---------------------------------------------------------------------------
static inline uint64_t div_up(uint64_t n, uint32_t d) {
  return ((n + d - 1) /d);
}

struct stat_counters_t {
  // counter for the total existing objects
  uint64_t objs_cnt = 0;
  // counter for the unique copies of objects
  uint64_t uniq_cnt = 0;
  // counter for deleted copies of existing objects
  uint64_t dels_cnt = 0;
  // counter for old versions of existing objects
  uint64_t vers_cnt = 0;

  // counter for buckets who failed to list
  uint32_t bad_bucket_cnt = 0;
};

struct params_t {
  const char* skip_buckets    = nullptr;
  const char* allowed_buckets = nullptr;
  //const char* endpoint   = "http://127.0.0.1:5000"; // RGW Balancer port
  const char* endpoint   = "http://127.0.0.1:8000"; // RGW default port
  const char* access_key = nullptr;
  const char* secret_key = nullptr;
  int threads_count = 4;
};

std::ostream &operator<<(std::ostream &stream, const params_t & p)
{
  if (p.skip_buckets) {
    stream << "skip_buckets_file=" << p.skip_buckets << std::endl;
  }
  if (p.allowed_buckets) {
    stream << "allowed_buckets_file=" << p.allowed_buckets << std::endl;
  }
  stream << "endpoint=" << p.endpoint << std::endl;
  stream << "access_key=" << ((p.access_key == nullptr) ? "default" : p.access_key) << std::endl;
  stream << "secret_key=" << ((p.secret_key == nullptr) ? "default" : p.secret_key) << std::endl;
  stream << "threads_count=" << p.threads_count << std::endl;
  return stream;
}

// 24 Bytes Keys
struct Key
{
  friend std::ostream &operator<<(std::ostream &stream, const Key & k);
  uint64_t md5_high;   // High Bytes of the Object Data MD5
  uint64_t md5_low;    // Low  Bytes of the Object Data MD5
  uint32_t obj_size;   // Object size in 4KB units (AWS MAX-SIZE is 5GB)
  uint16_t num_parts;  // How many parts were used in multipart upload (AWS MAX-PART is 10,000)
  uint16_t pad16;      // Pad to get 8 Bytes alignment
} __attribute__((__packed__));	// 24Bytes are 8Bytes aligned so should probably be packed already

std::ostream &operator<<(std::ostream &stream, const Key & k)
{
  stream << std::hex << "0x" << k.md5_high << k.md5_low << "::" << std::dec << 4*k.obj_size << "KiB::" << k.num_parts;
  if (k.pad16) {
    stream << "PAD=" << k.pad16 << "\n";
  }
  return stream;
}

struct KeyHash
{
  std::size_t operator()(const struct Key& k) const
  {
    // The MD5 is already a hashing function so no need for another hash
    return k.md5_low;
  }
};

struct KeyEqual
{
  bool operator()(const struct Key& lhs, const struct Key& rhs) const
  {
    return (
      lhs.md5_high  == rhs.md5_high &&
      lhs.md5_low   == rhs.md5_low  &&
      lhs.obj_size  == rhs.obj_size &&
      lhs.num_parts == rhs.num_parts);
  }
};

using MD5_Dict = std::unordered_map<struct Key, uint32_t, KeyHash, KeyEqual>;

std::mutex dict_mtx;
std::mutex print_mtx;

//==========================================================================
struct arr_entry {
  friend std::ostream &operator<<(std::ostream& stream, const arr_entry& e);

  // empty CTOR
  arr_entry() {
    this->count    = 0;
    this->tot_size = 0;
    this->min_size = 0xFFFFFFFF;
    this->max_size = 0;
  }

  // emplace CTOR
  arr_entry(uint32_t size) {
    this->count    = 1;
    this->tot_size = size;
    this->min_size = size;
    this->max_size = size;
  }

  void add_entry(uint32_t size) {
    this->count ++;
    this->tot_size += size;
    if (size < this->min_size) {
      this->min_size = size;
    }
    if (size > this->max_size) {
      this->max_size = size;
    }
  }

  uint32_t get_count() const {
    return this->count;
  }

private:
  uint32_t count;
  uint64_t tot_size;
  uint32_t min_size;
  uint32_t max_size;
};

std::ostream &operator<<(std::ostream& stream, const arr_entry& e)
{
  if (e.get_count()) {
    stream << "Min: " << e.min_size << " KiB, Max: " << e.max_size
	   << " KiB, Avg: " << e.tot_size/e.get_count() << " KiB";
  }
  else {
    stream << "Null object" << std::endl;
  }
  return stream;
}

//==========================================================================
class hashstreambuf : public std::streambuf {
  unsigned      m_chunk_size;
  unsigned      m_pos = 0;
  uint64_t      m_fp_cnt = 0;
  uint64_t      m_bytes  = 0;
  char          m_buff[64*1024];
  std::fstream *m_filep;
public:

  hashstreambuf(std::fstream *hashfile, unsigned chunk_size) : m_chunk_size(chunk_size), m_filep(hashfile) { }

  void process_single_chunk(const char *p) {
    std::string sv(p, m_chunk_size);
    ByteBuffer  bb = HashingUtils::CalculateSHA1(sv);
    m_filep->write((const char*)bb.GetUnderlyingData(), bb.GetLength());
    m_fp_cnt++;
    m_bytes += m_chunk_size;
  }

  std::streamsize xsputn (const char *p, const std::streamsize size) override {
    //return size;
    unsigned    n = size;
    if (m_pos > 0) {
      // we got leftover data
      assert(m_pos < m_chunk_size);
      unsigned missing_bytes = (m_chunk_size - m_pos);
      if (n > missing_bytes) {
	std::memcpy(m_buff+m_pos, p, missing_bytes);
	process_single_chunk(m_buff);
	m_pos = 0;
	// skip past read data
	p += missing_bytes;
	n -= missing_bytes;
      }
    }

    while (n >= m_chunk_size) {
      process_single_chunk(p);
      // skip past read data
      p += m_chunk_size;
      n -= m_chunk_size;
    }

    if (n > 0) {
      // copy leftover data to buffer
      std::memcpy(m_buff, p, n);
      m_pos = n;
    }

    return size;
  }
};

//==========================================================================
//---------------------------------------------------------------------------
static void print_report(const MD5_Dict &etags_dict)
{
  // on disk allocation is done in 4KB units
  uint64_t duplicated_data_units = 0;
  uint64_t unique_data_units     = 0;

  uint64_t multipart_obj_count   = 0;
  uint64_t single_part_obj_count = 0;

  constexpr unsigned ARR_SIZE = (64*1024);
  std::array<arr_entry, ARR_SIZE+1> summery;

  for (auto const& entry : etags_dict) {
    const Key & key = entry.first;
    const unsigned count = entry.second;
    if (key.num_parts == 1) {
      single_part_obj_count += count;
    }
    else if (key.num_parts > 1) {
      multipart_obj_count +=count;
    }
    else {
      std::cerr << "Bad Key with zero parts! " << key << std::endl;
    }

    unique_data_units     += key.obj_size;
    duplicated_data_units += (count-1)*(key.obj_size);

    // on disk allocation is done in 4KB units
    if (count < ARR_SIZE) {
      summery[count].add_entry(key.obj_size * 4);
    }
    else {
      summery[ARR_SIZE].add_entry(key.obj_size * 4);
    }
  }

  if (unique_data_units == 0) {
    std::cout << "We had a total of 0 KiB stored in the system" << std::endl;
    return;
  }

  std::cout << "We had " << multipart_obj_count  << " multipart objects out of "
	    << (multipart_obj_count + single_part_obj_count) << std::endl;

  // on disk allocation is done in 4KB units
  uint64_t duplicated_data_kb = duplicated_data_units * 4;
  uint64_t unique_data_kb = unique_data_units * 4;
  uint64_t total_size_kb = duplicated_data_kb + unique_data_kb;
  std::cout << "We had a total of " << total_size_kb << " KiB stored in the system\n";
  std::cout << "We had " << unique_data_kb << " Unique Data KiB stored in the system\n";
  std::cout << "We had " << duplicated_data_kb << " Duplicated KiB Bytes stored in the system\n";
  std::cout << "Dedup Ratio = " << (double)total_size_kb/(double)unique_data_kb << std::endl;
  std::cout << "===========================================================================\n" << std::endl;
  for (unsigned idx = 0; idx < ARR_SIZE; idx ++) {
    if (summery[idx].get_count() > 0){
#if 1
      std::cout << "We had " << summery[idx].get_count() << " keys with " << idx
		<< " repetitions (" << summery[idx] << ")" << std::endl;
#else
      // build output in a checkable format
      std::cout << summery[idx].get_count() << " " << idx << std::endl;
#endif
    }
  }
  if (summery[ARR_SIZE].get_count() > 0) {
    std::cout << "We had " << summery[ARR_SIZE].get_count() << " keys with more than " << ARR_SIZE
	      << " repetitions (" << summery[ARR_SIZE] << ")" << std::endl;
  }
}

// convert a hex-string to a 64bit integer (max 16 hex digits)
//---------------------------------------------------------------------------
uint64_t hex2int(char *p, char* p_end)
{
  if (p_end - p <= sizeof(uint64_t) * 2) {
    uint64_t val = 0;
    while (p < p_end) {
      // get current character then increment
      uint8_t byte = *p++;
      // transform hex character to the 4bit equivalent number, using the ascii table indexes
      if (byte >= '0' && byte <= '9') {
	byte = byte - '0';
      }
      else if (byte >= 'a' && byte <='f') {
	byte = byte - 'a' + 10;
      }
      else if (byte >= 'A' && byte <='F') {
	byte = byte - 'A' + 10;
      }
      else {
	// terminate on the first non hex char
	return val;
      }
      // shift 4 to make space for new digit, and add the 4 bits of the new digit
      val = (val << 4) | (byte & 0xF);
    }
    return val;
  }
  else {
    std::cerr << __func__ << "Value size too big: " << (p_end - p) << std::endl;
    return 0;
  }
}

//---------------------------------------------------------------------------
uint16_t dec2int(char *p, char* p_end)
{
  constexpr unsigned max_uint16_digits = 5; // 65536
  if (p_end - p <= max_uint16_digits) {
    uint16_t val = 0;
    while (p < p_end) {
      uint8_t byte = *p++;
      if (byte >= '0' && byte <= '9') {
	val = val * 10 + (byte - '0');
      }
      else {
	// terminate on the first non hex char
	return val;
      }
    }
    return val;
  }
  else {
    std::cerr << __func__ << "Value size too big: " << (p_end - p) << std::endl;
    return 0;
  }
}

//---------------------------------------------------------------------------
static uint16_t get_num_parts(const std::string & etag)
{
  // 16Bytes MD5 takes 32 chars + 2 chars for the "" signs
  if (etag.length() <= 34) {
    return 1;
  }
  // Amazon S3 multipart upload Maximum number = 10,000 (5 decimal digits)
  // We need 1 extra byte for the '-' delimiter and 1 extra byte for '"' at the end
  // 7 Bytes should suffice, but we roundup to 8 Bytes
  constexpr unsigned max_part_len = 8;
  std::string::size_type n = etag.find('-', etag.length() - max_part_len);
  if (n != std::string::npos) {
    // again, 1 extra byte for the '-' delimiter and 1 extra byte for '"' at the end
    unsigned copy_size = etag.length() - (n + 1 + 1);
    char buff[copy_size+1];
    unsigned nbytes = etag.copy(buff, copy_size, n+1);
    uint64_t num_parts = dec2int(buff, buff+nbytes);
    return num_parts;
  }
  else {
    std::cerr << "Bad MD5=" << etag << std::endl;
    return 1;
  }
}

//---------------------------------------------------------------------------
bool list_objects_single_bucket(Aws::S3::S3Client & s3_client,
				const std::string & bucket_name,
				MD5_Dict *p_etags_dict,
				stat_counters_t *p_stats)
{
  uint64_t objs_cnt = 0;
  uint64_t uniq_cnt = 0;
  Aws::S3::Model::ListObjectsRequest request;
  request.WithBucket(bucket_name);
  unsigned page_num = 0;
  bool     has_more = true;
  while (has_more) {
    auto outcome = s3_client.ListObjects(request);
    if (!outcome.IsSuccess()) {
      std::cerr << bucket_name << " retry: " << __func__ << ": "
		<< outcome.GetError().GetMessage() << std::endl;
      // retry one time
      outcome = s3_client.ListObjects(request);
      if (!outcome.IsSuccess()) {
	std::cerr << bucket_name << " Error: " << __func__ << ": "
		  << outcome.GetError().GetMessage() << std::endl;
	return false;
      }
    }

    auto & listing = outcome.GetResult();
    has_more = listing.GetIsTruncated();
    const Aws::Vector<Aws::S3::Model::Object>& objects = listing.GetContents();
    auto nextMarker = listing.GetNextMarker();
    request.SetMarker(nextMarker);

    char buff[64];
    for (const Aws::S3::Model::Object &object: objects) {
      objs_cnt ++;
      const auto   & etag      = object.GetETag();
      // on disk allocation is done in 4KB units
      // round up to find the on-disk space used by the object
      const uint32_t size      = div_up(object.GetSize(), 4*1024);
      const uint16_t num_parts = get_num_parts(etag);

      const unsigned nbytes    = etag.copy(buff, 32, 1);
      const uint64_t high      = hex2int(buff, buff+16);
      const uint64_t low       = hex2int(buff+16, buff+32);
      Key key = {high, low, size, num_parts, 0};
      std::unique_lock<std::mutex> lock(dict_mtx);
      auto itr = p_etags_dict->find(key);
      if (itr == p_etags_dict->end()) {
	(*p_etags_dict)[key] = 1;
	uniq_cnt ++;
      }
      else {
	itr->second ++;
      }
    }

    page_num++;
  }

  p_stats->objs_cnt += objs_cnt;
  p_stats->uniq_cnt += uniq_cnt;
  return true;
}

//---------------------------------------------------------------------------
bool list_objects_versions_single_bucket(Aws::S3::S3Client & s3_client,
					 const std::string & bucket_name,
					 MD5_Dict *p_etags_dict,
					 stat_counters_t *p_stats)
{
  uint64_t dels_cnt = 0;
  uint64_t vers_cnt = 0;
  uint64_t objs_cnt = 0;
  uint64_t uniq_cnt = 0;
  Aws::S3::Model::ListObjectVersionsRequest request;
  request.WithBucket(bucket_name);

  string   prev_key;
  unsigned page_num = 0;
  bool     has_more = true;
  while (has_more) {
    //request.SetMaxKeys(4);
    auto outcome = s3_client.ListObjectVersions(request);
    if (!outcome.IsSuccess()) {
      std::cerr << bucket_name << " retry: " << __func__ << ": "
		<< outcome.GetError().GetMessage() << std::endl;
      // retry one time
      outcome = s3_client.ListObjectVersions(request);
      if (!outcome.IsSuccess()) {
	std::cerr << bucket_name << " Error: " << __func__ << ": "
		  << outcome.GetError().GetMessage() << std::endl;
	return false;
      }
    }

    auto & listing = outcome.GetResult();
    const Aws::Vector<Aws::S3::Model::ObjectVersion>& objects = listing.GetVersions();
    has_more = listing.GetIsTruncated();

    auto nextMarker = listing.GetNextKeyMarker();
    request.SetKeyMarker(nextMarker);
    auto nextVerMarker = listing.GetNextVersionIdMarker();
    request.SetVersionIdMarker(nextVerMarker);

#if 0
    std::cout << "\nPage #" << page_num << "::obj count=" << objects.size()
	      << ", markers are " << nextMarker << "||" << nextVerMarker << std::endl;
#endif
    dels_cnt += listing.GetDeleteMarkers().size();

    char buff[64];
    for (const Aws::S3::Model::ObjectVersion &object: objects) {
      objs_cnt ++;
      if (object.GetKey() == prev_key) {
	vers_cnt++;
      }
      else {
	prev_key = object.GetKey();
      }
#if 0
      std::cout << object.GetKey() << "::" << object.GetVersionId()
		<< "::ETag=" << object.GetETag() << std::endl;
#endif
      const auto   & etag      = object.GetETag();
      // on disk allocation is done in 4KB units
      // round up to find the on-disk space used by the object
      const uint32_t size      = div_up(object.GetSize(), 4*1024);
      const uint16_t num_parts = get_num_parts(etag);

      const unsigned nbytes    = etag.copy(buff, 32, 1);
      const uint64_t high      = hex2int(buff, buff+16);
      const uint64_t low       = hex2int(buff+16, buff+32);
      Key key = {high, low, size, num_parts, 0};
      std::unique_lock<std::mutex> lock(dict_mtx);
      auto itr = p_etags_dict->find(key);
      if (itr == p_etags_dict->end()) {
	(*p_etags_dict)[key] = 1;
	uniq_cnt ++;
      }
      else {
	itr->second ++;
      }
    }

    page_num++;
  }

  p_stats->objs_cnt += objs_cnt;
  p_stats->uniq_cnt += uniq_cnt;
  p_stats->dels_cnt += dels_cnt;
  p_stats->vers_cnt += vers_cnt;

  return true;
}

//---------------------------------------------------------------------------
static Aws::S3::S3Client* allocate_s3Client(const Aws::Client::ClientConfiguration &clientConfig,
					    const params_t &params)
{
  Aws::S3::S3Client *p_s3Client;
  if (params.access_key != nullptr && params.secret_key != nullptr) {
    Aws::Auth::AWSCredentials credentials(params.access_key, params.secret_key);
    credentials.SetAWSAccessKeyId(params.access_key);
    credentials.SetAWSSecretKey(params.secret_key);
    Aws::Client::AWSAuthV4Signer::PayloadSigningPolicy signPayloads;
    p_s3Client = new Aws::S3::S3Client(credentials, clientConfig, signPayloads, false);
  }
  else {
    p_s3Client = new Aws::S3::S3Client(clientConfig);
  }

  if (p_s3Client == nullptr) {
    std::cerr << "Failed calling: new Aws::S3::S3Client()" << std::endl;
  }
  return p_s3Client;
}

//---------------------------------------------------------------------------
static bool is_versioning_enabled_bucket(Aws::S3::S3Client & s3_client,
					 const std::string & bucket_name,
					 bool verbose)
{
  Model::GetBucketVersioningRequest request;
  request.WithBucket(bucket_name);
  auto out = s3_client.GetBucketVersioning(request);
  if (!out.IsSuccess()) {
    std::cerr << "Error: " << __func__ << ": "
	      << out.GetError().GetMessage() << std::endl;
    return false;
  }
  auto status = out.GetResult().GetStatus();
  if (verbose) {
    if (status == BucketVersioningStatus::NOT_SET) {
      //std::cout << bucket_name << "::versions NOT_SET" << std::endl;
    }
    else if (status == BucketVersioningStatus::Enabled) {
      std::cout << bucket_name << "::versions Enabled" << std::endl;
    }
    else if (status == BucketVersioningStatus::Suspended) {
      std::cout << bucket_name << "::versions Suspended" << std::endl;
    }
    else {
      std::cerr << bucket_name << "::bad version status" << std::endl;
    }
  }

  return (status == BucketVersioningStatus::Enabled);
}

//---------------------------------------------------------------------------
bool ListObjects(const Aws::Client::ClientConfiguration &clientConfig,
		 const params_t &params,
		 const std::vector<std::string> & bucket_names,
		 unsigned thread_id,
		 unsigned threads_count,
		 MD5_Dict *p_etags_dict,
		 stat_counters_t *p_stats)
{
  Aws::S3::S3Client *p_s3Client = allocate_s3Client(clientConfig, params);
  if (p_s3Client) {
    for (unsigned idx = 0; idx < bucket_names.size(); idx++) {
      if ( idx % threads_count == thread_id) {
#if 0
	std::unique_lock<std::mutex> lock(print_mtx);
	std::cout << "Thread: " << thread_id << " processing bucket: " << bucket_names[idx] << std::endl;
	lock.unlock();
#endif
	bool success;
	if (is_versioning_enabled_bucket(*p_s3Client, bucket_names[idx], true) ) {
	  success = list_objects_versions_single_bucket(*p_s3Client, bucket_names[idx],
							p_etags_dict, p_stats);
	}
	else {
	  success = list_objects_single_bucket(*p_s3Client, bucket_names[idx],
					       p_etags_dict, p_stats);
	}
	if (!success) {
	  p_stats->bad_bucket_cnt++;
	}
      }
    }
    delete p_s3Client;
    return true;
  }
  else {
    return false;
  }

}

//---------------------------------------------------------------------------
static int read_bucket_names_from_file(const char *filename, std::set<std::string> *buckets)
{
  std::ifstream ifs;
  ifs.open(filename, std::ifstream::in);
  if (ifs.fail()) {
    std::cerr << "Failed to open file " << filename << std::endl;
    return -1;
  }

  std::string line;
  while (std::getline(ifs, line)) {
    buckets->insert(line);
  }
  return 0;
}

//---------------------------------------------------------------------------
static int filter_buckets_list(std::vector<std::string> &bucket_names, const struct params_t *params)
{
  std::set<std::string> skip_buckets;
  std::set<std::string> allowed_buckets;

  if (!params->skip_buckets && !params->allowed_buckets) {
    // nothing to do
    return 0;
  }

  if (params->skip_buckets) {
    if (read_bucket_names_from_file(params->skip_buckets, &skip_buckets) != 0) {
      return -1;
    }
  }
  if (params->allowed_buckets) {
    if (read_bucket_names_from_file(params->allowed_buckets, &allowed_buckets) != 0) {
      return -1;
    }
  }

  if ((skip_buckets.size() > 0) || (allowed_buckets.size() > 0)) {
    std::sort(bucket_names.begin(), bucket_names.end());

    if (allowed_buckets.size() > 0) {
      std::vector<std::string> intersection;
      std::set_intersection(bucket_names.begin(), bucket_names.end(),
			    allowed_buckets.begin(), allowed_buckets.end(),
			    std::back_inserter(intersection));
      bucket_names.swap(intersection);
    }

    if (skip_buckets.size() > 0) {
      std::vector<std::string> diff;
      std::set_difference(bucket_names.begin(), bucket_names.end(),
			  skip_buckets.begin(), skip_buckets.end(),
			  std::inserter(diff, diff.begin()));
      bucket_names.swap(diff);
    }
  }

  return 0;
}

//---------------------------------------------------------------------------
static bool argv_name_is(const char **argv, unsigned idx, const char *name)
{
  return(strncmp(argv[idx], name, strlen(name)) == 0);
}

//---------------------------------------------------------------------------
static int get_argv_val(const char **argv, unsigned idx, unsigned val_max)
{
  const char *pch = strchr(argv[idx], '=');
  if (!pch) {
    std::cerr << __func__ << "::Bad value in argv[" << idx << "] (missing '=' sign before value)" << std::endl;
    return -1;
  }

  int val = atoi(pch+1);
  if (val <= 0 || val > val_max) {
    std::cerr << __func__ << "::Bad value for argv[" << idx << "] '" << val
	      << "' (max legal value is " << val_max << ")" << std::endl;
    return -1;
  }

  return val;
}

//---------------------------------------------------------------------------
static const char* get_argv_string(const char **argv, unsigned idx)
{
  const char *pch = strchr(argv[idx], '=');
  if (!pch) {
    std::cerr << __func__ << "::Bad value in argv[" << idx << "] (missing '=' sign before value)" << std::endl;
    return nullptr;
  }

  return (pch+1);
}

//---------------------------------------------------------------------------
static int validate_endpoint(const char *endpoint)
{
  const char *http_prefix = "http";
  int ret = strncmp( http_prefix, endpoint, strlen(http_prefix));
  if (ret != 0) {
    std::cerr << "endpoint url must start with an 'http://' prefix" << std::endl;
    return -1;
  }

  const char *pch = strrchr(endpoint, ':');
  if (!pch) {
    std::cerr << "endpoint url must include port number" << std::endl;
    return -1;
  }

  int port = atoi(pch+1);
  if (port <= 0 || port > 0xFFFF) {
    std::cerr << "endpoint url has an illegal port number <" << port << ">" << std::endl;
    return -1;
  }

  return 0;
}

//---------------------------------------------------------------------------
int usage(const char **argv)
{
  std::cerr << "\nusage: " << argv[0] << " [options] \n"
    "options:\n"
    "   --skip_buckets=skip-buckets-filename\n"
    "        pass in a filename containing list of bucket names to skip\n"
    "   --allowed_buckets=allowed-buckets-filename\n"
    "        pass in a filename containing list of all allowed bucket names to process\n"
    "   --thread-count=count\n"
    "        set the number of threads to run (default 4 threads)\n"
    "   --endpoint=url:port\n"
    "        set the endpoint url of the s3 gateway (default http://127.0.0.1:8000)\n"
    "   --access-key=key\n"
    "        set the access key to the S3 GW (default empty - take key from configuration)\n"
    "   --secret-key=key\n"
    "        set the secret key to the S3 GW (default empty - take key from configuration)\n"
	    << std::endl;
  return -1;
}

//---------------------------------------------------------------------------
static int check_argv(int argc, const char **argv, struct params_t *params)
{
  constexpr unsigned THREAD_COUNT_MAX = 32; // no more than 32 threads

  for (int i = 1; i < argc; i++) {
    if (argv_name_is(argv, i, "--help")) {
      return -1;
    }
    else if (argv_name_is(argv, i, "--thread-count")) {
      params->threads_count = get_argv_val(argv, i, THREAD_COUNT_MAX);
      if (params->threads_count <= 0) {
	return -1;
      }
    }
    else if (argv_name_is(argv, i, "--skip_buckets")) {
      params->skip_buckets = get_argv_string(argv, i);
      if (params->skip_buckets == nullptr) {
	return -1;
      }
    }
    else if (argv_name_is(argv, i, "--allowed_buckets")) {
      params->allowed_buckets = get_argv_string(argv, i);
      if (params->allowed_buckets == nullptr) {
	return -1;
      }
    }
    else if (argv_name_is(argv, i, "--access-key")) {
      params->access_key = get_argv_string(argv, i);
      if (params->access_key == nullptr) {
	return -1;
      }
    }
    else if (argv_name_is(argv, i, "--secret-key")) {
      params->secret_key = get_argv_string(argv, i);
      if (params->secret_key == nullptr) {
	return -1;
      }
    }
    else if (argv_name_is(argv, i, "--endpoint")) {
      params->endpoint = get_argv_string(argv, i);
      if (params->endpoint == nullptr || validate_endpoint(params->endpoint) == -1) {
	return -1;
      }
    }
    else {
      cerr << __func__ << "::Bad argument argv[" << i << "] = " << argv[i] << std::endl;
      return -1;
    }
  }

  return 0;
}

//---------------------------------------------------------------------------
int main(int argc, const char **argv)
{
  struct params_t params;
  if (check_argv(argc, argv, &params) != 0) {
    std::cerr << "failed check_argv" << std::endl;
    return usage(argv);
  }
  cout << params << std::endl;
  unsigned thread_count = params.threads_count;
  uint64_t objs_cnt = 0, uniq_cnt = 0;
  uint64_t dels_cnt = 0, vers_cnt = 0;
  uint32_t bad_bucket_cnt = 0;
  MD5_Dict etags_dict;

  std::thread* thread_arr[thread_count];
  stat_counters_t stats_arr[thread_count];
  //uint64_t objs_cnt_arr[thread_count];
  //uint64_t uniq_cnt_arr[thread_count];

  memset(thread_arr, 0, sizeof(thread_arr));
  memset(stats_arr, 0, sizeof(stats_arr));
  //memset(objs_cnt_arr, 0, sizeof(objs_cnt_arr));
  //memset(uniq_cnt_arr, 0, sizeof(uniq_cnt_arr));

  unsigned thread_id = 0;
  std::vector<std::string> bucket_names;
  unsigned num_buckets = 0;

  Aws::SDKOptions options;
  //options.loggingOptions.logLevel = Aws::Utils::Logging::LogLevel::Debug;
  Aws::InitAPI(options); // Should only be called once.
  Aws::Client::ClientConfiguration clientConfig;
  clientConfig.endpointOverride = params.endpoint;
  Aws::S3::S3Client *p_s3Client = allocate_s3Client(clientConfig, params);
  if (p_s3Client != nullptr) {
    auto reply = p_s3Client->ListBuckets();
    delete p_s3Client;
    if (!reply.IsSuccess()) {
      std::cerr << "s3Client.ListBuckets() failed with error: " << reply.GetError() << std::endl;
      Aws::ShutdownAPI(options); // Should only be called once.
      return -1;
    }

    num_buckets = reply.GetResult().GetBuckets().size();

    for (auto &bucket: reply.GetResult().GetBuckets()) {
      //std::cout << "bucket = " << bucket.GetName() << std::endl;
      bucket_names.emplace_back(bucket.GetName());
    }
  }
  else {
    Aws::ShutdownAPI(options); // Should only be called once.
    return -1;
  }

  if (filter_buckets_list(bucket_names, &params) != 0) {
    return usage(argv);
  }

  unsigned max_thread = std::min(thread_count, num_buckets);
  std::cout << "thread_count = " << thread_count << ", Actual Thread_COUNT = " << max_thread << std::endl;
  for (thread_id = 0; thread_id < max_thread; thread_id++) {
    thread_arr[thread_id] = new std::thread(ListObjects, clientConfig, params, bucket_names, thread_id,
					    max_thread, &etags_dict, stats_arr+thread_id);
  }

  for( int id = 0; id < thread_id; id ++ ) {
    if( thread_arr[id] != nullptr ) {
      //std::cout << "Join Thread-ID=" << id << std::endl;
      thread_arr[id]->join();
      delete thread_arr[id];
      thread_arr[id] =  nullptr;
    }
  }
  for (int id = 0; id < thread_id; id ++ ) {
    objs_cnt += stats_arr[id].objs_cnt;
    uniq_cnt += stats_arr[id].uniq_cnt;
    dels_cnt += stats_arr[id].dels_cnt;
    vers_cnt += stats_arr[id].vers_cnt;
    bad_bucket_cnt += stats_arr[id].bad_bucket_cnt;
  }
  std::cout << "===========================================================================\n" << std::endl;
  if (bad_bucket_cnt > 0) {
    std::cerr << "Error: " << __func__ << ": We skipped "
	      << bad_bucket_cnt << " bad buckets" << std::endl;
  }
  std::cout << "bucket count: " << num_buckets << ", total objects: " << objs_cnt << std::endl;
  std::cout << "We had " << uniq_cnt << " unique keys from a total of " << objs_cnt << " keys" << std::endl;
  if (dels_cnt) {
    std::cout << "We had " << dels_cnt << " deleted objs" << std::endl;
  }
  if (vers_cnt) {
    std::cout << "We had " << vers_cnt << " older objs versions" << std::endl;
  }
  print_report(etags_dict);

  Aws::ShutdownAPI(options); // Should only be called once.
  return 0;
}
//===========================================================================
//                                 E O F
//===========================================================================
