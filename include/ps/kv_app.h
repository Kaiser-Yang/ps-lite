/**
 *  Copyright (c) 2015 by Contributors
 */
#ifndef PS_KV_APP_H_
#define PS_KV_APP_H_
#include <limits>
#include <algorithm>
#include <utility>
#include <vector>
#include <unordered_map>
#include <ctime>
#include <chrono>
#include <cmath>
#include "ps/base.h"
#include "ps/simple_app.h"
#include "my_thread_pool.h"
#include "mxnet/tensor_blob.h"
#include "mxnet/base.h"

namespace ps {

/**
 * \brief the structure for a list of key-value pairs
 *
 * The keys must be unique and sorted in an increasing order.  The length of a
 * value can be more than one. If \a lens is empty, then the length
 * of a value is determined by `k=vals.size()/keys.size()`.  The \a i-th KV pair
 * is then
 *
 * \verbatim {keys[i], (vals[i*k], ..., vals[(i+1)*k-1])} \endverbatim
 *
 * If \a lens is given, then `lens[i]` is the length of the \a i-th
 * value. Let
 *
 * \verbatim n = lens[0] + .. + lens[i-1]  \endverbatim
 *
 * then the \a i-th KV pair is presented as
 *
 * \verbatim {keys[i], (vals[n], ..., vals[lens[i]+n-1])} \endverbatim
 */
template <typename Val>
struct KVPairs {
  // /** \brief empty constructor */
  // KVPairs() {}
  /** \brief the list of keys */
  SArray<Key> keys;
  /** \brief the according values */
  SArray<Val> vals;
  /** \brief the according value lengths (could be empty) */
  SArray<int> lens;
  /** \brief priority */
  int priority = 0;
};

/** \brief meta information about a kv request */
struct KVMeta {
  /** \brief the int cmd */
  int cmd;
  /** \brief whether or not this is a push request */
  bool push;
  /** \brief whether or not this is a pull request */
  bool pull;
  /** \brief sender's node id */
  int sender;
  /** \brief the associated timestamp */
  int timestamp;
  /** \brief the customer id of worker */
  int customer_id;

  int control_cmd;
  int num_aggregation;
  int key;
  int version;
  int num_merge;
  int app_id;
};

/**
 * \brief A worker node that can \ref Push (\ref Pull) key-value pairs to (from) server
 * nodes
 *
 * \tparam Val the type of value, which should be primitive types such as
 * int32_t and float
 */
template<typename Val>
class KVWorker : public SimpleApp {
 public:
  /** avoid too many this-> */
  using SimpleApp::obj_;
  /**
   * \brief callback function for \ref Push and \ref Pull
   *
   * It is called by the data receiving thread of this instance when the push or
   * pull is actually finished. Namely the kv pairs have already written into
   * servers' data structure or the kv pairs have already pulled back.
   */
  using Callback = std::function<void()>;
  using ReqHandle = std::function<void(const KVMeta& req_meta,
                                       const KVPairs<Val>& req_data,
                                       KVWorker* server)>;
  void set_request_handle(const ReqHandle& request_handle) {
    CHECK(request_handle) << "invalid request handle";
    request_handle_ = request_handle;
  }

  /**
   * \brief constructor
   *
   * \param app_id the app id, should match with \ref KVServer's id
   * \param customer_id the customer id which is unique locally
   */
  explicit KVWorker(int app_id, int customer_id) : SimpleApp() {
    using namespace std::placeholders;
    slicer_ = std::bind(&KVWorker<Val>::DefaultSlicer, this, _1, _2, _3);
    obj_ = new Customer(app_id, customer_id, std::bind(&KVWorker<Val>::Process, this, _1));
  }

  /** \brief deconstructor */
  virtual ~KVWorker() { delete obj_; obj_ = nullptr; }

  /**
   * \brief Pushes a list of key-value pairs to all server nodes.
   *
   * This function pushes a KV list specified by \a keys and \a vals to all
   * server nodes.
   *
   * Sample usage: the following codes push two KV pairs `{1, (1.1, 1.2)}` and `{3,
   * (3.1,3.2)}` to server nodes, where the value is a length-2 float vector
   * \code
   *   KVWorker<float> w;
   *   std::vector<Key> keys = {1, 3};
   *   std::vector<float> vals = {1.1, 1.2, 3.1, 3.2};
   *   w.Push(keys, vals);
   * \endcode
   *
   * If \a lens is given, then the value can be various length. See
   * \ref KVPairs for more information.
   *
   * The KV list is partitioned and sent based on the key range each server
   * maintaining. This function returns without waiting the data are sent
   * actually. Instead, use either \ref Wait or the callback to know when
   * finished. This function is thread-safe.
   *
   * @param keys a list of keys, must be unique and sorted in increasing order
   * @param vals the according values
   * @param lens optional, lens[i] stores the value length of the \a
   * i-th KV pair
   * @param cmd an optional command sent to the servers
   * @param cb the callback which is called when the push is finished.
   * @return the timestamp of this request
   */
  int Push(const std::vector<Key>& keys,
           const std::vector<Val>& vals,
           const std::vector<int>& lens = {},
           int cmd = 0,
           const Callback& cb = nullptr,
           int priority = 0,
           int uniq_key=0,
           int key_version=0) {
    return ZPush(
        SArray<Key>(keys), SArray<Val>(vals), SArray<int>(lens), cmd, cb,
        priority, false, 0, uniq_key, key_version);
  }

  /**
   * \brief Pulls the values associated with the keys from the server nodes
   *
   * This function pulls the values of the keys specified in \a keys from the
   * server nodes. The format is same to \ref KVPairs
   *
   * Sample usage: the following codes pull the values of keys \a 1 and \a 3
   * from the server nodes.
   * \code
   *   KVWorker<float> w;
   *   std::vector<Key> keys = {1, 3};
   *   std::vector<float> vals;
   *   w.Pull(keys, &vals);
   * \endcode
   *
   * It's a non-blocking call. The actual pulling is finished,
   * namely \a vals (and \a lens) is filled with pulled values, only
   * if \ref Wait returns or the callback is called.
   *
   * @param keys a list of keys, must be unique and sorted in increasing order
   * @param vals the buffer for the pulled values. It can be 0 size.
   * @param lens optional buffer for the value length. If set, it can be 0 size.
   * @param cmd an optional command sent to the servers
   * @param cb the callback which is called when the pull is finished.
   * @return the timestamp of this request
   */
  int Pull(const std::vector<Key>& keys,
           std::vector<Val>* vals,
           std::vector<int>* lens = nullptr,
           int cmd = 0,
           const Callback& cb = nullptr,
           int priority = 0) {
    SArray<Key> skeys(keys);
    int ts = AddPullCB(skeys, vals, lens, cmd, cb);
    KVPairs<Val> kvs;
    kvs.keys = skeys;
    kvs.priority = priority;
    Send(ts, false, true, cmd, kvs);
    return ts;
  }

  /**
   * \brief Pushes and Pulls a list of key-value pairs to and from the server
   * nodes.
   *
   * This function pushes the values of the keys specified in \a keys to the
   * server nodes and subsequently pulls and updates the values in \a vals.
   *
   * Sample usage: the following code pushes and pulls the values of keys
   * \a 1 and \a 3 to and from the server nodes.
   * \code
   *   KVWorker<float> w;
   *   std::vector<Key> keys = {1, 3};
   *   std::vector<float> vals;
   *   w.PushPull(keys, &vals);
   * \endcode
   *
   * It's a non-blocking call. The actual pulling is finished,
   * namely \a vals (and \a lens) is filled with pulled values, only
   * if \ref Wait returns or the callback is called.
   *
   * @param keys a list of keys, must be unique and sorted in increasing order
   * @param vals the according values
   * @param outs the buffer for the pulled values. It can be 0 size.
   * @param lens optional buffer for the value length. If set, it can be 0 size.
   * @param cmd an optional command sent to the servers
   * @param cb the callback which is called when the pull is finished.
   * @return the timestamp of this request
   */
  int PushPull(const std::vector<Key>& keys,
               const std::vector<Val>& vals,
               std::vector<Val>* outs,
               std::vector<int>* lens = nullptr,
               int cmd = 0,
               const Callback& cb = nullptr,
               int priority = 0) {
    CHECK_NOTNULL(outs);
    if (outs->empty())
      outs->resize(vals.size());
    else
      CHECK_EQ(vals.size(), outs->size());

    SArray<Key> skeys(keys);
    SArray<Val> svals(vals);
    auto souts = new SArray<Val>(outs->data(), outs->size());
    SArray<int>* slens = lens ?
        new SArray<int>(lens->data(), lens->size()) : nullptr;
    int ts = ZPushPull(skeys, svals, souts, slens, cmd,
        [this, cb, souts, slens]() {
          delete souts;
          delete slens;
          if (cb) cb();
        }, priority);
    return ts;
  }

  /**
   * \brief Waits until a push or pull has been finished
   *
   * Sample usage:
   * \code
   *   int ts = w.Pull(keys, &vals);
   *   Wait(ts);
   *   // now vals is ready for use
   * \endcode
   *
   * \param timestamp the timestamp returned by the push or pull
   */
  void Wait(int timestamp) { obj_->WaitRequest(timestamp); }

  /**
   * \brief zero-copy Push
   *
   * This function is similar to \ref Push except that all data
   * will not be copied into system for better performance. It is the caller's
   * responsibility to keep the content to be not changed before actually
   * finished.
   */
  int ZPush(const SArray<Key>& keys,
            const SArray<Val>& vals,
            const SArray<int>& lens = {},
            int cmd = 0,
            const Callback& cb = nullptr,
            int priority = 0,
            bool isInit = false,
            int key = 0,
            int uniq_key = 0,
            int version = 0) {
    int ts = obj_->NewRequest(kServerGroup);
    AddCallback(ts, cb);
    KVPairs<Val> kvs;
    kvs.keys = keys;
    kvs.vals = vals;
    kvs.lens = lens;
    kvs.priority = priority;
    // for init operation, we only need worker 0 to send data, so we'll use the original version.
      Meta meta;
      meta.push = true;
      meta.num_aggregation = 1;
      meta.key = key;
    if (!isInit && GetEnv("ENABLE_LEMETHOD", 0)) {
      // ZPush is Called by Engine::Get()->PushAsync(),
      // so LocalAggregation must be Called in the Customer::recv_thread_.
      // otherwise in LocalAggregation, it will call WaitToRead() and will form a deadlock,
      // because now thread is blocked for WaitToRead()
      // and WaitToRead() can not be started for now thread is not finished.
      // therefore we need Send() and let recv_thread_ call LocalAggregation().
      Message msg;
      msg.meta.app_id = 0;
      msg.meta.customer_id = 0;
      msg.meta.push = true;
      msg.meta.num_aggregation = 1;
      msg.meta.key = key;
      msg.meta.head = cmd;
      msg.meta.control.cmd = Control::LOCAL_AGGREGATION;
      msg.meta.timestamp = ts;
      msg.meta.sender = Postoffice::Get()->van()->my_node().id;
      msg.meta.recver = Postoffice::Get()->van()->my_node().id;
      msg.AddData(kvs.keys);
      msg.AddData(kvs.vals);
      msg.AddData(kvs.lens);
      Postoffice::Get()->van()->Send(msg);
    } else if (GetEnv("ENABLE_TSENGINE", 0)) {
      KVMeta meta;
      meta.cmd       = cmd;
      meta.push      = true;
      meta.sender    = Postoffice::Get()->van()->my_node().id;
      meta.timestamp = ts;
      meta.app_id = obj_->app_id();
      meta.customer_id = obj_->customer_id();
      meta.key = uniq_key;
      meta.version = version;
      meta.num_merge = 1;
      request_handle_(meta,kvs,this);
      Postoffice::Get()->van()->Ask1(meta.app_id , meta.customer_id, ts);
    } else {
      Send(ts, true, false, cmd, kvs, isInit, -1, key);
    }
    return ts;
  }

  /**
   * \brief zero-copy Pull
   *
   * This function is similar to \ref Pull except that all data
   * will not be copied into system for better performance. It is the caller's
   * responsibility to keep the content to be not changed before actually
   * finished.
   */
  int ZPull(const SArray<Key>& keys,
            SArray<Val>* vals,
            SArray<int>* lens = nullptr,
            int cmd = 0,
            const Callback& cb = nullptr,
            int priority = 0) {
    int ts = AddPullCB(keys, vals, lens, cmd, cb);
    KVPairs<Val> kvs;
    kvs.keys = keys;
    kvs.priority = priority;
    Send(ts, false, true, cmd, kvs);
    return ts;
  }

  /**
   * \brief zero-copy PushPull
   *
   * This function is similar to \ref PushPull except that all data
   * will not be copied into system for better performance. It is the caller's
   * responsibility to keep the content to be not changed before actually
   * finished.
   */
  int ZPushPull(const SArray<Key>& keys,
                const SArray<Val>& vals,
                SArray<Val>* outs,
                SArray<int>* lens = nullptr,
                int cmd = 0,
                const Callback& cb = nullptr,
                int priority = 0) {
    int ts = AddPullCB(keys, outs, lens, cmd, cb);
    KVPairs<Val> kvs;
    kvs.keys = keys;
    kvs.vals = vals;
    kvs.priority = priority;
    if (lens)
      kvs.lens = *lens;
    Send(ts, true, true, cmd, kvs);
    return ts;
  }
  using SlicedKVs = std::vector<std::pair<bool, KVPairs<Val>>>;
  /**
   * \brief a slicer partitions a key-value list according to the key ranges
   * \param send the kv list for partitioning
   * \param ranges the key ranges, ranges[i] is the key range of server i
   * \param sliced the sliced lists. slices[i] should only contains keys in
   * ranges[i] and the according values
   */
  using Slicer = std::function<void(
      const KVPairs<Val>& send, const std::vector<Range>& ranges,
      SlicedKVs* sliced)>;

  /**
   * \brief set a user-defined slicer
   */
  void set_slicer(const Slicer& slicer) {
    CHECK(slicer); slicer_ = slicer;
  }

  void PullFromReceiveKvs(int key, SArray<Val>* vals, SArray<int> *lens, int cmd, const Callback& cb);

  int AutoPull(int uniq_key,
               const SArray <Key> &keys,
               SArray <Val> *vals,
               SArray<int> *lens = nullptr,
               int cmd = 0);

  void Response(const KVMeta& req);

  void Send2(int timestamp, bool push, int cmd, const KVPairs<Val>& kvs, int uniq_key,
             int key_version, int app, int customer, int merge);

 private:
  /**
   * \brief internal pull, C/D can be either SArray or std::vector
   */
  template <typename C, typename D>
  int AddPullCB(const SArray<Key>& keys, C* vals, D* lens,
            int cmd, const Callback& cb);
  /**
   * \brief add a callback for a request. threadsafe.
   * @param cb callback
   * @param timestamp the timestamp of the request
   */
  void AddCallback(int timestamp, const Callback& cb) {
    if (!cb) return;
    std::lock_guard<std::mutex> lk(mu_);
    callbacks_[timestamp] = cb;
  }

  /**
   * \brief run and delete the callback
   * \param timestamp the timestamp of the callback
   */
  void RunCallback(int timestamp);
  /**
   * \brief send the kv list to all servers
   * @param timestamp the timestamp of the request
   * @param push whether or not it is a push request
   * @param push whether or not it is a pull request
   * @param cmd command
   */
  void Send(int timestamp,
            bool push,
            bool pull,
            int cmd,
            const KVPairs<Val>& kvs,
            bool isInit = false,
            int receiver = -1,
            int key = 0,
            int uniq_key = 0,
            int key_version = 0);

  /** \brief internal receive handle */
  void Process(const Message& msg);
  void LocalAggregation(const int cmd, const Meta& reqMeta, const KVPairs<Val>& reqData);
  /** \brief default kv slicer */
  void DefaultSlicer(const KVPairs<Val>& send,
                     const std::vector<Range>& ranges,
                     SlicedKVs* sliced);

  void ModelDistribution(const Meta reqMeta, const KVPairs<Val>* kvs);

  void AutoPullRpy(const int sender);

  void AutoPullUpdate(const int version,const int iters, const int req, const KVPairs<Val>& kvs);

  std::unordered_map<int, std::unordered_map<Key, KVPairs<Val>>> auto_pull_kvs_;
  std::unordered_map<int, int> data_version_;
  std::condition_variable cond_;
  int memo = -1;
  MyThreadPool pool;
  int send_push = 0;
  ReqHandle request_handle_;

  /** \brief data buffer for received kvs for each timestamp */
  std::unordered_map<int, std::vector<KVPairs<Val>>> recv_kvs_;
  /** \brief callbacks for each timestamp */
  std::unordered_map<int, Callback> callbacks_;
  /** \brief lock */
  std::mutex mu_;
  /** \brief kv list slicer */
  Slicer slicer_;
  // How many workers's local model has been aggregated on the current node
  int num_aggregation_ = 0;
  std::condition_variable cv_;
  std::unordered_map<int, KVPairs<Val>> receive_kvs_;
  std::unordered_map<int, SArray<Val>> update_buf_;
  enum class RequestType { kDefaultPushPull, kRowSparsePushPull, kCompressedPushPull };

  struct DataHandleType {
    RequestType requestType;
    int dtype;
  };

  DataHandleType DepairDataHandleType(int cmd) {
    int w = std::floor((std::sqrt(8 * cmd + 1) - 1) / 2);
    int t = ((w * w) + w) / 2;
    int y = cmd - t;
    int x = w - y;
    CHECK_GE(x, 0);
    CHECK_GE(y, 0);
    DataHandleType type;
    type.requestType = static_cast<RequestType>(x);
    type.dtype       = y;
    return type;
  }
};

/**
 * \brief A server node for maintaining key-value pairs
 */
template <typename Val>
class KVServer : public SimpleApp {
 public:
  /**
   * \brief constructor
   * \param app_id the app id, should match with \ref KVWorker's id
   */
  explicit KVServer(int app_id) : SimpleApp() {
    using namespace std::placeholders;
    obj_ = new Customer(app_id, app_id, std::bind(&KVServer<Val>::Process, this, _1));
  }

  /** \brief deconstructor */
  virtual ~KVServer() { delete obj_; obj_ = nullptr; }

  /**
   * \brief the handle to process a push/pull request from a worker
   * \param req_meta meta-info of this request
   * \param req_data kv pairs of this request
   * \param server this pointer
   */
  using ReqHandle = std::function<void(const KVMeta& req_meta,
                                       const KVPairs<Val>& req_data,
                                       KVServer* server)>;
  void set_request_handle(const ReqHandle& request_handle) {
    CHECK(request_handle) << "invalid request handle";
    request_handle_ = request_handle;
  }

  /**
   * \brief response to the push/pull request
   * \param req the meta-info of the request
   * \param res the kv pairs that will send back to the worker
   */
  void Response(const KVMeta& req, const KVPairs<Val>& res = KVPairs<Val>());

  void AutoPullUpdate(const int version, const KVMeta& req, const KVPairs<Val>& kvs = KVPairs<Val>()) {
    iter++;
    int throughput = -1;
    int last_recv_id = -1;
    while (true) {
      int receiver = Postoffice::Get()->van()->GetReceiver(throughput, last_recv_id, iter);
      if (receiver == -1) { break; }
      if (kvs.keys.size()) {
        Message msg;
        msg.meta.app_id = obj_->app_id();
        msg.meta.customer_id = obj_->customer_id();
        msg.meta.request = true;
        msg.meta.push = false;
        msg.meta.sender = Postoffice::Get()->van()->my_node().id;
        msg.meta.recver = receiver;
        msg.meta.key = req.key;
        msg.meta.version = version;
        msg.meta.iters = iter;
        msg.meta.timestamp = -1;
        msg.AddData(kvs.keys);
        msg.AddData(kvs.vals);
        if (kvs.lens.size()) {
          msg.AddData(kvs.lens);
        }
        // the origin codes are wrong,
        // because clock will not count the time when the process is sleeping
        auto starts = std::chrono::high_resolution_clock::now();
        Postoffice::Get()->van()->Send(msg);
        Postoffice::Get()->van()->Wait_for_finished();
        auto ends = std::chrono::high_resolution_clock::now();
        const std::chrono::duration<double> diff = ends - starts;
        throughput = std::numeric_limits<int>::max() - int(diff.count() * 1000000);
        last_recv_id = receiver;
      }
    }
  }

 private:
  /** \brief internal receive handle */
  void Process(const Message& msg);
  /** \brief request handle */
  ReqHandle request_handle_;
  int iter = -1;
};


/**
 * \brief an example handle adding pushed kv into store
 */
template <typename Val>
struct KVServerDefaultHandle {
  void operator()(
      const KVMeta& req_meta, const KVPairs<Val>& req_data, KVServer<Val>* server) {
    size_t n = req_data.keys.size();
    KVPairs<Val> res;
    if (!req_meta.pull) {
      CHECK_EQ(n, req_data.vals.size());
    } else {
      res.keys = req_data.keys; res.vals.resize(n);
    }
    for (size_t i = 0; i < n; ++i) {
      Key key = req_data.keys[i];
      if (req_meta.push) {
        store[key] += req_data.vals[i];
      }
      if (req_meta.pull) {
        res.vals[i] = store[key];
      }
    }
    server->Response(req_meta, res);
  }
  std::unordered_map<Key, Val> store;
};


///////////////////////////////////////////////////////////////////////////////

template <typename Val>
void KVServer<Val>::Process(const Message& msg) {
  if (msg.meta.simple_app) {
    SimpleApp::Process(msg); return;
  }
  KVMeta meta;
  meta.cmd       = msg.meta.head;
  meta.push      = msg.meta.push;
  meta.pull      = msg.meta.pull;
  meta.sender    = msg.meta.sender;
  meta.timestamp = msg.meta.timestamp;
  meta.customer_id = msg.meta.customer_id;
  meta.control_cmd = msg.meta.control.cmd;
  meta.num_aggregation = msg.meta.num_aggregation;
  meta.key = msg.meta.key;
  meta.version = msg.meta.version;
  meta.num_merge = msg.meta.iters;
  KVPairs<Val> data;
  int n = msg.data.size();
  if (n) {
    CHECK_GE(n, 2);
    data.keys = msg.data[0];
    data.vals = msg.data[1];
    if (n > 2) {
      CHECK_EQ(n, 3);
      data.lens = msg.data[2];
      CHECK_EQ(data.lens.size(), data.keys.size());
    }
    if (GetEnv("ENABLE_LEMETHOD", 0)) {
      if (msg.meta.control.cmd == Control::LOCAL_AGGREGATION) {
        Postoffice::Get()->van()->DecreaseNumAsReceiver();
      }
    }
  }
  CHECK(request_handle_);
  request_handle_(meta, data, this);
  if(GetEnv("ENABLE_TSENGINE", 0) && msg.meta.push){
    Postoffice::Get()->van()->Ask1( msg.meta.app_id , msg.meta.customer_id, msg.meta.timestamp);
  }
}

template <typename Val>
void KVServer<Val>::Response(const KVMeta& req, const KVPairs<Val>& res) {
  Message msg;
  msg.meta.app_id = obj_->app_id();
  msg.meta.customer_id = req.customer_id;
  msg.meta.request     = false;
  msg.meta.push        = req.push;
  msg.meta.pull        = req.pull;
  msg.meta.head        = req.cmd;
  msg.meta.timestamp   = req.timestamp;
  msg.meta.recver      = req.sender;
  msg.meta.key         = req.key;
  msg.meta.version     = req.version;
  if (res.keys.size()) {
    msg.AddData(res.keys);
    msg.AddData(res.vals);
    if (res.lens.size()) {
      msg.AddData(res.lens);
    }
  }
  Postoffice::Get()->van()->Send(msg);
}

template <typename Val>
void KVWorker<Val>::DefaultSlicer(
    const KVPairs<Val>& send, const std::vector<Range>& ranges,
    typename KVWorker<Val>::SlicedKVs* sliced) {
  sliced->resize(ranges.size());

  // find the positions in msg.key
  size_t n = ranges.size();
  std::vector<size_t> pos(n+1);
  const Key* begin = send.keys.begin();
  const Key* end = send.keys.end();
  for (size_t i = 0; i < n; ++i) {
    if (i == 0) {
      pos[0] = std::lower_bound(begin, end, ranges[0].begin()) - begin;
      begin += pos[0];
    } else {
      CHECK_EQ(ranges[i-1].end(), ranges[i].begin());
    }
    size_t len = std::lower_bound(begin, end, ranges[i].end()) - begin;
    begin += len;
    pos[i+1] = pos[i] + len;

    // don't send it to servers for empty kv
    sliced->at(i).first = (len != 0);
  }
  CHECK_EQ(pos[n], send.keys.size());
  if (send.keys.empty()) return;

  // the length of value
  size_t k = 0, val_begin = 0, val_end = 0;
  if (send.lens.empty()) {
    k = send.vals.size() / send.keys.size();
    CHECK_EQ(k * send.keys.size(), send.vals.size());
  } else {
    CHECK_EQ(send.keys.size(), send.lens.size());
  }

  // slice
  for (size_t i = 0; i < n; ++i) {
    if (pos[i+1] == pos[i]) {
      sliced->at(i).first = false;
      continue;
    }
    sliced->at(i).first = true;
    auto& kv = sliced->at(i).second;
    kv.keys = send.keys.segment(pos[i], pos[i+1]);
    if (send.lens.size()) {
      kv.lens = send.lens.segment(pos[i], pos[i+1]);
      for (int l : kv.lens) val_end += l;
      kv.vals = send.vals.segment(val_begin, val_end);
      val_begin = val_end;
    } else {
      kv.vals = send.vals.segment(pos[i]*k, pos[i+1]*k);
    }
  }
}

template <typename Val>
void KVWorker<Val>::Send(int timestamp,
                         bool push,
                         bool pull,
                         int cmd,
                         const KVPairs<Val>& kvs,
                         bool isInit,
                         int receiver,
                         int key,
                         int uniq_key,
                         int key_version) {
  // slice the message
  SlicedKVs sliced;
  slicer_(kvs, Postoffice::Get()->GetServerKeyRanges(), &sliced);

  // need to add response first, since it will not always trigger the callback
  int skipped = 0;
  for (size_t i = 0; i < sliced.size(); ++i) {
    if (!sliced[i].first) ++skipped;
  }
  obj_->AddResponse(timestamp, skipped);
  if ((size_t)skipped == sliced.size()) {
    RunCallback(timestamp);
  }

  for (size_t i = 0; i < sliced.size(); ++i) {
    auto& s = sliced[i];
    if (!s.first) continue;
    Message msg;
    msg.meta.app_id = obj_->app_id();
    msg.meta.customer_id = obj_->customer_id();
    msg.meta.request     = true;
    msg.meta.push        = push;
    msg.meta.pull        = pull;
    msg.meta.head        = cmd;
    msg.meta.timestamp   = timestamp;
    msg.meta.recver      = Postoffice::Get()->ServerRankToID(i);
    msg.meta.priority    = kvs.priority;
    auto& kvs = s.second;
    if (GetEnv("ENABLE_LEMETHOD", 0)) {
      msg.meta.key = key;
      msg.meta.sender = Postoffice::Get()->van()->my_node().id;
      {
        std::lock_guard<std::mutex> locker{mu_};
        msg.meta.num_aggregation = num_aggregation_;
      }
      if (isInit && push) {
        msg.meta.control.cmd = Control::INIT;
      } else if (!isInit && push) {
        msg.meta.control.cmd = Control::LOCAL_AGGREGATION;
        msg.meta.recver = receiver;
        std::lock_guard<std::mutex> locker{mu_};
        kvs.vals.CopyFrom(update_buf_[key]);
      }
    } else if (GetEnv("ENABLE_TSENGINE", 0)) {
      msg.meta.key = uniq_key;
      msg.meta.version = key_version;
    }
    if (kvs.keys.size()) {
      msg.AddData(kvs.keys);
      msg.AddData(kvs.vals);
      if (kvs.lens.size()) {
        msg.AddData(kvs.lens);
      }
    }
    Postoffice::Get()->van()->Send(msg);
  }
}

template <typename Val>
void KVWorker<Val>::Send2(int timestamp, bool push, int cmd,
                          const KVPairs<Val>& kvs, int uniq_key,
                          int key_version,int app, int customer, int merge) {
  // slice the message
  SlicedKVs sliced;
  slicer_(kvs, Postoffice::Get()->GetServerKeyRanges(), &sliced);
  // need to add response first, since it will not always trigger the callback
  int skipped = 0;
  for (size_t i = 0; i < sliced.size(); ++i) {
    if (!sliced[i].first) ++skipped;
  }
  obj_->AddResponse(timestamp, skipped);
  if ((size_t) skipped == sliced.size()) {
    RunCallback(timestamp);
  }
  for (size_t i = 0; i < sliced.size(); ++i) {
    const auto &s = sliced[i];
    if (!s.first) continue;
    Message msg;
    msg.meta.app_id = app;
    msg.meta.customer_id = customer;
    msg.meta.request = true;
    msg.meta.push = push;
    msg.meta.head = cmd;
    msg.meta.timestamp = timestamp;
    msg.meta.sender = Postoffice::Get()->van()->my_node().id;
    msg.meta.iters = merge;
    msg.meta.recver = send_push;
    msg.meta.key = uniq_key;
    msg.meta.version = key_version;
    const auto &kvs = s.second;
    if (kvs.keys.size()) {
      msg.AddData(kvs.keys);
      msg.AddData(kvs.vals);
      if (kvs.lens.size()) {
        msg.AddData(kvs.lens);
      }
    }
    Postoffice::Get()->van()->Send(msg);
    send_push = 0;
  }
}

template <typename Val>
void KVWorker<Val>::AutoPullRpy(const int sender){
  Message rpy;
  rpy.meta.recver = sender;
  rpy.meta.control.cmd = Control::AUTOPULLRPY;
  Postoffice::Get()->van()->Send(rpy);
}

template <typename Val>
void KVWorker<Val>::AutoPullUpdate(const int version, const int iters,
                                   const int req, const KVPairs<Val>& kvs ) {
  int throughput = -1;
  int last_recv_id = -1;
  while (true) {
    int receiver=Postoffice::Get()->van()->GetReceiver(throughput, last_recv_id,iters);
    if (receiver == -1) break;    //whether transmition is over
    if (kvs.keys.size()) {
      Message msg;
      msg.meta.app_id = obj_->app_id();
      msg.meta.customer_id = obj_->customer_id();
      msg.meta.request = true;
      msg.meta.push = false;
      msg.meta.sender = Postoffice::Get()->van()->my_node().id;
      msg.meta.recver = receiver;
      msg.meta.key = req;
      msg.meta.version = version;
      msg.meta.iters = iters;
      msg.meta.timestamp = -1;
      msg.AddData(kvs.keys);
      msg.AddData(kvs.vals);
      if (kvs.lens.size()) {
          msg.AddData(kvs.lens);
      }
      // the origin codes are wrong,
      // because clock will not count the time when the process is sleeping
      auto starts = std::chrono::high_resolution_clock::now();
      Postoffice::Get()->van()->Send(msg);
      Postoffice::Get()->van()->Wait_for_finished();
      auto ends = std::chrono::high_resolution_clock::now();
      const std::chrono::duration<double> diff = ends - starts;
      throughput = std::numeric_limits<int>::max() - int(diff.count() * 1000000);
      last_recv_id = receiver;
    }
  }
}

template <typename Val>
void KVWorker<Val>::Response(const KVMeta& req) {
  Message msg;
  msg.meta.app_id = obj_->app_id();
  msg.meta.customer_id = req.customer_id;
  msg.meta.request     = false;
  msg.meta.push        = req.push;
  msg.meta.head        = req.cmd;
  msg.meta.timestamp   = req.timestamp;
  msg.meta.recver      = req.sender;
  msg.meta.key         = req.key;
  msg.meta.version     = req.version;
  Postoffice::Get()->van()->Send(msg);
}

template <typename Val>
void KVWorker<Val>::LocalAggregation(const int cmd,
                                     const Meta& reqMeta,
                                     const KVPairs<Val>& reqData) {
  int key = reqMeta.key;
  DataHandleType type = DepairDataHandleType(cmd);
  CHECK_EQ(type.dtype, mshadow::kFloat32) << "LeMethod only supports for float32";
  size_t size = reqData.lens[0] / mshadow::mshadow_sizeof(type.dtype);
  std::lock_guard<std::mutex> locker{mu_};
  auto& updates = update_buf_[key];
  if (num_aggregation_ == 0) {
    // This is zero data copy.
    // Because, we just asign one pointer to another.
    updates = reqData.vals;
  } else {
    Val* lhs = updates.data();
    Val* rhs = reqData.vals.data();
    for (size_t i = 0; i < size; i++) {
      *((float*)lhs + i) += *((float*)rhs + i);
    }
  }
  num_aggregation_ += reqMeta.num_aggregation;
}

template <typename Val>
void KVWorker<Val>::Process(const Message& msg) {
  if (msg.meta.simple_app) {
    SimpleApp::Process(msg); return;
  }
  // store the data for pulling
  int ts = msg.meta.timestamp;
  if (msg.meta.pull) {
    CHECK_GE(msg.data.size(), (size_t)2);
    KVPairs<Val> kvs;
    kvs.keys = msg.data[0];
    kvs.vals = msg.data[1];
    if (msg.data.size() > (size_t)2) {
      kvs.lens = msg.data[2];
    }
    mu_.lock();
    recv_kvs_[ts].push_back(kvs);
    mu_.unlock();
  }
  if (GetEnv("ENABLE_LEMETHOD", 0)) {
    auto& ctrl = msg.meta.control;
    if (ctrl.cmd == Control::LOCAL_AGGREGATION) {
      KVPairs<Val> data;
      CHECK_EQ(msg.data.size(), 3);
      data.keys = msg.data[0];
      data.vals = msg.data[1];
      data.lens = msg.data[2];
      LocalAggregation(msg.meta.head, msg.meta, data);
      if (msg.meta.sender != msg.meta.recver) {
        Postoffice::Get()->van()->DecreaseNumAsReceiver();
        return;
      }
      // for self-sending msg, we need GetLocalAggregationReceiver
      // and we need do this in another thread
      // so that we can receive LOCAL_AGGREGATION from other workers to prevent from deadlock
      // if we do this in current thread,
      // deadlock will be caused by WaitForLocalAggregationFinish()
      // because current thread is blocked we can not receive,
      // WaitForLocalAggregationFinish() will not end forever.
      int cmd = msg.meta.head, key = msg.meta.key, ts = msg.meta.timestamp;
      std::thread t{[this, cmd, key, ts, data](){
        int receiver = Postoffice::Get()->van()->GetLocalAggregationReceiver();
        Postoffice::Get()->van()->WaitForLocalAggregationFinish();
        Send(ts, true, false, cmd, data, false, receiver, key);
        std::lock_guard<std::mutex> locker{mu_};
        num_aggregation_ = 0;
      }};
      t.detach();
    } else if (ctrl.cmd == Control::MODEL_DISTRIBUTION) {
      CHECK_EQ(msg.data.size(), (size_t)3);
      KVPairs<Val> *kvs = new KVPairs<Val>();
      kvs->keys = msg.data[0];
      kvs->vals = msg.data[1];
      kvs->lens = msg.data[2];
      {
        std::lock_guard<std::mutex> locker{mu_};
        receive_kvs_[msg.meta.key] = *kvs;
        cv_.notify_all();
      }
      std::thread t{&KVWorker<Val>::ModelDistribution, this, msg.meta, kvs};
      t.detach();
    }
  } else if (GetEnv("ENABLE_TSENGINE", 0)) {
    int key = msg.meta.key;
    int version = msg.meta.version;
    if (msg.data.size()) {
      if (msg.meta.push && msg.meta.request) {//intercept push request msg from other workers, transmit it to workersmerge in kvstore_dist.h, and send a ask1 msg to continue
        KVMeta meta;
        meta.cmd       = msg.meta.head;
        meta.push      = msg.meta.push;
        meta.sender    = msg.meta.sender;
        meta.timestamp = msg.meta.timestamp;
        meta.customer_id = msg.meta.customer_id;
        meta.key = msg.meta.key;
        meta.version = msg.meta.version;
        meta.num_merge = msg.meta.iters;

        KVPairs<Val> kvs;
        kvs.keys = msg.data[0];
        kvs.vals = msg.data[1];
        kvs.lens = msg.data[2];

        request_handle_(meta,kvs,this);
        Postoffice::Get()->van()->Ask1(msg.meta.app_id,msg.meta.customer_id,msg.meta.timestamp);
      } else{
        CHECK_GE(msg.data.size(), (size_t)2);
        KVPairs<Val> kvs;
        kvs.keys = msg.data[0];
        kvs.vals = msg.data[1];
        if (msg.data.size() > (size_t)2) {
          kvs.lens = msg.data[2];
          CHECK_EQ(kvs.keys.size(), kvs.lens.size());
        }
        if (msg.meta.request) {//autopull msg
          AutoPullRpy(msg.meta.sender);  //added by huaman, send autopull reply msg
          CHECK_EQ(kvs.keys.size(), (size_t)1);
          pool.enqueue(&KVWorker<Val>::AutoPullUpdate, this, version, msg.meta.iters, key, kvs);//opt worker pull propagation by a separate thread
          mu_.lock();
          if (data_version_.find(key) == data_version_.end() || version == data_version_[key]) {
            auto_pull_kvs_[key][kvs.keys[0]]=kvs;
            data_version_[key] = version;
          } else if (version > data_version_[key]) {
            data_version_[key] = version;
            auto_pull_kvs_.erase(key);
            auto_pull_kvs_[key][kvs.keys[0]] = kvs;
          }
          mu_.unlock();
          cond_.notify_all();
        } else {
          mu_.lock();
          recv_kvs_[ts].push_back(kvs);
          mu_.unlock();
        }
      }
    } else {
      if(msg.meta.push && msg.meta.request){//it's time to send this worker's push request msg
        send_push = msg.meta.iters;
        KVMeta meta;
        meta.num_merge = -1;//just as a opt flag
        KVPairs<char> kvs;
        request_handle_(meta,kvs,this);
      }
    }

    if (ts == -1 || msg.meta.request) { return; }
  }
  // finished, run callbacks
  if (obj_->NumResponse(ts) == Postoffice::Get()->num_servers() - 1)  {
    RunCallback(ts);
  }
}

template <typename Val>
void KVWorker<Val>::PullFromReceiveKvs(int key,
                                       SArray<Val> *vals,
                                       SArray<int> *lens,
                                       int cmd,
                                       const Callback& cb) {
  std::unique_lock<std::mutex> locker{mu_};
  cv_.wait(locker, [this, key]() { return receive_kvs_.count(key) != 0; });
  auto &kvs = receive_kvs_[key];
  Val *pVals = vals->data();
  int *pLens = nullptr;
  vals->resize(kvs.vals.size());
  if (lens != nullptr) {
    lens->resize(kvs.lens.size());
    pLens = lens->data();
  }
  memcpy(pVals, kvs.vals.data(), kvs.vals.size() * sizeof(Val));
  if (pLens != nullptr) { memcpy(pLens, kvs.lens.data(), kvs.lens.size() * sizeof(int)); }

  receive_kvs_.erase(key);

  if(cb != nullptr) { cb(); }
}


template <typename Val>
void KVWorker<Val>::ModelDistribution(const Meta reqMeta, const KVPairs<Val>* kvs) {
  int lastBandwidth = ps::Van::UNKNOWN;
  int lastReceiver = ps::Van::UNKNOWN;
  int receiver = ps::Van::UNKNOWN;
  Message msg;
  msg.meta.app_id = 0;
  msg.meta.customer_id = 0;
  msg.meta.sender = Postoffice::Get()->van()->my_node().id;
  msg.meta.timestamp = reqMeta.timestamp;
  msg.meta.control.cmd = Control::MODEL_DISTRIBUTION;
  msg.meta.key = reqMeta.key;
  msg.meta.version = reqMeta.version;
  msg.AddData(kvs->keys);
  msg.AddData(kvs->vals);
  msg.AddData(kvs->lens);
  delete kvs;
  while (true) {
    receiver = Postoffice::Get()->van()->GetModelReceiver(lastBandwidth, lastReceiver, reqMeta.version);
    if (receiver == ps::Van::QUIT) { break; }
    msg.meta.recver = receiver;
    // the origin codes are wrong,
    // because clock will not count the time when the process is sleeping
    auto starts = std::chrono::high_resolution_clock::now();
    Postoffice::Get()->van()->Send(msg);
    Postoffice::Get()->van()->WaitForModelDistributionReply();
    auto ends = std::chrono::high_resolution_clock::now();
    const std::chrono::duration<double> diff = starts - ends ;
    lastBandwidth = int(diff.count() * 1000000);
    lastReceiver = receiver;
  }
}

template <typename Val>
int KVWorker<Val>::AutoPull(int uniq_key, const SArray <Key> &keys, SArray<Val> *vals, SArray<int> *lens, int cmd) {
  std::unique_lock<std::mutex> lk(mu_);
  while(auto_pull_kvs_[uniq_key].size() != keys.size()){
    cond_.wait(lk);
  }
  auto& autokvs = auto_pull_kvs_[uniq_key];
  Val* p_vals = vals->data();
  int* p_lens = nullptr;
  size_t total_vals = 0;
  for (auto& kvs : autokvs) {
    total_vals += kvs.second.vals.size();
  }
  CHECK_NOTNULL(vals);
  if (vals->empty()) {
    vals->resize(total_vals);
  } else {
    CHECK_EQ(vals->size(), total_vals);
  }
  if (lens) {
    if (lens->empty()) {
      lens->resize(keys.size());
    } else {
      CHECK_EQ(lens->size(), keys.size());
    }
    p_lens = lens->data();
  }
  for (unsigned long key : keys){
    memcpy(p_vals, autokvs[key].vals.data(), autokvs[key].vals.size() * sizeof(Val));
    p_vals += autokvs[key].vals.size();
    if (p_lens) {
      memcpy(p_lens, autokvs[key].lens.data(), autokvs[key].lens.size() * sizeof(int));
      p_lens += autokvs[key].lens.size();
    }
  }
  auto_pull_kvs_.erase(uniq_key);

  // this data_version_ is not the same with the data_version_ of kvstore_dist.h, they belongs to different classes.
  return data_version_[uniq_key];
}

template <typename Val>
void KVWorker<Val>::RunCallback(int timestamp) {
  mu_.lock();
  auto it = callbacks_.find(timestamp);
  if (it != callbacks_.end()) {
    mu_.unlock();

    CHECK(it->second);
    it->second();

    mu_.lock();
    callbacks_.erase(it);
  }
  mu_.unlock();
}

template <typename Val>
template <typename C, typename D>
int KVWorker<Val>::AddPullCB(
    const SArray<Key>& keys, C* vals, D* lens, int cmd,
    const Callback& cb) {
  int ts = obj_->NewRequest(kServerGroup);
  AddCallback(ts, [this, ts, keys, vals, lens, cb]() mutable {
      mu_.lock();
      auto& kvs = recv_kvs_[ts];
      mu_.unlock();

      // do check
      size_t total_key = 0, total_val = 0;
      for (const auto& s : kvs) {
        Range range = FindRange(keys, s.keys.front(), s.keys.back()+1);
        CHECK_EQ(range.size(), s.keys.size())
            << "unmatched keys size from one server";
        if (lens) CHECK_EQ(s.lens.size(), s.keys.size());
        total_key += s.keys.size();
        total_val += s.vals.size();
      }
      CHECK_EQ(total_key, keys.size()) << "lost some servers?";

      // fill vals and lens
      std::sort(kvs.begin(), kvs.end(), [](
          const KVPairs<Val>& a, const KVPairs<Val>& b) {
                  return a.keys.front() < b.keys.front();
        });
      CHECK_NOTNULL(vals);
      if (vals->empty()) {
        vals->resize(total_val);
      } else {
        CHECK_EQ(vals->size(), total_val);
      }
      Val* p_vals = vals->data();
      int *p_lens = nullptr;
      if (lens) {
        if (lens->empty()) {
          lens->resize(keys.size());
        } else {
          CHECK_EQ(lens->size(), keys.size());
        }
        p_lens = lens->data();
      }
      for (const auto& s : kvs) {
        memcpy(p_vals, s.vals.data(), s.vals.size() * sizeof(Val));
        p_vals += s.vals.size();
        if (p_lens) {
          memcpy(p_lens, s.lens.data(), s.lens.size() * sizeof(int));
          p_lens += s.lens.size();
        }
      }

      mu_.lock();
      recv_kvs_.erase(ts);
      mu_.unlock();
      if (cb) cb();
    });

  return ts;
}

}  // namespace ps
#endif  // PS_KV_APP_H_
