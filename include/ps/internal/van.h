/**
 *  Copyright (c) 2015 by Contributors
 */
#ifndef PS_INTERNAL_VAN_H_
#define PS_INTERNAL_VAN_H_
#include <atomic>
#include <ctime>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <unordered_map>
#include <unordered_set>
#include <vector>
#include "ps/base.h"
#include "ps/internal/message.h"
#include "my_thread_pool.h"
namespace ps {
class Resender;
class PBMeta;
/**
 * \brief Van sends messages to remote nodes
 *
 * If environment variable PS_RESEND is set to be 1, then van will resend a
 * message if it no ACK messsage is received within PS_RESEND_TIMEOUT
 * millisecond
 */
class Van {
 public:
  /**
   * \brief create Van
   * \param type zmq, socket, ...
   */
  static Van *Create(const std::string &type);

  /** \brief constructer, do nothing. use \ref Start for real start */
  Van() {}

  /**\brief deconstructer, do nothing. use \ref Stop for real stop */
  virtual ~Van() {}

  enum RECEIVER_STATUS {
    UNKNOWN = -2
  };

  /**
   * \brief start van
   *
   * must call it before calling Send
   *
   * it initializes all connections to other nodes.  start the receiving
   * thread, which keeps receiving messages. if it is a system
   * control message, give it to postoffice::manager, otherwise, give it to the
   * corresponding app.
   */
  virtual void Start(int customer_id);

  /**
   * \brief send a message, It is thread-safe
   * \return the number of bytes sent. -1 if failed
   */
  int Send(const Message &msg);

  /**
   * \brief return my node
   */
  inline const Node &my_node() const {
    CHECK(ready_) << "call Start() first";
    return my_node_;
  }

  /**
   * \brief stop van
   * stop receiving threads
   */
  virtual void Stop();

  /**
   * \brief get next available timestamp. thread safe
   */
  inline int GetTimestamp() { return timestamp_++; }

  /**
   * \brief whether it is ready for sending. thread safe
   */
  inline bool IsReady() { return ready_; }

  void AskModelReceiver(int last_bandwidth, int last_receiver_id, int version);

  void AskLocalAggregation();

  int GetModelReceiver(int lastBandwidth, int lastReceiver, int iteration);

  int GetLocalAggregationReceiver();

  void WaitForModelDistributionReply();

  void WaitForLocalAggregationFinish();

  bool IsServerNode();

  void NoticeWorkersOneIterationFinish();

  void DecreaseNumAsReceiver();

 protected:
  /**
   * \brief connect to a node
   */
  virtual void Connect(const Node &node) = 0;

  /**
   * \brief bind to my node
   * do multiple retries on binding the port. since it's possible that
   * different nodes on the same machine picked the same port
   * \return return the port binded, -1 if failed.
   */
  virtual int Bind(const Node &node, int max_retry) = 0;

  /**
   * \brief block until received a message
   * \return the number of bytes received. -1 if failed or timeout
   */
  virtual int RecvMsg(Message *msg) = 0;

  /**
   * \brief send a mesage
   * \return the number of bytes sent
   */
  virtual int SendMsg(const Message &msg) = 0;

  /**
   * \brief pack meta into a string
   */
  void PackMeta(const Meta &meta, char **meta_buf, int *buf_size);

  /**
   * \brief pack meta into protobuf
   */
  void PackMetaPB(const Meta &meta, PBMeta *pb);

  /**
   * \brief unpack meta from a string
   */
  void UnpackMeta(const char *meta_buf, int buf_size, Meta *meta);

  Node scheduler_;
  Node my_node_;
  bool is_scheduler_;
  std::mutex start_mu_;

 private:
  /** thread function for receving */
  void Receiving();

  /** thread function for heartbeat */
  void Heartbeat();

  // node's address string (i.e. ip:port) -> node id
  // this map is updated when ip:port is received for the first time
  std::unordered_map<std::string, int> connected_nodes_;
  // maps the id of node which is added later to the id of node
  // which is with the same ip:port and added first
  std::unordered_map<int, int> shared_node_mapping_;

  /** whether it is ready for sending */
  std::atomic<bool> ready_{false};
  std::atomic<size_t> send_bytes_{0};
  size_t recv_bytes_ = 0;
  int num_servers_ = 0;
  int num_workers_ = 0;
  /** the thread for receiving messages */
  std::unique_ptr<std::thread> receiver_thread_;
  /** the thread for sending heartbeat */
  std::unique_ptr<std::thread> heartbeat_thread_;
  std::vector<int> barrier_count_;
  /** msg resender */
  Resender *resender_ = nullptr;
  int drop_rate_ = 0;
  std::atomic<int> timestamp_{0};
  int init_stage = 0;
  int local_aggregation_receiver_ = -1;
  int num_as_receiver_ = 0;
  bool can_be_receiver_ = true;

  std::unordered_set<int> unreceived_nodes_md_, unreceived_nodes_ma_;
  std::mutex mu_ma_, mu_md_, mu_on_bw_lt_;
  std::mutex mutex_on_km_ma_, mutex_on_km_md_;
  std::unordered_set<int> left_nodes_ma_, right_nodes_ma_, left_nodes_md_, right_nodes_md_;
  std::vector<std::vector<int>> edge_weight_ma_, edge_weight_md_;
  std::vector<int> receiver_ma_, sender_ma_, receiver_md_, sender_md_;
  int num_ma_ = 0, num_md_ = 0;
  std::vector<std::vector<int>> bandwidth_, lifetime_;
  std::condition_variable cv_;
  std::mutex cv_mu_;
  MyThreadPool threadPool_;
  int reply_node_id_ = -1;
  bool receive_model_distribution_reply_ = false;
  static constexpr int INF = 86400;
  static constexpr double DEFAULT_GREED_RATE = 0.5;
  static constexpr int DEFAULT_MAX_THREAD_NUM = 1000;
  static constexpr int BANDWIDTH_EXPIRATION_TIME = 5;

  double greed_rate_;
  int max_thread_num_;
  int iteration_ = 0;
  int model_receiver_ = -2;
  int ask_as_receiver_status_ = false;

  bool WaitForAskAsReceiverReply(int nodeID);

  bool CanToFloat(const char *str);

  bool CanToInteger(const char *str);

  bool IsVirtualNode(int id);

  void GetEdgeWeight(bool reverse, std::unordered_set<int>& left_nodes_, std::unordered_set<int>& right_nodes_, std::vector<std::vector<int>>& edge_weight_);

  void AddVirtualNodes(std::unordered_set<int> &leftNodes, std::unordered_set<int> &rightNodes);

  void CheckExpiration();

  int RandomGetReceiver(int rightNode);

  void KMBfs(std::unordered_set<int> &leftNodes, std::unordered_set<int> &rightNodes,
             std::vector<std::vector<int>> &edgeWeight, std::vector<int> &receiver,
             std::vector<int> &leftWeight, std::vector<int> &rightWeight,
             int startNode, int maxID);

  void KM(std::unordered_set<int> &leftNodes, std::unordered_set<int> &rightNodes,
          std::vector<std::vector<int>> &edgeWeight, std::vector<int> &receiver);

  void ProcessAskAsReceiver(Message *msg);

  void ProcessInit(Message *msg);

  void ProcessLocalAggregation(Message *msg);

  void ProcessAskModelReceiverReply(Message *msg);

  void ProcessAskLocalAggregationReply(Message *msg);

  void ProcessAskAsReceiverReply(Message *msg);

  void ProcessModelDistributionReply(Message *msg);

  void ProcessAskModelReceiver(Message msg);

  void ProcessAskLocalAggregation(Message msg);

  void ProcessReplyAskModelReceiver(Message *msg);

  void ProcessModelDistribution(Message *msg);

  void ProcessNoticeWorkersOneIterationFinish(Message *msg);

  void CheckModelDistributionFinish();

  void CheckModelAggregationFinish();

  /**
   * \brief processing logic of AddNode message for scheduler
   */
  void ProcessAddNodeCommandAtScheduler(Message *msg, Meta *nodes,
                                        Meta *recovery_nodes);

  /**
   * \brief processing logic of Terminate message
   */
  void ProcessTerminateCommand();

  /**
   * \brief processing logic of AddNode message (run on each node)
   */
  void ProcessAddNodeCommand(Message *msg, Meta *nodes, Meta *recovery_nodes);

  /**
   * \brief processing logic of Barrier message (run on each node)
   */
  void ProcessBarrierCommand(Message *msg);

  /**
   * \brief processing logic of AddNode message (run on each node)
   */
  void ProcessHearbeat(Message *msg);

  /**
   * \brief processing logic of Data message
   */
  void ProcessDataMsg(Message *msg);

  /**
   * \brief called by ProcessAddNodeCommand, in scheduler it assigns an id to
   * the newly added node; in other nodes, it updates the node id with what is
   * received from scheduler
   */
  void UpdateLocalID(Message *msg, std::unordered_set<int> *deadnodes_set,
                     Meta *nodes, Meta *recovery_nodes);

  const char *heartbeat_timeout_val =
      Environment::Get()->find("PS_HEARTBEAT_TIMEOUT");
  int heartbeat_timeout_ =
      heartbeat_timeout_val ? atoi(heartbeat_timeout_val) : 0;

  DISALLOW_COPY_AND_ASSIGN(Van);
};
}  // namespace ps
#endif  // PS_INTERNAL_VAN_H_
