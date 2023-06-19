/**
 *  Copyright (c) 2015 by Contributors
 */

#include <chrono>
#include <thread>

#include "ps/base.h"
#include "ps/internal/customer.h"
#include "ps/internal/postoffice.h"
#include "ps/internal/van.h"
#include "ps/sarray.h"

#include "./meta.pb.h"
#include "./network_utils.h"
#include "./ibverbs_van.h"
#include "./resender.h"
#include "./zmq_van.h"
#include "./p3_van.h"

namespace ps {

// interval in second between to heartbeast signals. 0 means no heartbeat.
// don't send heartbeast in default. because if the scheduler received a
// heartbeart signal from a node before connected to that node, then it could be
// problem.
static const int kDefaultHeartbeatInterval = 0;

Van* Van::Create(const std::string& type) {
  if (type == "zmq") {
    return new ZMQVan();
  } else if (type == "p3") {
    return new P3Van();
#ifdef DMLC_USE_IBVERBS
} else if (type == "ibverbs") {
    return new IBVerbsVan();
#endif
  } else {
    LOG(FATAL) << "Unsupported van type: " << type;
    return nullptr;
  }
}

void Van::ProcessTerminateCommand() {
  PS_VLOG(1) << my_node().ShortDebugString() << " is stopped";
  ready_ = false;
}

void Van::ProcessAddNodeCommandAtScheduler(Message* msg, Meta* nodes,
                                           Meta* recovery_nodes) {
  recovery_nodes->control.cmd = Control::ADD_NODE;
  time_t t = time(NULL);
  size_t num_nodes =
      Postoffice::Get()->num_servers() + Postoffice::Get()->num_workers();
  if (nodes->control.node.size() == num_nodes) {
    // sort the nodes according their ip and port,
    std::sort(nodes->control.node.begin(), nodes->control.node.end(),
              [](const Node& a, const Node& b) {
                return (a.hostname.compare(b.hostname) | (a.port < b.port)) > 0;
              });
    // assign node rank
    for (auto& node : nodes->control.node) {
      std::string node_host_ip =
          node.hostname + ":" + std::to_string(node.port);
      if (connected_nodes_.find(node_host_ip) == connected_nodes_.end()) {
        CHECK_EQ(node.id, Node::kEmpty);
        int id = node.role == Node::SERVER
                     ? Postoffice::ServerRankToID(num_servers_)
                     : Postoffice::WorkerRankToID(num_workers_);
        PS_VLOG(1) << "assign rank=" << id << " to node " << node.DebugString();
        node.id = id;
        Connect(node);
        Postoffice::Get()->UpdateHeartbeat(node.id, t);
        connected_nodes_[node_host_ip] = id;
      } else {
        int id = node.role == Node::SERVER
                     ? Postoffice::ServerRankToID(num_servers_)
                     : Postoffice::WorkerRankToID(num_workers_);
        shared_node_mapping_[id] = connected_nodes_[node_host_ip];
        node.id = connected_nodes_[node_host_ip];
      }
      if (node.role == Node::SERVER) num_servers_++;
      if (node.role == Node::WORKER) num_workers_++;
    }
    nodes->control.node.push_back(my_node_);
    nodes->control.cmd = Control::ADD_NODE;
    Message back;
    back.meta = *nodes;
    for (int r : Postoffice::Get()->GetNodeIDs(kWorkerGroup + kServerGroup)) {
      int recver_id = r;
      if (shared_node_mapping_.find(r) == shared_node_mapping_.end()) {
        back.meta.recver = recver_id;
        back.meta.timestamp = timestamp_++;
        Send(back);
      }
    }
    PS_VLOG(1) << "the scheduler is connected to " << num_workers_
               << " workers and " << num_servers_ << " servers";
    ready_ = true;
  } else if (!recovery_nodes->control.node.empty()) {
    auto dead_nodes = Postoffice::Get()->GetDeadNodes(heartbeat_timeout_);
    std::unordered_set<int> dead_set(dead_nodes.begin(), dead_nodes.end());
    // send back the recovery node
    CHECK_EQ(recovery_nodes->control.node.size(), 1);
    Connect(recovery_nodes->control.node[0]);
    Postoffice::Get()->UpdateHeartbeat(recovery_nodes->control.node[0].id, t);
    Message back;
    for (int r : Postoffice::Get()->GetNodeIDs(kWorkerGroup + kServerGroup)) {
      if (r != recovery_nodes->control.node[0].id &&
          dead_set.find(r) != dead_set.end()) {
        // do not try to send anything to dead node
        continue;
      }
      // only send recovery_node to nodes already exist
      // but send all nodes to the recovery_node
      back.meta =
          (r == recovery_nodes->control.node[0].id) ? *nodes : *recovery_nodes;
      back.meta.recver = r;
      back.meta.timestamp = timestamp_++;
      Send(back);
    }
  }
}

void Van::UpdateLocalID(Message* msg, std::unordered_set<int>* deadnodes_set,
                        Meta* nodes, Meta* recovery_nodes) {
  auto& ctrl = msg->meta.control;
  size_t num_nodes =
      Postoffice::Get()->num_servers() + Postoffice::Get()->num_workers();
  // assign an id
  if (msg->meta.sender == Meta::kEmpty) {
    CHECK(is_scheduler_);
    CHECK_EQ(ctrl.node.size(), 1);
    if (nodes->control.node.size() < num_nodes) {
      nodes->control.node.push_back(ctrl.node[0]);
    } else {
      // some node dies and restarts
      CHECK(ready_.load());
      for (size_t i = 0; i < nodes->control.node.size() - 1; ++i) {
        const auto& node = nodes->control.node[i];
        if (deadnodes_set->find(node.id) != deadnodes_set->end() &&
            node.role == ctrl.node[0].role) {
          auto& recovery_node = ctrl.node[0];
          // assign previous node id
          recovery_node.id = node.id;
          recovery_node.is_recovery = true;
          PS_VLOG(1) << "replace dead node " << node.DebugString()
                     << " by node " << recovery_node.DebugString();
          nodes->control.node[i] = recovery_node;
          recovery_nodes->control.node.push_back(recovery_node);
          break;
        }
      }
    }
  }

  // update my id
  for (size_t i = 0; i < ctrl.node.size(); ++i) {
    const auto& node = ctrl.node[i];
    if (my_node_.hostname == node.hostname && my_node_.port == node.port) {
      if (getenv("DMLC_RANK") == nullptr || my_node_.id == Meta::kEmpty) {
        my_node_ = node;
        std::string rank = std::to_string(Postoffice::IDtoRank(node.id));
#ifdef _MSC_VER
        _putenv_s("DMLC_RANK", rank.c_str());
#else
        setenv("DMLC_RANK", rank.c_str(), true);
#endif
      }
    }
  }
}

void Van::ProcessHearbeat(Message* msg) {
  auto& ctrl = msg->meta.control;
  time_t t = time(NULL);
  for (auto& node : ctrl.node) {
    Postoffice::Get()->UpdateHeartbeat(node.id, t);
    if (is_scheduler_) {
      Message heartbeat_ack;
      heartbeat_ack.meta.recver = node.id;
      heartbeat_ack.meta.control.cmd = Control::HEARTBEAT;
      heartbeat_ack.meta.control.node.push_back(my_node_);
      heartbeat_ack.meta.timestamp = timestamp_++;
      // send back heartbeat
      Send(heartbeat_ack);
    }
  }
}

void Van::ProcessBarrierCommand(Message* msg) {
  auto& ctrl = msg->meta.control;
  if (msg->meta.request) {
    if (barrier_count_.empty()) {
      barrier_count_.resize(8, 0);
    }
    int group = ctrl.barrier_group;
    ++barrier_count_[group];
    PS_VLOG(1) << "Barrier count for " << group << " : "
               << barrier_count_[group];
    if (barrier_count_[group] ==
        static_cast<int>(Postoffice::Get()->GetNodeIDs(group).size())) {
      barrier_count_[group] = 0;
      Message res;
      res.meta.request = false;
      res.meta.app_id = msg->meta.app_id;
      res.meta.customer_id = msg->meta.customer_id;
      res.meta.control.cmd = Control::BARRIER;
      for (int r : Postoffice::Get()->GetNodeIDs(group)) {
        int recver_id = r;
        if (shared_node_mapping_.find(r) == shared_node_mapping_.end()) {
          res.meta.recver = recver_id;
          res.meta.timestamp = timestamp_++;
          Send(res);
        }
      }
    }
  } else {
    Postoffice::Get()->Manage(*msg);
  }
}

void Van::ProcessDataMsg(Message* msg) {
  // data msg
  CHECK_NE(msg->meta.sender, Meta::kEmpty);
  CHECK_NE(msg->meta.recver, Meta::kEmpty);
  CHECK_NE(msg->meta.app_id, Meta::kEmpty);
  int app_id = msg->meta.app_id;
  int customer_id =
      Postoffice::Get()->is_worker() ? msg->meta.customer_id : app_id;
  auto* obj = Postoffice::Get()->GetCustomer(app_id, customer_id, 5);
  CHECK(obj) << "timeout (5 sec) to wait App " << app_id << " customer "
             << customer_id << " ready at " << my_node_.role;
  obj->Accept(*msg);
}

void Van::ProcessAddNodeCommand(Message* msg, Meta* nodes,
                                Meta* recovery_nodes) {
  auto dead_nodes = Postoffice::Get()->GetDeadNodes(heartbeat_timeout_);
  std::unordered_set<int> dead_set(dead_nodes.begin(), dead_nodes.end());
  auto& ctrl = msg->meta.control;

  UpdateLocalID(msg, &dead_set, nodes, recovery_nodes);

  if (is_scheduler_) {
    ProcessAddNodeCommandAtScheduler(msg, nodes, recovery_nodes);
  } else {
    for (const auto& node : ctrl.node) {
      std::string addr_str = node.hostname + ":" + std::to_string(node.port);
      if (connected_nodes_.find(addr_str) == connected_nodes_.end()) {
        Connect(node);
        connected_nodes_[addr_str] = node.id;
      }
      if (!node.is_recovery && node.role == Node::SERVER) ++num_servers_;
      if (!node.is_recovery && node.role == Node::WORKER) ++num_workers_;
    }
    PS_VLOG(1) << my_node_.ShortDebugString() << " is connected to others";
    ready_ = true;
  }
}

void Van::Start(int customer_id) {
  // get scheduler info
  start_mu_.lock();

  if (init_stage == 0) {
    scheduler_.hostname = std::string(
        CHECK_NOTNULL(Environment::Get()->find("DMLC_PS_ROOT_URI")));
    scheduler_.port =
        atoi(CHECK_NOTNULL(Environment::Get()->find("DMLC_PS_ROOT_PORT")));
    scheduler_.role = Node::SCHEDULER;
    scheduler_.id = kScheduler;
    is_scheduler_ = Postoffice::Get()->is_scheduler();

    // get my node info
    if (is_scheduler_) {
      my_node_ = scheduler_;
      const char *enableTsengineVal = Environment::Get()->find("ENABLE_TSENGINE");
      const char *enableLemethodVal = Environment::Get()->find("ENABLE_LEMETHOD");
      bool enableTsengine = false;
      bool enableLemethod = false;
      if (enableTsengineVal != nullptr) {
        CHECK(CanToInteger(enableTsengineVal)) << "failed to convert ENABLE_TSENGINE to integer.";
        enableTsengine = (bool)atoi(enableTsengineVal);
      }
      if (enableLemethodVal != nullptr) {
        CHECK(CanToInteger(enableLemethodVal)) << "failed to convert ENABLE_LEMETHOD to integer.";
        enableLemethod = (bool)atoi(enableLemethodVal);
      }
      CHECK(!(enableTsengine && enableLemethod)) << "you can not assign ENABLE_LEMETHOD and ENABLE_TSENGINE with 1 at the same time.";
      if (enableTsengine) {
        max_greed_rate= atof(Environment::Get()->find("GREED_RATE"));
        int num_servers = Postoffice::Get()->num_servers();
        int num_workers = Postoffice::Get()->num_workers();
        int num_max = num_servers > num_workers ? num_servers : num_workers;
        int num_node_id = 2 * num_max + 8;
        std::vector<int> temp(num_node_id, -1);
        for (int i = 0; i < num_node_id; i++) {
          A.push_back(temp);
          lifetime.push_back(temp);
        }
        for (int i = 0; i < num_node_id; i++) {
          B.push_back(0);
          B1.push_back(0);
        }
        ask_q.push(8);
      } else if (enableLemethod) {
        const char *greedRateStr = Environment::Get()->find("GREED_RATE");
        CHECK(greedRateStr == nullptr || CanToFloat(greedRateStr)) << "failed to convert GREED_RATE to float";
        if (greedRateStr == nullptr) { greed_rate_ = DEFAULT_GREED_RATE; }
        else { greed_rate_ = atof(greedRateStr); }
        CHECK(greed_rate_ >= 0 && greed_rate_ <= 1) << "GREED_RATE must be in [0, 1]";
        const char *maxThreadNumStr = Environment::Get()->find("MAX_THREAD_NUM");
        CHECK(maxThreadNumStr == nullptr || CanToInteger(maxThreadNumStr)) << "failed to convert MAX_THREAD_NUM to integer";
        if (maxThreadNumStr == nullptr) { max_thread_num_ = DEFAULT_MAX_THREAD_NUM; }
        else { max_thread_num_ = atoi(maxThreadNumStr); }
        CHECK(max_thread_num_ > 0 && max_thread_num_ < std::numeric_limits<int>::max())
          << "MAX_THREAD_NUM must be in (0, "
          << std::numeric_limits<int>::max() << ")";
        threadPool_.set_max_thread_num(max_thread_num_);
        for (int i = 0; i < Postoffice::Get()->num_workers(); i++) {
          unreceived_nodes_ma_.insert(Postoffice::Get()->WorkerRankToID(i));
          unreceived_nodes_md_.insert(Postoffice::Get()->WorkerRankToID(i));
        }
        unreceived_nodes_ma_.insert(Postoffice::Get()->ServerRankToID(0));
        unreceived_nodes_md_.insert(Postoffice::Get()->ServerRankToID(0));
        int maxNodeID = 2 * std::max(Postoffice::Get()->num_servers(), Postoffice::Get()->num_workers()) + 8;
        for (int i = 0; i < maxNodeID; i++) {
          bandwidth_.push_back(std::vector<int>(maxNodeID, 0));
          lifetime_.push_back(std::vector<int>(maxNodeID, -1));
          edge_weight_ma_.push_back(std::vector<int>(maxNodeID, -INF));
          edge_weight_md_.push_back(std::vector<int>(maxNodeID, -INF));
        }
        receiver_ma_.resize(maxNodeID, UNKNOWN);
        receiver_md_.resize(maxNodeID, UNKNOWN);
        sender_ma_.resize(maxNodeID, UNKNOWN);
        sender_md_.resize(maxNodeID, UNKNOWN);
      }
    } else {
      auto role = Postoffice::Get()->is_worker() ? Node::WORKER : Node::SERVER;
      const char* nhost = Environment::Get()->find("DMLC_NODE_HOST");
      std::string ip;
      if (nhost) ip = std::string(nhost);
      if (ip.empty()) {
        const char* itf = Environment::Get()->find("DMLC_INTERFACE");
        std::string interface;
        if (itf) interface = std::string(itf);
        if (interface.size()) {
          GetIP(interface, &ip);
        } else {
          GetAvailableInterfaceAndIP(&interface, &ip);
        }
        CHECK(!interface.empty()) << "failed to get the interface";
      }
      int port = GetAvailablePort();
      const char* pstr = Environment::Get()->find("PORT");
      if (pstr) port = atoi(pstr);
      CHECK(!ip.empty()) << "failed to get ip";
      CHECK(port) << "failed to get a port";
      my_node_.hostname = ip;
      my_node_.role = role;
      my_node_.port = port;
      // cannot determine my id now, the scheduler will assign it later
      // set it explicitly to make re-register within a same process possible
      my_node_.id = Node::kEmpty;
      my_node_.customer_id = customer_id;
    }

    // bind.
    my_node_.port = Bind(my_node_, is_scheduler_ ? 0 : 40);
    PS_VLOG(1) << "Bind to " << my_node_.DebugString();
    CHECK_NE(my_node_.port, -1) << "bind failed";

    // connect to the scheduler
    Connect(scheduler_);

    // for debug use
    if (Environment::Get()->find("PS_DROP_MSG")) {
      drop_rate_ = atoi(Environment::Get()->find("PS_DROP_MSG"));
    }
    // start receiver
    receiver_thread_ =
        std::unique_ptr<std::thread>(new std::thread(&Van::Receiving, this));
    init_stage++;
  }
  start_mu_.unlock();

  if (!is_scheduler_) {
    // let the scheduler know myself
    Message msg;
    Node customer_specific_node = my_node_;
    customer_specific_node.customer_id = customer_id;
    msg.meta.recver = kScheduler;
    msg.meta.control.cmd = Control::ADD_NODE;
    msg.meta.control.node.push_back(customer_specific_node);
    msg.meta.timestamp = timestamp_++;
    Send(msg);
  }

  // wait until ready
  while (!ready_.load()) {
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
  }

  start_mu_.lock();
  if (init_stage == 1) {
    // resender
    if (Environment::Get()->find("PS_RESEND") &&
        atoi(Environment::Get()->find("PS_RESEND")) != 0) {
      int timeout = 1000;
      if (Environment::Get()->find("PS_RESEND_TIMEOUT")) {
        timeout = atoi(Environment::Get()->find("PS_RESEND_TIMEOUT"));
      }
      resender_ = new Resender(timeout, 10, this);
    }

    if (!is_scheduler_) {
      // start heartbeat thread
      heartbeat_thread_ =
          std::unique_ptr<std::thread>(new std::thread(&Van::Heartbeat, this));
    }
    init_stage++;
  }
  start_mu_.unlock();
}

void Van::Stop() {
  // stop threads
  Message exit;
  exit.meta.control.cmd = Control::TERMINATE;
  exit.meta.recver = my_node_.id;
  // only customer 0 would call this method
  exit.meta.customer_id = 0;
  int ret = SendMsg(exit);
  CHECK_NE(ret, -1);
  receiver_thread_->join();
  init_stage = 0;
  if (!is_scheduler_) heartbeat_thread_->join();
  if (resender_) delete resender_;
  ready_ = false;
  connected_nodes_.clear();
  shared_node_mapping_.clear();
  send_bytes_ = 0;
  timestamp_ = 0;
  my_node_.id = Meta::kEmpty;
  barrier_count_.clear();
}

int Van::Send(const Message& msg) {
  int send_bytes = SendMsg(msg);
  CHECK_NE(send_bytes, -1);
  send_bytes_ += send_bytes;
  if (resender_) resender_->AddOutgoing(msg);
  if (Postoffice::Get()->verbose() >= 2) {
    PS_VLOG(2) << msg.DebugString();
  }
  return send_bytes;
}

void Van::Receiving() {
  Meta nodes;
  Meta recovery_nodes;  // store recovery nodes
  recovery_nodes.control.cmd = Control::ADD_NODE;

  while (true) {
    Message msg;
    int recv_bytes = RecvMsg(&msg);
    // For debug, drop received message
    if (ready_.load() && drop_rate_ > 0) {
      unsigned seed = time(NULL) + my_node_.id;
      if (rand_r(&seed) % 100 < drop_rate_) {
        LOG(WARNING) << "Drop message " << msg.DebugString();
        continue;
      }
    }

    CHECK_NE(recv_bytes, -1);
    recv_bytes_ += recv_bytes;
    if (Postoffice::Get()->verbose() >= 2) {
      PS_VLOG(2) << msg.DebugString();
    }
    // duplicated message
    if (resender_ && resender_->AddIncomming(msg)) continue;

    if (!msg.meta.control.empty()) {
      // control msg
      auto& ctrl = msg.meta.control;
      if (ctrl.cmd == Control::TERMINATE) {
        ProcessTerminateCommand();
        break;
      } else if (ctrl.cmd == Control::ADD_NODE) {
        ProcessAddNodeCommand(&msg, &nodes, &recovery_nodes);
      } else if (ctrl.cmd == Control::BARRIER) {
        ProcessBarrierCommand(&msg);
      } else if (ctrl.cmd == Control::HEARTBEAT) {
        ProcessHearbeat(&msg);
      } else if (ctrl.cmd == Control::ASK_LOCAL_AGGREGATION) {
        threadPool_.enqueue(&Van::ProcessAskLocalAggregation, this, msg);
      } else if (ctrl.cmd == Control::ASK_AS_RECEIVER) {
        ProcessAskAsReceiver(&msg);
      } else if (ctrl.cmd == Control::ASK_AS_RECEIVER_REPLY) {
        ProcessAskAsReceiverReply(&msg);
      } else if (ctrl.cmd == Control::ASK_LOCAL_AGGREGATION_REPLY) {
        ProcessAskLocalAggregationReply(&msg);
      } else if (ctrl.cmd == Control::LOCAL_AGGREGATION) {
        ProcessLocalAggregation(&msg);
      } else if (ctrl.cmd == Control::ASK_MODEL_RECEIVER) {
        threadPool_.enqueue(&Van::ProcessAskModelReceiver, this, msg);
      } else if (ctrl.cmd == Control::ASK_MODEL_RECEIVER_REPLY) {
        ProcessAskModelReceiverReply(&msg);
      } else if (ctrl.cmd == Control::MODEL_DISTRIBUTION) {
        ProcessModelDistribution(&msg);
      } else if (ctrl.cmd == Control::MODEL_DISTRIBUTION_REPLY) {
        ProcessModelDistributionReply(&msg);
      } else if (ctrl.cmd == Control::INIT) {
        ProcessInit(&msg);
      } else if (ctrl.cmd == Control::NOTICE_WORKER_ONE_ITERATION_FINISH) {
        ProcessNoticeWorkersOneIterationFinish(&msg);
      } else if (ctrl.cmd == Control::AUTOPULLRPY) {
        ProcessAutopullrpy();
      } else if (ctrl.cmd == Control::ASK) {
        ProcessAskCommand(&msg);
      } else if (ctrl.cmd == Control::ASK1) {
        ProcessAsk1Command(&msg);
      } else if (ctrl.cmd == Control::REPLY) {
        ProcessReplyCommand(&msg);
      } else {
        LOG(WARNING) << "Drop unknown typed message " << msg.DebugString();
      }
    } else {
      ProcessDataMsg(&msg);
    }
  }
}

void Van::Wait_for_finished() {
  std::unique_lock<std::mutex> ver_lk(ver_mu);
  while (!ver_flag) {
    ver_cond.wait(ver_lk);
  }
  ver_flag = false;
  ver_lk.unlock();
}

void Van::ProcessAutopullrpy() {
  std::unique_lock<std::mutex> ver_lk(ver_mu);
  ver_flag = true;
  ver_lk.unlock();
  ver_cond.notify_one();
}

void Van::Ask(int throughput, int last_recv_id, int version) {
  Message msg;
  msg.meta.customer_id = last_recv_id;//last receiver id
  msg.meta.app_id = throughput;
  msg.meta.sender = my_node_.id;
  msg.meta.recver = kScheduler;
  msg.meta.control.cmd = Control::ASK;
  msg.meta.version = version;
  msg.meta.timestamp = timestamp_++;
  Send(msg);
}

void Van::Ask1(int app, int customer, int timestamp){
  Message msg;
  msg.meta.sender = my_node_.id;
  msg.meta.recver = kScheduler;
  msg.meta.control.cmd = Control::ASK1;
  msg.meta.timestamp = timestamp;
  msg.meta.app_id = app;
  msg.meta.customer_id = customer;
  Send(msg);
}

void Van::ProcessAsk1Command(Message* msg){
  Message rpl;
  rpl.meta.sender = my_node_.id;
  rpl.meta.app_id = msg->meta.app_id;
  rpl.meta.customer_id = msg->meta.customer_id;
  rpl.meta.timestamp = msg->meta.timestamp;
  rpl.meta.push = true;
  rpl.meta.request = true;
  std::unique_lock<std::mutex> lk_sch1(sched1);
  ask_q.push(msg->meta.sender);
  if (ask_q.size() > 1) {
    int node_a = ask_q.front();
    ask_q.pop();
    int node_b = ask_q.front();
    ask_q.pop();
    if (node_a == 8 ||node_b == 8) {
      if (node_a == 8) {
        rpl.meta.iters = node_a;
        rpl.meta.recver = node_b;
        B1[node_b] = 1;
        Send(rpl);
      }else{
        rpl.meta.iters = node_b;
        rpl.meta.recver = node_a;
        B1[node_a] = 1;
        Send(rpl);
      }
    } else {
      if ( A[node_a][node_b] > A[node_b][node_a]) {
          rpl.meta.iters = node_b;
          rpl.meta.recver = node_a;
          Send(rpl);
          B1[node_a] = 1;
      } else {
          rpl.meta.iters = node_a;
          rpl.meta.recver = node_b;
          Send(rpl);
          B1[node_b] = 1;
      }
    }
  }

  int count = 0;
  for (auto it : B1) { count += it; }
  if (count == Postoffice::Get()->num_workers()) {
    for (std::size_t i = 0; i < B1.size(); i++) B1[i] = 0;
  }

  lk_sch1.unlock();
}

void Van::ProcessAskCommand(Message* msg) {
  std::unique_lock<std::mutex> lks(sched);
  int req_node_id = msg->meta.sender;
  if (msg->meta.app_id != -1) {//isn't the first ask
    A[req_node_id][msg->meta.customer_id] = msg->meta.app_id;
    lifetime[req_node_id][msg->meta.customer_id] = msg->meta.version;
  }
  //create reply message
  Message rpl;
  rpl.meta.sender = my_node_.id;
  rpl.meta.recver = req_node_id;
  rpl.meta.control.cmd = Control::REPLY;
  rpl.meta.timestamp = timestamp_++;

  int temp = 0;
  for (std::size_t i = 0; i < B.size(); i++) temp += B[i];

  if (temp == Postoffice::Get()->num_workers()) {
    for (std::size_t i = 0; i < B.size(); i++) B[i] = 0;
    iters++;
  }
  if (msg->meta.version <= iters) {
    rpl.meta.customer_id = -1;
  }
  else{//decision making for receiver
    for (std::size_t i = 0;i < lifetime[req_node_id].size(); i++) {
      if (lifetime[req_node_id][i] != -1 && iters-lifetime[req_node_id][i] > 5){
        lifetime[req_node_id][i] = -1;
        A[req_node_id][i] = -1;
      }
    }
    int receiver_id = -1;
    int num_know_node = 0, num_unknow_node = 0;
    for (std::size_t i = 0; i < A[req_node_id].size(); i++) {
      if ((i % 2) && B[i] == 0) {
        if (A[req_node_id][i] != -1) num_know_node++;
        else num_unknow_node++;
      }
    }
    //adjust value
    num_unknow_node -= 4;
    if (Postoffice::Get()->num_servers() > Postoffice::Get()->num_workers()) {
      num_unknow_node -= (Postoffice::Get()->num_servers() - Postoffice::Get()->num_workers());
    }
    //choose dicision mode
    unsigned int seed = time(0);
    srand(seed);
    int rand_number = rand() % 10;
    double greed_rate = double(num_know_node / (num_know_node+num_unknow_node)) < max_greed_rate ?
                        double(num_know_node / (num_know_node+num_unknow_node)) : max_greed_rate;
    if (num_know_node != 0 && rand_number <= greed_rate * 10) { //greedy mode
      int throughput = -1;
      for (std::size_t i = 0; i < A[req_node_id].size(); i++) {
        if (B[i] == 0 && A[req_node_id][i] > throughput) {
          receiver_id = i;
          throughput = A[req_node_id][i];
        }
      }
    }
    else {//random mode
      if (num_unknow_node == 0) { // updated by kaiserqzyue
        int cnt = rand() % (Postoffice::Get()->num_workers() - temp);
        for (size_t i = 9; i < A[req_node_id].size(); i += 2) {
          if (B[i] == 0) {
            if (cnt == 0) {
              receiver_id = i;
              break;
            }
            cnt--;
          }
        }
      } else {
        rand_number = (rand() % num_unknow_node) + 5;
        int counter = 0;
        for (std::size_t i = 0; i < A[req_node_id].size(); i++){
          if (B[i]==0 && (i%2) && (A[req_node_id][i] == -1)) {
            counter++;
            if (counter == rand_number) {
                receiver_id = i;
                break;
            }
          }
        }
      }
    }
    //send reply message
    rpl.meta.customer_id = receiver_id;
  }
  if (rpl.meta.customer_id != -1) B[rpl.meta.customer_id] = 1;
  lks.unlock();
  Send(rpl);
}

void Van::ProcessReplyCommand(Message* reply) {
  std::unique_lock<std::mutex> ask_lk(ask_mu);
  receiver_=reply->meta.customer_id;
  ask_lk.unlock();
  ask_cond.notify_one();
}

int Van::GetReceiver(int throughput, int last_recv_id, int version) {
    Ask(throughput, last_recv_id, version);
    std::unique_lock<std::mutex> ask_lk(ask_mu);
    while (receiver_== -2){ ask_cond.wait(ask_lk); }
    int temp = receiver_;
    receiver_ = -2 ;
    ask_lk.unlock();
    return temp;
}

void Van::PackMetaPB(const Meta& meta, PBMeta* pb) {
  pb->set_head(meta.head);
  if (meta.app_id != Meta::kEmpty) pb->set_app_id(meta.app_id);
  if (meta.timestamp != Meta::kEmpty) pb->set_timestamp(meta.timestamp);
  if (meta.body.size()) pb->set_body(meta.body);
  pb->set_push(meta.push);
  pb->set_request(meta.request);
  pb->set_simple_app(meta.simple_app);
  pb->set_priority(meta.priority);
  pb->set_customer_id(meta.customer_id);
  for (auto d : meta.data_type) pb->add_data_type(d);
  if (!meta.control.empty()) {
    auto ctrl = pb->mutable_control();
    ctrl->set_cmd(meta.control.cmd);
    if (meta.control.cmd == Control::BARRIER) {
      ctrl->set_barrier_group(meta.control.barrier_group);
    } else if (meta.control.cmd == Control::ACK) {
      ctrl->set_msg_sig(meta.control.msg_sig);
    }
    for (const auto& n : meta.control.node) {
      auto p = ctrl->add_node();
      p->set_id(n.id);
      p->set_role(n.role);
      p->set_port(n.port);
      p->set_hostname(n.hostname);
      p->set_is_recovery(n.is_recovery);
      p->set_customer_id(n.customer_id);
    }
  }
  pb->set_data_size(meta.data_size);
}

void Van::PackMeta(const Meta& meta, char** meta_buf, int* buf_size) {
  // convert into protobuf
  PBMeta pb;
  pb.set_head(meta.head);
  if (meta.app_id != Meta::kEmpty) pb.set_app_id(meta.app_id);
  if (meta.timestamp != Meta::kEmpty) pb.set_timestamp(meta.timestamp);
  if (meta.local_aggregation_receiver != Meta::kEmpty) pb.set_local_aggregation_receiver(meta.local_aggregation_receiver);
  if (meta.model_receiver != Meta::kEmpty) pb.set_model_receiver(meta.model_receiver);
  if (meta.last_receiver != Meta::kEmpty) pb.set_last_receiver(meta.last_receiver);
  if (meta.last_bandwidth != Meta::kEmpty) pb.set_last_bandwidth(meta.last_bandwidth);
  if (meta.num_aggregation != Meta::kEmpty) pb.set_num_aggregation(meta.num_aggregation);
  if (meta.version != Meta::kEmpty) pb.set_version(meta.version);
  if (meta.key != Meta::kEmpty) pb.set_key(meta.key);
  if (meta.iters != Meta::kEmpty) pb.set_iters(meta.iters);
  pb.set_ask_as_receiver_status(meta.ask_as_receiver_status);
  if (meta.body.size()) pb.set_body(meta.body);
  pb.set_push(meta.push);
  pb.set_pull(meta.pull);
  pb.set_request(meta.request);
  pb.set_simple_app(meta.simple_app);
  pb.set_priority(meta.priority);
  pb.set_customer_id(meta.customer_id);
  for (auto d : meta.data_type) pb.add_data_type(d);
  if (!meta.control.empty()) {
    auto ctrl = pb.mutable_control();
    ctrl->set_cmd(meta.control.cmd);
    if (meta.control.cmd == Control::BARRIER) {
      ctrl->set_barrier_group(meta.control.barrier_group);
    } else if (meta.control.cmd == Control::ACK) {
      ctrl->set_msg_sig(meta.control.msg_sig);
    }
    for (const auto& n : meta.control.node) {
      auto p = ctrl->add_node();
      p->set_id(n.id);
      p->set_role(n.role);
      p->set_port(n.port);
      p->set_hostname(n.hostname);
      p->set_is_recovery(n.is_recovery);
      p->set_customer_id(n.customer_id);
    }
  }

  // to string
  *buf_size = pb.ByteSize();
  *meta_buf = new char[*buf_size + 1];
  CHECK(pb.SerializeToArray(*meta_buf, *buf_size))
      << "failed to serialize protbuf";
}

void Van::UnpackMeta(const char* meta_buf, int buf_size, Meta* meta) {
  // to protobuf
  PBMeta pb;
  CHECK(pb.ParseFromArray(meta_buf, buf_size))
      << "failed to parse string into protobuf";
  meta->local_aggregation_receiver = pb.has_local_aggregation_receiver() ? pb.local_aggregation_receiver() : Meta::kEmpty;
  meta->model_receiver = pb.has_model_receiver() ? pb.model_receiver() : Meta::kEmpty;
  meta->last_receiver = pb.has_last_receiver() ? pb.last_receiver() : Meta::kEmpty;
  meta->last_bandwidth = pb.has_last_bandwidth() ? pb.last_bandwidth() : Meta::kEmpty;
  meta->num_aggregation = pb.has_num_aggregation() ? pb.num_aggregation() : Meta::kEmpty;
  meta->version = pb.has_version() ? pb.version() : Meta::kEmpty;
  meta->key = pb.has_key() ? pb.key() : Meta::kEmpty;
  meta->iters = pb.has_iters() ? pb.iters() : Meta::kEmpty;
  meta->ask_as_receiver_status = pb.ask_as_receiver_status();
  meta->head = pb.head();
  meta->app_id = pb.has_app_id() ? pb.app_id() : Meta::kEmpty;
  meta->timestamp = pb.has_timestamp() ? pb.timestamp() : Meta::kEmpty;
  meta->request = pb.request();
  meta->push = pb.push();
  meta->pull = pb.pull();
  meta->simple_app = pb.simple_app();
  meta->priority = pb.priority();
  meta->body = pb.body();
  meta->customer_id = pb.customer_id();
  meta->data_type.resize(pb.data_type_size());
  for (int i = 0; i < pb.data_type_size(); ++i) {
    meta->data_type[i] = static_cast<DataType>(pb.data_type(i));
  }
  if (pb.has_control()) {
    const auto& ctrl = pb.control();
    meta->control.cmd = static_cast<Control::Command>(ctrl.cmd());
    meta->control.barrier_group = ctrl.barrier_group();
    meta->control.msg_sig = ctrl.msg_sig();
    for (int i = 0; i < ctrl.node_size(); ++i) {
      const auto& p = ctrl.node(i);
      Node n;
      n.role = static_cast<Node::Role>(p.role());
      n.port = p.port();
      n.hostname = p.hostname();
      n.id = p.has_id() ? p.id() : Node::kEmpty;
      n.is_recovery = p.is_recovery();
      n.customer_id = p.customer_id();
      meta->control.node.push_back(n);
    }
  } else {
    meta->control.cmd = Control::EMPTY;
  }
}

void Van::Heartbeat() {
  const char* val = Environment::Get()->find("PS_HEARTBEAT_INTERVAL");
  const int interval = val ? atoi(val) : kDefaultHeartbeatInterval;
  while (interval > 0 && ready_.load()) {
    std::this_thread::sleep_for(std::chrono::seconds(interval));
    Message msg;
    msg.meta.recver = kScheduler;
    msg.meta.control.cmd = Control::HEARTBEAT;
    msg.meta.control.node.push_back(my_node_);
    msg.meta.timestamp = timestamp_++;
    Send(msg);
  }
}

void Van::ProcessInit(Message *msg) {
  ProcessDataMsg(msg);
}

void Van::AskModelReceiver(int lastBandwidth, int lastReceiver, int version) {
  Message msg;
  msg.meta.last_receiver = lastReceiver;
  msg.meta.last_bandwidth = lastBandwidth;
  msg.meta.sender = my_node_.id;
  msg.meta.recver = kScheduler;
  msg.meta.control.cmd = Control::ASK_MODEL_RECEIVER;
  msg.meta.version = version;
  Send(msg);
}

void Van::CheckModelDistributionFinish() {
  if (num_md_ != Postoffice::Get()->num_workers()) { return; }
  auto &unreceived_nodes_ = unreceived_nodes_md_;
  num_md_ = 0;
  iteration_++;
  for (int i = 0; i < Postoffice::Get()->num_workers(); i++) {
    unreceived_nodes_.insert(Postoffice::Get()->WorkerRankToID(i));
  }
  unreceived_nodes_.insert(Postoffice::Get()->ServerRankToID(0));
  CheckExpiration();
}

// this will be excuted in another thread so the parameter should copy from the origin
void Van::ProcessAskModelReceiver(Message msg) {
  auto &unreceived_nodes_ = unreceived_nodes_md_;
  auto &left_nodes_ = left_nodes_md_;
  auto &right_nodes_ = right_nodes_md_;
  auto &edge_weight_ = edge_weight_md_;
  auto &receiver_ = receiver_md_;
  auto &sender_ = sender_md_;
  auto &mu_ = mu_md_;
  auto &mutex_on_km_ = mutex_on_km_md_;
  Message rpl;
  rpl.meta.sender = my_node_.id;
  rpl.meta.control.cmd = Control::ASK_MODEL_RECEIVER_REPLY;
  rpl.meta.recver = msg.meta.sender;
  int requestor = msg.meta.sender;
  Postoffice* postoffice = Postoffice::Get();
  if (msg.meta.last_receiver != -1) {
    std::lock_guard<std::mutex> locker{mu_on_bw_lt_};
    bandwidth_[requestor][msg.meta.last_receiver] = msg.meta.last_bandwidth;
    lifetime_[requestor][msg.meta.last_receiver] = msg.meta.version;
  }
  {
    std::unique_lock<std::mutex> locker1{mu_};
    if (msg.meta.version <= iteration_) {
      locker1.unlock();
      rpl.meta.model_receiver = -1;
      {
        std::lock_guard<std::mutex> locker2{mutex_on_km_};
        receiver_[requestor] = UNKNOWN;
      }
      Send(rpl);
      return;
    }
    unreceived_nodes_.erase(requestor);
    if (unreceived_nodes_.empty()) {
      locker1.unlock();
      std::unique_lock<std::mutex> locker2{mutex_on_km_};
      rpl.meta.model_receiver = receiver_[requestor] < 0 ? -1 : receiver_[requestor];
      receiver_[requestor] = UNKNOWN;
      locker2.unlock();
      if (rpl.meta.model_receiver != -1) {
        locker1.lock();
        num_md_++;
        CheckModelDistributionFinish();
        locker1.unlock();
      }
      Send(rpl);
      return;
    }
  }
  {
    std::unique_lock<std::mutex> locker{mutex_on_km_};
    if (receiver_[requestor] != UNKNOWN) {
      rpl.meta.model_receiver = receiver_[requestor];
      receiver_[requestor] = UNKNOWN;
      locker.unlock();
      if (rpl.meta.model_receiver != -1) {
        std::lock_guard<std::mutex> locker2{mu_};
        num_md_++;
        CheckModelDistributionFinish();
      }
      Send(rpl);
      return;
    }
  }
  std::unique_lock<std::mutex> locker1{mu_, std::defer_lock};
  std::unique_lock<std::mutex> locker2{mutex_on_km_, std::defer_lock};
  std::lock(locker1, locker2);
  left_nodes_.clear(); right_nodes_.clear();
  for (int leftNode : unreceived_nodes_) { left_nodes_.insert(leftNode); }
  int workerID = 0;
  for (int i = 0; i < postoffice->num_workers(); i++) {
    workerID = postoffice->WorkerRankToID(i);
    if (unreceived_nodes_.count(workerID) == 0 && receiver_[workerID] == UNKNOWN) {
      right_nodes_.insert(workerID);
    }
  }
  int psID = postoffice->ServerRankToID(0);
  if (unreceived_nodes_.count(psID) == 0 && receiver_[psID] == UNKNOWN) {
    right_nodes_.insert(psID);
  }
  locker1.unlock();
  if (left_nodes_.size() > right_nodes_.size()) {
    GetEdgeWeight(true, left_nodes_, right_nodes_, edge_weight_);
    KM(right_nodes_, left_nodes_, edge_weight_, sender_);
    for (int leftNode : left_nodes_) {
      if (sender_[leftNode] != -1) { receiver_[sender_[leftNode]] = leftNode; }
    }
  } else {
    GetEdgeWeight(false, left_nodes_, right_nodes_, edge_weight_);
    KM(left_nodes_, right_nodes_, edge_weight_, receiver_);
  }
  locker1.lock();
  for (int rightNode : right_nodes_) {
    if (receiver_[rightNode] != -1) {
      unreceived_nodes_.erase(receiver_[rightNode]);
    }
  }
  srand(time(0));
  for (int rightNode : right_nodes_) {
    // 10000 means that the minimum precsion for greed_rate_ is 0.0001
    int randNumber = rand() % 10000;
    if (lifetime_[rightNode][receiver_[rightNode]] != -1 && randNumber > greed_rate_ * 10000 && unreceived_nodes_.size() > 0){
      int newReceiver = RandomGetReceiver(rightNode);
      unreceived_nodes_.insert(receiver_[rightNode]);
      receiver_[rightNode] = newReceiver;
      unreceived_nodes_.erase(receiver_[rightNode]);
    }
  }
  rpl.meta.model_receiver = receiver_[requestor];
  receiver_[requestor] = UNKNOWN;
  locker2.unlock();
  if (rpl.meta.model_receiver != -1) {
    num_md_++;
    CheckModelDistributionFinish();
    locker1.unlock();
  }
  Send(rpl);
}

void Van::AskLocalAggregation() {
  Message msg;
  msg.meta.sender = my_node_.id;
  msg.meta.recver = kScheduler;
  msg.meta.control.cmd = Control::ASK_LOCAL_AGGREGATION;
  Send(msg);
}

void Van::CheckModelAggregationFinish() {
  num_ma_++;
  if (num_ma_ != Postoffice::Get()->num_workers()) { return; }
  num_ma_ = 0;
  auto &unreceived_nodes_ = unreceived_nodes_ma_;
  auto &receiver_ = receiver_ma_;
  for (int i = 0; i < Postoffice::Get()->num_workers(); i++) {
    unreceived_nodes_.insert(Postoffice::Get()->WorkerRankToID(i));
    receiver_[Postoffice::Get()->WorkerRankToID(i)] = UNKNOWN;
  }
}

// this will be excuted in another thread so the parameter should copy from the origin
void Van::ProcessAskLocalAggregation(Message msg) {
  auto &unreceived_nodes_ = unreceived_nodes_ma_;
  auto &left_nodes_ = left_nodes_ma_;
  auto &right_nodes_ = right_nodes_ma_;
  auto &edge_weight_ = edge_weight_ma_;
  auto &receiver_ = receiver_ma_;
  auto &sender_ = sender_ma_;
  auto &mu_ = mu_ma_;
  auto &mutex_on_km_ = mutex_on_km_ma_;
  Message rpl, req;
  req.meta.sender = my_node_.id;
  rpl.meta.sender = my_node_.id;
  rpl.meta.recver = msg.meta.sender;
  rpl.meta.control.cmd = Control::ASK_LOCAL_AGGREGATION_REPLY;
  int requestor = msg.meta.sender;
  Postoffice *postoffice = Postoffice::Get();
  {
    std::unique_lock<std::mutex> locker{mu_};
    unreceived_nodes_.erase(requestor);
    if (unreceived_nodes_.size() == 1) {
      locker.unlock();
      std::unique_lock<std::mutex> locker1{mu_, std::defer_lock};
      std::unique_lock<std::mutex> locker2{mutex_on_km_, std::defer_lock};
      std::lock(locker1, locker2);
      CheckModelAggregationFinish();
      rpl.meta.local_aggregation_receiver = postoffice->ServerRankToID(0);
      Send(rpl);
      return;
    }
  }
  {
    std::unique_lock<std::mutex> locker1{mutex_on_km_};
    if (receiver_[requestor] != UNKNOWN) {
      if (receiver_[requestor] != postoffice->ServerRankToID(0)){
        req.meta.recver = receiver_[requestor];
        req.meta.control.cmd = Control::ASK_AS_RECEIVER;
        Send(req);
        bool ok = WaitForAskAsReceiverReply(req.meta.recver);
        if (ok) {
          rpl.meta.local_aggregation_receiver = receiver_[requestor];
          locker1.unlock();
          {
            std::unique_lock<std::mutex> locker1{mu_, std::defer_lock};
            std::unique_lock<std::mutex> locker2{mutex_on_km_, std::defer_lock};
            std::lock(locker1, locker2);
            CheckModelAggregationFinish();
          }
          Send(rpl);
          return;
        } else {
          receiver_[requestor] = UNKNOWN;
        }
      } else {
        rpl.meta.local_aggregation_receiver = receiver_[requestor];
        locker1.unlock();
        {
          std::unique_lock<std::mutex> locker1{mu_, std::defer_lock};
          std::unique_lock<std::mutex> locker2{mutex_on_km_, std::defer_lock};
          std::lock(locker1, locker2);
          CheckModelAggregationFinish();
        }
        Send(rpl);
        return;
      }
    }
  }
  std::unique_lock<std::mutex> locker1{mu_, std::defer_lock};
  std::unique_lock<std::mutex> locker2{mutex_on_km_, std::defer_lock};
  std::lock(locker1, locker2);
  left_nodes_.clear(); right_nodes_.clear();
  for (int leftNode : unreceived_nodes_) { left_nodes_.insert(leftNode); }
  int workerID = 0;
  for (int i = 0; i < Postoffice::Get()->num_workers(); i++) {
    workerID = Postoffice::Get()->WorkerRankToID(i);
    if (unreceived_nodes_.count(workerID) == 0 && receiver_[workerID] == UNKNOWN) {
      right_nodes_.insert(workerID);
    }
  }
  locker1.unlock();
  if (left_nodes_.size() >= right_nodes_.size()) {
    GetEdgeWeight(true, left_nodes_, right_nodes_, edge_weight_);
    KM(right_nodes_, left_nodes_, edge_weight_, sender_);
    for (int leftNode : left_nodes_) {
      if (sender_[leftNode] != -1) { receiver_[sender_[leftNode]] = leftNode; }
    }
  } else {
    GetEdgeWeight(false, left_nodes_, right_nodes_, edge_weight_);
    KM(left_nodes_, right_nodes_, edge_weight_, receiver_);
    int maxScore = -1, toLeftNode = -1, score = -INF;
    {
      std::lock_guard<std::mutex> locker3{mu_on_bw_lt_}; // ProcessAskModelReceiver may be running, so there is need to lock.
      while(left_nodes_.size() < right_nodes_.size()) {
        maxScore = -1; toLeftNode = -1; score = -INF;
        for (int rightNode : right_nodes_) {
          score = -INF;
          for (int anotherRightNode : right_nodes_) {
            if (anotherRightNode == rightNode) { continue; }
            if (lifetime_[anotherRightNode][rightNode] == -1) {
              score = std::max(score, 0);
              continue;
            }
            score = std::max(score, bandwidth_[anotherRightNode][rightNode]);
          }
          if (score > maxScore) {
            maxScore = score;
            toLeftNode = rightNode;
          }
        }
        left_nodes_.insert(toLeftNode);
        right_nodes_.erase(toLeftNode);
        receiver_[toLeftNode] = UNKNOWN;
      }
    }
    GetEdgeWeight(true, left_nodes_, right_nodes_, edge_weight_);
    KM(right_nodes_, left_nodes_, edge_weight_, sender_);
    for (int leftNode : left_nodes_) {
      if (sender_[leftNode] != -1) { receiver_[sender_[leftNode]] = leftNode; }
    }
  }
  // this if means that requestor is move to the left_nodes_, we just let it send to PS.
  if (receiver_[requestor] == UNKNOWN) { receiver_[requestor] = postoffice->ServerRankToID(0); }
  else if (receiver_[requestor] != postoffice->ServerRankToID(0)){
    req.meta.recver = receiver_[requestor];
    req.meta.control.cmd = Control::ASK_AS_RECEIVER;
    Send(req);
    bool ok = WaitForAskAsReceiverReply(req.meta.recver);
    if (!ok) { receiver_[requestor] = postoffice->ServerRankToID(0); }
  }
  rpl.meta.local_aggregation_receiver = receiver_[requestor];
  locker1.lock();
  CheckModelAggregationFinish();
  locker1.unlock();
  locker2.unlock();
  Send(rpl);
}

void Van::ProcessAskLocalAggregationReply(Message *msg) {
  std::lock_guard<std::mutex> locker{cv_mu_};
  local_aggregation_receiver_ = msg->meta.local_aggregation_receiver;
  cv_.notify_all();
}

bool Van::IsVirtualNode(int id) {
  Postoffice* postoffice = Postoffice::Get();
  return id >= postoffice->WorkerRankToID(postoffice->num_workers());
}

void Van::GetEdgeWeight(bool reverse, std::unordered_set<int>& left_nodes_, std::unordered_set<int>& right_nodes_, std::vector<std::vector<int>>& edge_weight_) {
  std::lock_guard<std::mutex> locker{mu_on_bw_lt_};
  if (reverse) {
    for (int leftNode : left_nodes_) {
      for (int rightNode : right_nodes_) {
        if (lifetime_[rightNode][leftNode] == -1) {
          edge_weight_[rightNode][leftNode] = -INF;
        } else {
          edge_weight_[rightNode][leftNode] = bandwidth_[rightNode][leftNode];
        }
      }
    }
  } else {
    for (int leftNode : left_nodes_) {
      for (int rightNode : right_nodes_) {
        if (lifetime_[rightNode][leftNode] == -1) {
          edge_weight_[leftNode][rightNode] = -INF;
        } else {
          edge_weight_[leftNode][rightNode] = bandwidth_[rightNode][leftNode];
        }
      }
    }
  }
}

void Van::AddVirtualNodes(std::unordered_set<int> &leftNodes, std::unordered_set<int> &rightNodes) {
  int virtualNodeID = Postoffice::Get()->WorkerRankToID(Postoffice::Get()->num_workers());
  while (leftNodes.size() > rightNodes.size()) {
    rightNodes.insert(virtualNodeID++);
  }
  while (leftNodes.size() < rightNodes.size()) {
    leftNodes.insert(virtualNodeID++);
  }
}

void Van::CheckExpiration() {
  for (int requestorID = 8; requestorID < lifetime_.size(); requestorID++) {
    for (auto &lifetime : lifetime_[requestorID]){
      if(lifetime != -1 && iteration_ - lifetime > BANDWIDTH_EXPIRATION_TIME){
          lifetime = -1;
      }
    }
  }
}

int Van::RandomGetReceiver(int rightNode) {
  auto &unreceived_nodes_ = unreceived_nodes_md_;
  auto &receiver_ = receiver_md_;
  int numUnknownBandwidth = 0;
  for (std::size_t i = 9; i < lifetime_[rightNode].size(); i += 2) {
    if (lifetime_[rightNode][i] == -1 && unreceived_nodes_.count(i) == 1) {
      numUnknownBandwidth++;
    }
  }
  int receiver = receiver_[rightNode];
  int randNumber = 0;
  if (numUnknownBandwidth == 0) {
    randNumber = rand() % unreceived_nodes_.size();
    for (std::size_t i = 9; i < lifetime_[rightNode].size(); i += 2) {
      if (unreceived_nodes_.count(i) == 1) {
        if (randNumber == 0) {
          receiver = i;
          break;
        }
        randNumber--;
      }
    }
  } else {
    randNumber = rand() % numUnknownBandwidth;
    for (std::size_t i = 9; i < lifetime_[rightNode].size(); i += 2) {
      if (lifetime_[rightNode][i] == -1 && unreceived_nodes_.count(i) == 1) {
        if (randNumber == 0) {
          receiver = i;
          break;
        }
        randNumber--;
      }
    }
  }
  return receiver;
}

void Van::KMBfs(std::unordered_set<int> &leftNodes, std::unordered_set<int> &rightNodes,
                std::vector<std::vector<int>> &edgeWeight, std::vector<int> &match,
                std::vector<int> &leftWeight, std::vector<int> &rightWeight,
                int startNode, int maxID) {
  int nextNode = -1, leftNode = -1, delta = INF, u = 0;
  match[u] = startNode;
  std::vector<bool> augmented(maxID, false);
  std::vector<int> parent(maxID, 0), slack(maxID, std::numeric_limits<int>::max());
  do {
    leftNode = match[u]; augmented[u] = true; delta = std::numeric_limits<int>::max();
    for (int rightNode : rightNodes) {
      if (augmented[rightNode]) { continue; }
      int temp = leftWeight[leftNode] + rightWeight[rightNode] - edgeWeight[leftNode][rightNode];
      if (slack[rightNode] > temp) {
          slack[rightNode] = temp;
          parent[rightNode] = u;
      }
      if (slack[rightNode] < delta) {
          delta = slack[rightNode];
          nextNode = rightNode;
      }
    }
    leftWeight[match[0]] -= delta;
    rightWeight[0] += delta;
    for (int rightNode : rightNodes) {
      if (augmented[rightNode]) {
          leftWeight[match[rightNode]] -= delta;
          rightWeight[rightNode] += delta;
      } else { slack[rightNode] -= delta; }
    }
    u = nextNode;
  } while (match[u] != -1);
  while (u != 0) { match[u] = match[parent[u]]; u = parent[u]; }
}

void Van::KM(std::unordered_set<int> &leftNodes, std::unordered_set<int> &rightNodes,
             std::vector<std::vector<int>> &edgeWeight, std::vector<int> &match) { // TODO this need to be checked.
  int maxID = -1;
  for (int id : leftNodes) { maxID = std::max(maxID, id); }
  for (int id : rightNodes) { maxID = std::max(maxID, id); }
  maxID++;
  std::vector<int> leftWeight(maxID, 0), rightWeight(maxID, 0);
  for (int rightNode : rightNodes) { match[rightNode] = -1; }
  for (int leftNode : leftNodes) {
    KMBfs(leftNodes, rightNodes, edgeWeight, match, leftWeight, rightWeight, leftNode, maxID);
  }
}

bool Van::CanToInteger(const char *str) {
  if (str == nullptr) { return false; }
  size_t len = strlen(str);
  for (int i = 0; i < len; i++) {
    if (isdigit(str[i]) || (str[i] == '-' && i == 0)) { continue; }
    else { return false; }
  }
  return true;
}

bool Van::CanToFloat(const char *str) {
  if (str == nullptr) { return false; }
  size_t len = strlen(str);
  bool hasPoint = false;
  int numberPart = 0;
  for (size_t i = 0; i < len; i++) {
    if (str[i] == '-') {
      if (i != 0) { return false; }
    } else if (isdigit(str[i])) {
      if ((!hasPoint && numberPart == 0) || (hasPoint && numberPart == 1)) { numberPart++; }
    } else if (str[i] == '.') {
      if (hasPoint) { return false; }
      hasPoint = true;
    } else {
      return false;
    }
  }
  return (hasPoint && numberPart == 2) || (!hasPoint && numberPart == 1);
}

bool Van::WaitForAskAsReceiverReply(int nodeID) {
  std::unique_lock<std::mutex> locker{cv_mu_};
  cv_.wait(locker, [this, nodeID]() {return reply_node_id_ == nodeID; });
  reply_node_id_ = -1;
  bool status = ask_as_receiver_status_;
  cv_.notify_all();
  return status;
}

void Van::WaitForModelDistributionReply() {
  std::unique_lock<std::mutex> locker{cv_mu_};
  cv_.wait(locker, [this]() { return receive_model_distribution_reply_; });
  receive_model_distribution_reply_ = false;
}

int Van::GetModelReceiver(int lastBandwidth, int lastReceiver, int iteration) {
  AskModelReceiver(lastBandwidth, lastReceiver, iteration);
  std::unique_lock<std::mutex> locker{cv_mu_};
  cv_.wait(locker, [this]() { return model_receiver_ != -2; });
  int res = model_receiver_;
  model_receiver_ = -2;
  return res;
}

int Van::GetLocalAggregationReceiver() {
  AskLocalAggregation();
  std::unique_lock<std::mutex> locker{cv_mu_};
  cv_.wait(locker, [this]() { return local_aggregation_receiver_ != -1; });
  int res = local_aggregation_receiver_;
  local_aggregation_receiver_ = -1;
  return res;
}

void Van::NoticeWorkersOneIterationFinish() {
  Message msg;
  msg.meta.sender = my_node_.id;
  msg.meta.control.cmd = Control::NOTICE_WORKER_ONE_ITERATION_FINISH;
  for (int receiver : Postoffice::Get()->GetNodeIDs(kWorkerGroup)) {
    msg.meta.recver = receiver;
    Send(msg);
  }
  Postoffice::Get()->Barrier(0, kWorkerGroup + kServerGroup);
}

void Van::ProcessNoticeWorkersOneIterationFinish(Message *msg) {
  {
    std::lock_guard<std::mutex> locker{cv_mu_};
    can_be_receiver_ = true;
  }
  Message req;
  req.meta.recver = kScheduler;
  req.meta.request = true;
  req.meta.control.cmd = Control::BARRIER;
  req.meta.app_id = 0;
  req.meta.customer_id = 0;
  req.meta.control.barrier_group = kWorkerGroup + kServerGroup;
  req.meta.timestamp = GetTimestamp();
  Send(req);
}

void Van::WaitForLocalAggregationFinish() {
  std::unique_lock<std::mutex> locker{cv_mu_};
  can_be_receiver_ = false;
  cv_.wait(locker, [this]() { return num_as_receiver_ == 0; });
}

void Van::ProcessAskAsReceiver(Message *msg) {
  Message rpl;
  rpl.meta.recver = kScheduler;
  rpl.meta.control.cmd = Control::ASK_AS_RECEIVER_REPLY;
  {
    std::lock_guard<std::mutex> locker{cv_mu_};
    rpl.meta.ask_as_receiver_status = can_be_receiver_;
    if (can_be_receiver_) { num_as_receiver_++; }
  }
  Send(rpl);
}

void Van::ProcessAskAsReceiverReply(Message *msg) {
  std::unique_lock<std::mutex> locker{cv_mu_};
  cv_.wait(locker, [this]() { return reply_node_id_ == -1; });
  reply_node_id_ = msg->meta.sender;
  ask_as_receiver_status_ = msg->meta.ask_as_receiver_status;
  cv_.notify_all();
}

bool Van::IsServerNode() {
  return my_node_.id >= 8 && my_node_.id % 2 == 0;
}

void Van::DecreaseNumAsReceiver() {
  if (IsServerNode()) { return; }
  std::lock_guard<std::mutex> locker{cv_mu_};
  num_as_receiver_--;
  if (num_as_receiver_ == 0) { cv_.notify_all(); }
}

void Van::ProcessLocalAggregation(Message *msg) {
  ProcessDataMsg(msg);
}

void Van::ProcessAskModelReceiverReply(Message *msg) {
  std::lock_guard<std::mutex> locker{cv_mu_};
  model_receiver_ = msg->meta.model_receiver;
  cv_.notify_all();
}

void Van::ProcessModelDistribution(Message *msg) {
  Message rpl;
  rpl.meta.recver = msg->meta.sender;
  rpl.meta.control.cmd = Control::MODEL_DISTRIBUTION_REPLY;
  Send(rpl);
  ProcessDataMsg(msg);
}

void Van::ProcessModelDistributionReply(Message *msg) {
  std::lock_guard<std::mutex> locker{cv_mu_};
  receive_model_distribution_reply_ = true;
  cv_.notify_all();
}

}  // namespace ps
