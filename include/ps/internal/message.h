/**
 *  Copyright (c) 2015 by Contributors
 */
#ifndef PS_INTERNAL_MESSAGE_H_
#define PS_INTERNAL_MESSAGE_H_
#include <vector>
#include <string>
#include <sstream>
#include "ps/sarray.h"
namespace ps {
/** \brief data type */
enum DataType {
  CHAR, INT8, INT16, INT32, INT64,
  UINT8, UINT16, UINT32, UINT64,
  FLOAT, DOUBLE, OTHER
};
/** \brief data type name */
static const char* DataTypeName[] = {
  "CHAR", "INT8", "INT16", "INT32", "INT64",
  "UINT8", "UINT16", "UINT32", "UINT64",
  "FLOAT", "DOUBLE", "OTHER"
};
/**
 * \brief compare if V and W are the same type
 */
template<typename V, typename W>
inline bool SameType() {
  return std::is_same<typename std::remove_cv<V>::type, W>::value;
}
/**
 * \brief return the DataType of V
 */
template<typename V>
DataType GetDataType() {
  if (SameType<V, int8_t>()) {
    return INT8;
  } else if (SameType<V, int16_t>()) {
    return INT16;
  } else if (SameType<V, int32_t>()) {
    return INT32;
  } else if (SameType<V, int64_t>()) {
    return INT64;
  } else if (SameType<V, uint8_t>()) {
    return UINT8;
  } else if (SameType<V, uint16_t>()) {
    return UINT16;
  } else if (SameType<V, uint32_t>()) {
    return UINT32;
  } else if (SameType<V, uint64_t>()) {
    return UINT64;
  } else if (SameType<V, float>()) {
    return FLOAT;
  } else if (SameType<V, double>()) {
    return DOUBLE;
  } else {
    return OTHER;
  }
}
/**
 * \brief information about a node
 */
struct Node {
  /** \brief the empty value */
  static const int kEmpty;
  /** \brief default constructor */
  Node() : id(kEmpty), port(kEmpty), is_recovery(false) {}
  /** \brief node roles */
  enum Role { SERVER, WORKER, SCHEDULER };
  /** \brief get debug string */
  std::string DebugString() const {
    std::stringstream ss;
    ss << "role=" << (role == SERVER ? "server" : (role == WORKER ? "worker" : "scheduler"))
       << (id != kEmpty ? ", id=" + std::to_string(id) : "")
       << ", ip=" << hostname << ", port=" << port << ", is_recovery=" << is_recovery;

    return ss.str();
  }
  /** \brief get short debug string */
  std::string ShortDebugString() const {
    std::string str = role == SERVER ? "S" : (role == WORKER ? "W" : "H");
    if (id != kEmpty) str += "[" + std::to_string(id) + "]";
    return str;
  }
  /** \brief the role of this node */
  Role role;
  /** \brief node id */
  int id;
  /** \brief customer id */
  int customer_id;
  /** \brief hostname or ip */
  std::string hostname;
  /** \brief the port this node is binding */
  int port;
  /** \brief whether this node is created by failover */
  bool is_recovery;
};
/**
 * \brief meta info of a system control message
 */
struct Control {
  /** \brief empty constructor */
  Control() : cmd(EMPTY) { }
  /** \brief return true is empty */
  inline bool empty() const { return cmd == EMPTY; }
  /** \brief get debug string */
  std::string DebugString() const {
    if (empty()) return "";
    std::vector<std::string> cmds = {
      "EMPTY", "TERMINATE", "ADD_NODE", "BARRIER", "ACK", "HEARTBEAT",
      "ASK_LOCAL_AGGREGATION",
      "ASK_MODEL_RECEIVER",
      "ASK_MODEL_RECEIVER_REPLY",
      "ASK_LOCAL_AGGREGATION_REPLY",
      "MODEL_DISTRIBUTION_REPLY",
      "LOCAL_AGGREGATION",
      "ASK_AS_RECEIVER",
      "MODEL_DISTRIBUTION",
      "ASK_AS_RECEIVER_REPLY",
      "FINISH_RECEIVING_LOCAL_AGGREGATION",
      "NOTIFY_WORKER_ONE_ITERATION_FINISH",
      "INIT",
      "AUTOPULLRPY",
      "ASK",
      "REPLY",
      "ASK1"
    };
    std::stringstream ss;
    ss << "cmd=" << cmds[cmd];
    if (node.size()) {
      ss << ", node={";
      for (const Node& n : node) ss << " " << n.DebugString();
      ss << " }";
    }
    if (cmd == BARRIER) ss << ", barrier_group=" << barrier_group;
    if (cmd == ACK) ss << ", msg_sig=" << msg_sig;
    return ss.str();
  }
  /** \brief all commands */
  enum Command {
    EMPTY, TERMINATE, ADD_NODE, BARRIER, ACK, HEARTBEAT,
    ASK_LOCAL_AGGREGATION,
    ASK_MODEL_RECEIVER,
    ASK_MODEL_RECEIVER_REPLY,
    ASK_LOCAL_AGGREGATION_REPLY,
    MODEL_DISTRIBUTION_REPLY,
    LOCAL_AGGREGATION,
    ASK_AS_RECEIVER,
    MODEL_DISTRIBUTION,
    ASK_AS_RECEIVER_REPLY,
    FINISH_RECEIVING_LOCAL_AGGREGATION,
    NOTIFY_WORKER_ONE_ITERATION_FINISH,
    INIT,
    AUTOPULLRPY,
    ASK,
    REPLY,
    ASK1
  };
  /** \brief the command */
  Command cmd;
  /** \brief node infos */
  std::vector<Node> node;
  /** \brief the node group for a barrier, such as kWorkerGroup */
  int barrier_group;
  /** message signature */
  uint64_t msg_sig;
};
/**
 * \brief meta info of a message
 */
struct Meta {
  /** \brief the empty value */
  static const int kEmpty;
  /** \brief default constructor */
  Meta() : head(kEmpty), app_id(kEmpty), customer_id(kEmpty),
           timestamp(kEmpty), sender(kEmpty), recver(kEmpty),
           local_aggregation_receiver(kEmpty), model_receiver(kEmpty), last_receiver(kEmpty),
           last_bandwidth(kEmpty), num_aggregation(kEmpty), version(kEmpty), key(kEmpty), iters(kEmpty) {}
  std::string DebugString() const {
    std::stringstream ss;
    if (sender == Node::kEmpty) {
      ss << "?";
    } else {
      ss << sender;
    }
    ss <<  " => " << recver;
    ss << ". Meta: request=" << request;
    if (timestamp != kEmpty) ss << ", timestamp=" << timestamp;
    if (!control.empty()) {
      ss << ", control={ " << control.DebugString() << " }";
    } else {
      ss << ", app_id=" << app_id
         << ", customer_id=" << customer_id
         << ", simple_app=" << simple_app
         << ", push=" << push;
    }
    if (head != kEmpty) ss << ", head=" << head;
    if (body.size()) ss << ", body=" << body;
    if (data_type.size()) {
      ss << ", data_type={";
      for (auto d : data_type) ss << " " << DataTypeName[static_cast<int>(d)];
      ss << " }";
    }
    return ss.str();
  }
  /** \brief an int head */
  int head;
  /** \brief the unique id of the application of messsage is for*/
  int app_id;
  /** \brief customer id*/
  int customer_id;
  /** \brief the timestamp of this message */
  int timestamp;
  /** \brief the node id of the sender of this message */
  int sender;
  /** \brief the node id of the receiver of this message */
  int recver;
  /** \brief whether or not this is a request message*/
  bool request{};
  /** \brief whether or not a push message */
  bool push{};
  /** \brief whether or not a pull message */
  bool pull{};
  /** \brief whether or not it's for SimpleApp */
  bool simple_app{};
  /** \brief an string body */
  std::string body;
  /** \brief data type of message.data[i] */
  std::vector<DataType> data_type;
  /** \brief system control message */
  Control control;
  /** \brief the byte size */
  int data_size = 0;
  /** \brief message priority */
  int priority = 0;
  // When the cmd is ASK_LOCAL_AGGREGATION_REPLY,
  // this is the receiver of local aggregation.
  int local_aggregation_receiver;
  // The receiver of model distribution.
  int model_receiver;
  // The last receiver of model.
  int last_receiver;
  // The last detected bandwidth.
  int last_bandwidth;
  // How many workers's local model has been aggregated in this message.
  int num_aggregation;
  // When the cmd is ASK_AS_RECEIVER_REPLY,
  // this is the status of whether this node can be a receiver.
  bool ask_as_receiver_status{};
  int version;
  int key;
  int iters;
};
/**
 * \brief messages that communicated amaong nodes.
 */
struct Message {
  /** \brief the meta info of this message */
  Meta meta;
  /** \brief the large chunk of data of this message */
  std::vector<SArray<char> > data;
  /**
   * \brief push array into data, and add the data type
   */
  template <typename V>
  void AddData(const SArray<V>& val) {
    CHECK_EQ(data.size(), meta.data_type.size());
    meta.data_type.push_back(GetDataType<V>());
    SArray<char> bytes(val);
    meta.data_size += bytes.size();
    data.push_back(bytes);
  }
  std::string DebugString() const {
    std::stringstream ss;
    ss << meta.DebugString();
    if (data.size()) {
      ss << " Body:";
      for (const auto& d : data) ss << " data_size=" << d.size();
    }
    return ss.str();
  }
};
}  // namespace ps
#endif  // PS_INTERNAL_MESSAGE_H_
