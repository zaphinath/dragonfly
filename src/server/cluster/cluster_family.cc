// Copyright 2023, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/cluster/cluster_family.h"

#include <jsoncons/json.hpp>
#include <memory>
#include <mutex>
#include <string>

#include "base/flags.h"
#include "base/logging.h"
#include "core/json_object.h"
#include "facade/cmd_arg_parser.h"
#include "facade/dragonfly_connection.h"
#include "facade/error.h"
#include "server/acl/acl_commands_def.h"
#include "server/command_registry.h"
#include "server/conn_context.h"
#include "server/dflycmd.h"
#include "server/error.h"
#include "server/journal/journal.h"
#include "server/main_service.h"
#include "server/server_family.h"
#include "server/server_state.h"

ABSL_FLAG(std::string, cluster_announce_ip, "", "ip that cluster commands announce to the client");

ABSL_DECLARE_FLAG(int32_t, port);

namespace dfly {
namespace {

using namespace std;
using namespace facade;
using CI = CommandId;
using ClusterShard = ClusterConfig::ClusterShard;
using ClusterShards = ClusterConfig::ClusterShards;
using Node = ClusterConfig::Node;
using SlotRange = ClusterConfig::SlotRange;

constexpr char kIdNotFound[] = "syncid not found";

constexpr string_view kClusterDisabled =
    "Cluster is disabled. Enabled via passing --cluster_mode=emulated|yes";
constexpr string_view kDflyClusterCmdPort = "DflyCluster command allowed only under admin port";

thread_local shared_ptr<ClusterConfig> tl_cluster_config;

}  // namespace

ClusterFamily::ClusterFamily(ServerFamily* server_family) : server_family_(server_family) {
  CHECK_NOTNULL(server_family_);
  ClusterConfig::Initialize();
}

ClusterConfig* ClusterFamily::cluster_config() {
  return tl_cluster_config.get();
}

ClusterShard ClusterFamily::GetEmulatedShardInfo(ConnectionContext* cntx) const {
  ClusterShard info{
      .slot_ranges = {{.start = 0, .end = ClusterConfig::kMaxSlotNum}},
      .master = {},
      .replicas = {},
  };

  optional<Replica::Info> replication_info = server_family_->GetReplicaInfo();
  ServerState& etl = *ServerState::tlocal();
  if (!replication_info.has_value()) {
    DCHECK(etl.is_master);
    std::string cluster_announce_ip = absl::GetFlag(FLAGS_cluster_announce_ip);
    std::string preferred_endpoint =
        cluster_announce_ip.empty() ? cntx->conn()->LocalBindAddress() : cluster_announce_ip;

    info.master = {.id = server_family_->master_id(),
                   .ip = preferred_endpoint,
                   .port = static_cast<uint16_t>(absl::GetFlag(FLAGS_port))};

    for (const auto& replica : server_family_->GetDflyCmd()->GetReplicasRoleInfo()) {
      info.replicas.push_back({.id = etl.remote_client_id_,
                               .ip = replica.address,
                               .port = static_cast<uint16_t>(replica.listening_port)});
    }
  } else {
    info.master = {
        .id = etl.remote_client_id_, .ip = replication_info->host, .port = replication_info->port};
    info.replicas.push_back({.id = server_family_->master_id(),
                             .ip = cntx->conn()->LocalBindAddress(),
                             .port = static_cast<uint16_t>(absl::GetFlag(FLAGS_port))});
  }

  return info;
}

void ClusterFamily::ClusterHelp(ConnectionContext* cntx) {
  string_view help_arr[] = {
      "CLUSTER <subcommand> [<arg> [value] [opt] ...]. Subcommands are:",
      "SLOTS",
      "   Return information about slots range mappings. Each range is made of:",
      "   start, end, master and replicas IP addresses, ports and ids.",
      "NODES",
      "   Return cluster configuration seen by node. Output format:",
      "   <id> <ip:port> <flags> <master> <pings> <pongs> <epoch> <link> <slot> ...",
      "INFO",
      "  Return information about the cluster",
      "HELP",
      "    Prints this help.",
  };
  auto* rb = static_cast<RedisReplyBuilder*>(cntx->reply_builder());
  return rb->SendSimpleStrArr(help_arr);
}

namespace {
void ClusterShardsImpl(const ClusterShards& config, ConnectionContext* cntx) {
  // For more details https://redis.io/commands/cluster-shards/
  constexpr unsigned int kEntrySize = 4;
  auto* rb = static_cast<RedisReplyBuilder*>(cntx->reply_builder());

  auto WriteNode = [&](const Node& node, string_view role) {
    constexpr unsigned int kNodeSize = 14;
    rb->StartArray(kNodeSize);
    rb->SendBulkString("id");
    rb->SendBulkString(node.id);
    rb->SendBulkString("endpoint");
    rb->SendBulkString(node.ip);
    rb->SendBulkString("ip");
    rb->SendBulkString(node.ip);
    rb->SendBulkString("port");
    rb->SendLong(node.port);
    rb->SendBulkString("role");
    rb->SendBulkString(role);
    rb->SendBulkString("replication-offset");
    rb->SendLong(0);
    rb->SendBulkString("health");
    rb->SendBulkString("online");
  };

  rb->StartArray(config.size());
  for (const auto& shard : config) {
    rb->StartArray(kEntrySize);
    rb->SendBulkString("slots");

    rb->StartArray(shard.slot_ranges.size() * 2);
    for (const auto& slot_range : shard.slot_ranges) {
      rb->SendLong(slot_range.start);
      rb->SendLong(slot_range.end);
    }

    rb->SendBulkString("nodes");
    rb->StartArray(1 + shard.replicas.size());
    WriteNode(shard.master, "master");
    for (const auto& replica : shard.replicas) {
      WriteNode(replica, "replica");
    }
  }
}
}  // namespace

void ClusterFamily::ClusterShards(ConnectionContext* cntx) {
  if (ClusterConfig::IsEmulated()) {
    return ClusterShardsImpl({GetEmulatedShardInfo(cntx)}, cntx);
  } else if (tl_cluster_config != nullptr) {
    return ClusterShardsImpl(tl_cluster_config->GetConfig(), cntx);
  } else {
    return cntx->SendError(kClusterNotConfigured);
  }
}

namespace {
void ClusterSlotsImpl(const ClusterShards& config, ConnectionContext* cntx) {
  // For more details https://redis.io/commands/cluster-slots/
  auto* rb = static_cast<RedisReplyBuilder*>(cntx->reply_builder());
  auto WriteNode = [&](const Node& node) {
    constexpr unsigned int kNodeSize = 3;
    rb->StartArray(kNodeSize);
    rb->SendBulkString(node.ip);
    rb->SendLong(node.port);
    rb->SendBulkString(node.id);
  };

  unsigned int slot_ranges = 0;
  for (const auto& shard : config) {
    slot_ranges += shard.slot_ranges.size();
  }

  rb->StartArray(slot_ranges);
  for (const auto& shard : config) {
    for (const auto& slot_range : shard.slot_ranges) {
      const unsigned int array_size =
          /* slot-start, slot-end */ 2 + /* master */ 1 + /* replicas */ shard.replicas.size();
      rb->StartArray(array_size);
      rb->SendLong(slot_range.start);
      rb->SendLong(slot_range.end);
      WriteNode(shard.master);
      for (const auto& replica : shard.replicas) {
        WriteNode(replica);
      }
    }
  }
}
}  // namespace

void ClusterFamily::ClusterSlots(ConnectionContext* cntx) {
  if (ClusterConfig::IsEmulated()) {
    return ClusterSlotsImpl({GetEmulatedShardInfo(cntx)}, cntx);
  } else if (tl_cluster_config != nullptr) {
    return ClusterSlotsImpl(tl_cluster_config->GetConfig(), cntx);
  } else {
    return cntx->SendError(kClusterNotConfigured);
  }
}

namespace {
void ClusterNodesImpl(const ClusterShards& config, string_view my_id, ConnectionContext* cntx) {
  // For more details https://redis.io/commands/cluster-nodes/

  string result;

  auto WriteNode = [&](const Node& node, string_view role, string_view master_id,
                       const vector<SlotRange>& ranges) {
    absl::StrAppend(&result, node.id, " ");

    absl::StrAppend(&result, node.ip, ":", node.port, "@", node.port, " ");

    if (my_id == node.id) {
      absl::StrAppend(&result, "myself,");
    }
    absl::StrAppend(&result, role, " ");

    absl::StrAppend(&result, master_id, " ");

    absl::StrAppend(&result, "0 0 0 connected");

    for (const auto& range : ranges) {
      absl::StrAppend(&result, " ", range.start);
      if (range.start != range.end) {
        absl::StrAppend(&result, "-", range.end);
      }
    }

    absl::StrAppend(&result, "\r\n");
  };

  for (const auto& shard : config) {
    WriteNode(shard.master, "master", "-", shard.slot_ranges);
    for (const auto& replica : shard.replicas) {
      // Only the master prints ranges, so we send an empty set for replicas.
      WriteNode(replica, "slave", shard.master.id, {});
    }
  }

  auto* rb = static_cast<RedisReplyBuilder*>(cntx->reply_builder());
  return rb->SendBulkString(result);
}
}  // namespace

void ClusterFamily::ClusterNodes(ConnectionContext* cntx) {
  if (ClusterConfig::IsEmulated()) {
    return ClusterNodesImpl({GetEmulatedShardInfo(cntx)}, server_family_->master_id(), cntx);
  } else if (tl_cluster_config != nullptr) {
    return ClusterNodesImpl(tl_cluster_config->GetConfig(), server_family_->master_id(), cntx);
  } else {
    return cntx->SendError(kClusterNotConfigured);
  }
}

namespace {
void ClusterInfoImpl(const ClusterShards& config, ConnectionContext* cntx) {
  std::string msg;
  auto append = [&msg](absl::AlphaNum a1, absl::AlphaNum a2) {
    absl::StrAppend(&msg, a1, ":", a2, "\r\n");
  };

  // Initialize response variables to emulated mode.
  string_view state = "ok"sv;
  SlotId slots_assigned = ClusterConfig::kMaxSlotNum + 1;
  size_t known_nodes = 1;
  long epoch = 1;
  size_t cluster_size = 1;

  if (config.empty()) {
    state = "fail"sv;
    slots_assigned = 0;
    cluster_size = 0;
    known_nodes = 0;
  } else {
    known_nodes = 0;
    cluster_size = 0;
    for (const auto& shard_config : config) {
      known_nodes += 1;  // For master
      known_nodes += shard_config.replicas.size();

      if (!shard_config.slot_ranges.empty()) {
        ++cluster_size;
      }
    }
  }

  append("cluster_state", state);
  append("cluster_slots_assigned", slots_assigned);
  append("cluster_slots_ok", slots_assigned);  // We do not support other failed nodes.
  append("cluster_slots_pfail", 0);
  append("cluster_slots_fail", 0);
  append("cluster_known_nodes", known_nodes);
  append("cluster_size", cluster_size);
  append("cluster_current_epoch", epoch);
  append("cluster_my_epoch", 1);
  append("cluster_stats_messages_ping_sent", 1);
  append("cluster_stats_messages_pong_sent", 1);
  append("cluster_stats_messages_sent", 1);
  append("cluster_stats_messages_ping_received", 1);
  append("cluster_stats_messages_pong_received", 1);
  append("cluster_stats_messages_meet_received", 0);
  append("cluster_stats_messages_received", 1);
  auto* rb = static_cast<RedisReplyBuilder*>(cntx->reply_builder());
  rb->SendBulkString(msg);
}
}  // namespace

void ClusterFamily::ClusterInfo(ConnectionContext* cntx) {
  if (ClusterConfig::IsEmulated()) {
    return ClusterInfoImpl({GetEmulatedShardInfo(cntx)}, cntx);
  } else if (tl_cluster_config != nullptr) {
    return ClusterInfoImpl(tl_cluster_config->GetConfig(), cntx);
  } else {
    return ClusterInfoImpl({}, cntx);
  }
}

void ClusterFamily::KeySlot(CmdArgList args, ConnectionContext* cntx) {
  if (args.size() != 2) {
    return cntx->SendError(WrongNumArgsError("CLUSTER KEYSLOT"));
  }

  SlotId id = ClusterConfig::KeySlot(ArgS(args, 1));
  return cntx->SendLong(id);
}

void ClusterFamily::Cluster(CmdArgList args, ConnectionContext* cntx) {
  // In emulated cluster mode, all slots are mapped to the same host, and number of cluster
  // instances is thus 1.

  ToUpper(&args[0]);
  string_view sub_cmd = ArgS(args, 0);

  if (!ClusterConfig::IsEnabledOrEmulated()) {
    return cntx->SendError(kClusterDisabled);
  }

  if (sub_cmd == "HELP") {
    return ClusterHelp(cntx);
  } else if (sub_cmd == "SHARDS") {
    return ClusterShards(cntx);
  } else if (sub_cmd == "SLOTS") {
    return ClusterSlots(cntx);
  } else if (sub_cmd == "NODES") {
    return ClusterNodes(cntx);
  } else if (sub_cmd == "INFO") {
    return ClusterInfo(cntx);
  } else if (sub_cmd == "KEYSLOT") {
    return KeySlot(args, cntx);
  } else {
    return cntx->SendError(facade::UnknownSubCmd(sub_cmd, "CLUSTER"), facade::kSyntaxErrType);
  }
}

void ClusterFamily::ReadOnly(CmdArgList args, ConnectionContext* cntx) {
  if (!ClusterConfig::IsEmulated()) {
    return cntx->SendError(kClusterDisabled);
  }
  cntx->SendOk();
}

void ClusterFamily::ReadWrite(CmdArgList args, ConnectionContext* cntx) {
  if (!ClusterConfig::IsEmulated()) {
    return cntx->SendError(kClusterDisabled);
  }
  cntx->SendOk();
}

void ClusterFamily::DflyCluster(CmdArgList args, ConnectionContext* cntx) {
  if (!ClusterConfig::IsEnabledOrEmulated()) {
    return cntx->SendError(kClusterDisabled);
  }

  if (cntx->conn() && !cntx->conn()->IsPrivileged()) {
    return cntx->SendError(kDflyClusterCmdPort);
  }

  ToUpper(&args[0]);
  string_view sub_cmd = ArgS(args, 0);
  args.remove_prefix(1);  // remove subcommand name
  if (sub_cmd == "GETSLOTINFO") {
    return DflyClusterGetSlotInfo(args, cntx);
  } else if (sub_cmd == "CONFIG") {
    return DflyClusterConfig(args, cntx);
  } else if (sub_cmd == "MYID") {
    return DflyClusterMyId(args, cntx);
  } else if (sub_cmd == "FLUSHSLOTS") {
    return DflyClusterFlushSlots(args, cntx);
  } else if (sub_cmd == "START-SLOT-MIGRATION") {
    return DflyClusterStartSlotMigration(args, cntx);
  } else if (sub_cmd == "SLOT-MIGRATION-STATUS") {
    return DflySlotMigrationStatus(args, cntx);
  }

  return cntx->SendError(UnknownSubCmd(sub_cmd, "DFLYCLUSTER"), kSyntaxErrType);
}

void ClusterFamily::DflyClusterMyId(CmdArgList args, ConnectionContext* cntx) {
  if (!args.empty()) {
    return cntx->SendError(WrongNumArgsError("DFLYCLUSTER MYID"));
  }
  auto* rb = static_cast<RedisReplyBuilder*>(cntx->reply_builder());
  rb->SendBulkString(server_family_->master_id());
}

namespace {
SlotSet GetDeletedSlots(bool is_first_config, const SlotSet& before, const SlotSet& after) {
  SlotSet result;
  for (SlotId id = 0; id <= ClusterConfig::kMaxSlotNum; ++id) {
    if ((before.contains(id) || is_first_config) && !after.contains(id)) {
      result.insert(id);
    }
  }
  return result;
}

// Guards set configuration, so that we won't handle 2 in parallel.
Mutex set_config_mu;

void DeleteSlots(const SlotSet& slots) {
  if (slots.empty()) {
    return;
  }

  auto cb = [&](auto*) {
    EngineShard* shard = EngineShard::tlocal();
    if (shard == nullptr)
      return;

    shard->db_slice().FlushSlots(slots);
  };
  shard_set->pool()->AwaitFiberOnAll(std::move(cb));
}

void WriteFlushSlotsToJournal(const SlotSet& slots) {
  if (slots.empty()) {
    return;
  }

  // Build args
  vector<string> args;
  args.reserve(slots.size() + 1);
  args.push_back("FLUSHSLOTS");
  for (const SlotId slot : slots) {
    args.push_back(absl::StrCat(slot));
  }

  // Build view
  vector<string_view> args_view(args.size());
  for (size_t i = 0; i < args.size(); ++i) {
    args_view[i] = args[i];
  }

  auto cb = [&](auto*) {
    EngineShard* shard = EngineShard::tlocal();
    if (shard == nullptr) {
      return;
    }

    auto journal = EngineShard::tlocal()->journal();
    if (journal == nullptr) {
      return;
    }

    // Send journal entry
    // TODO: Break slot migration upon FLUSHSLOTS
    journal->RecordEntry(/* txid= */ 0, journal::Op::COMMAND, /* dbid= */ 0,
                         /* shard_cnt= */ shard_set->size(), nullopt,
                         make_pair("DFLYCLUSTER", args_view), false);
  };
  shard_set->pool()->AwaitFiberOnAll(std::move(cb));
}
}  // namespace

void ClusterFamily::DflyClusterConfig(CmdArgList args, ConnectionContext* cntx) {
  SinkReplyBuilder* rb = cntx->reply_builder();

  if (args.size() != 1) {
    return rb->SendError(WrongNumArgsError("DFLYCLUSTER CONFIG"));
  }

  string_view json_str = ArgS(args, 0);
  optional<JsonType> json = JsonFromString(json_str);
  if (!json.has_value()) {
    LOG(WARNING) << "Can't parse JSON for ClusterConfig " << json_str;
    return rb->SendError("Invalid JSON cluster config", kSyntaxErrType);
  }

  shared_ptr<ClusterConfig> new_config =
      ClusterConfig::CreateFromConfig(server_family_->master_id(), json.value());
  if (new_config == nullptr) {
    LOG(WARNING) << "Can't set cluster config";
    return rb->SendError("Invalid cluster configuration.");
  }

  lock_guard gu(set_config_mu);

  if (!outgoing_migration_jobs_.empty()) {
    // TODO refactor after we approve cluster migration finalization approach
    auto deleted_slots =
        GetDeletedSlots(false, tl_cluster_config->GetOwnedSlots(), new_config->GetOwnedSlots());

    if (!deleted_slots.empty()) {
      for (const auto& migration : outgoing_migration_jobs_) {
        // I use realy weak check here and suppose that new config stops
        // only one migration and don't drop extra slots
        // TODO add additional checks or refactor
        if (ContainsAllSlots(deleted_slots, migration.GetSlotRange())) {
          server_family_->service().proactor_pool().AwaitFiberOnAll([] {
            ServerState::tlocal()->SetIsMigrationFinalization(true);
            tl_cluster_config->SetMigratedSlots(std::move(deleted_slots));
          });
          break;
        }
      }
    }
  }

  bool is_first_config = true;
  SlotSet before;
  if (tl_cluster_config != nullptr) {
    is_first_config = false;
    before = tl_cluster_config->GetOwnedSlots();
  }

  // Ignore blocked commands because we filter them with CancelBlockingOnThread
  DispatchTracker tracker{server_family_->GetListeners(), cntx->conn(), false /* ignore paused */,
                          true /* ignore blocked */};

  auto blocking_filter = [&new_config](ArgSlice keys) {
    bool moved = any_of(keys.begin(), keys.end(), [&](auto k) { return !new_config->IsMySlot(k); });
    return moved ? OpStatus::KEY_MOVED : OpStatus::OK;
  };

  auto cb = [this, &tracker, &new_config, blocking_filter](util::ProactorBase* pb) {
    server_family_->CancelBlockingOnThread(blocking_filter);
    tl_cluster_config = new_config;
    tracker.TrackOnThread();
  };

  server_family_->service().proactor_pool().AwaitFiberOnAll(std::move(cb));
  DCHECK(tl_cluster_config != nullptr);

  if (!tracker.Wait(absl::Seconds(1))) {
    LOG(WARNING) << "Cluster config change timed out";
  }

  SlotSet after = tl_cluster_config->GetOwnedSlots();
  if (ServerState::tlocal()->is_master) {
    auto deleted_slots = GetDeletedSlots(is_first_config, before, after);
    DeleteSlots(deleted_slots);
    WriteFlushSlotsToJournal(deleted_slots);
  }

  return rb->SendOk();
}

void ClusterFamily::DflyClusterGetSlotInfo(CmdArgList args, ConnectionContext* cntx) {
  CmdArgParser parser(args);
  parser.ExpectTag("SLOTS");
  auto* rb = static_cast<RedisReplyBuilder*>(cntx->reply_builder());

  vector<std::pair<SlotId, SlotStats>> slots_stats;
  do {
    auto sid = parser.Next<uint32_t>();
    if (sid > ClusterConfig::kMaxSlotNum)
      return rb->SendError("Invalid slot id");
    slots_stats.emplace_back(sid, SlotStats{});
  } while (parser.HasNext());

  if (auto err = parser.Error(); err)
    return rb->SendError(err->MakeReply());

  Mutex mu;

  auto cb = [&](auto*) {
    EngineShard* shard = EngineShard::tlocal();
    if (shard == nullptr)
      return;

    lock_guard lk(mu);
    for (auto& [slot, data] : slots_stats) {
      data += shard->db_slice().GetSlotStats(slot);
    }
  };

  shard_set->pool()->AwaitFiberOnAll(std::move(cb));

  rb->StartArray(slots_stats.size());

  for (const auto& slot_data : slots_stats) {
    rb->StartArray(9);
    rb->SendLong(slot_data.first);
    rb->SendBulkString("key_count");
    rb->SendLong(static_cast<long>(slot_data.second.key_count));
    rb->SendBulkString("total_reads");
    rb->SendLong(static_cast<long>(slot_data.second.total_reads));
    rb->SendBulkString("total_writes");
    rb->SendLong(static_cast<long>(slot_data.second.total_writes));
    rb->SendBulkString("memory_bytes");
    rb->SendLong(static_cast<long>(slot_data.second.memory_bytes));
  }
}

void ClusterFamily::DflyClusterFlushSlots(CmdArgList args, ConnectionContext* cntx) {
  SlotSet slots;
  slots.reserve(args.size());
  for (size_t i = 0; i < args.size(); ++i) {
    unsigned slot;
    if (!absl::SimpleAtoi(ArgS(args, i), &slot) || (slot > ClusterConfig::kMaxSlotNum)) {
      return cntx->SendError(kSyntaxErrType);
    }
    slots.insert(static_cast<SlotId>(slot));
  }

  DeleteSlots(slots);

  return cntx->SendOk();
}

void ClusterFamily::DflyClusterStartSlotMigration(CmdArgList args, ConnectionContext* cntx) {
  CmdArgParser parser(args);
  auto [host_ip, port] = parser.Next<std::string_view, uint16_t>();
  std::vector<SlotRange> slots;
  do {
    auto [slot_start, slot_end] = parser.Next<SlotId, SlotId>();
    slots.emplace_back(SlotRange{slot_start, slot_end});
  } while (parser.HasNext());

  if (auto err = parser.Error(); err)
    return cntx->SendError(err->MakeReply());

  auto* node = AddMigration(std::string(host_ip), port, std::move(slots));
  if (!node) {
    return cntx->SendError("Can't start the migration, another one is in progress");
  }
  node->Start(cntx);

  return cntx->SendOk();
}

static std::string_view state_to_str(MigrationState state) {
  switch (state) {
    case MigrationState::C_NO_STATE:
      return "NO_STATE"sv;
    case MigrationState::C_CONNECTING:
      return "CONNECTING"sv;
    case MigrationState::C_FULL_SYNC:
      return "FULL_SYNC"sv;
    case MigrationState::C_STABLE_SYNC:
      return "STABLE_SYNC"sv;
  }
  DCHECK(false) << "Unknown State value " << static_cast<underlying_type_t<MigrationState>>(state);
  return "UNDEFINED_STATE"sv;
}

void ClusterFamily::DflySlotMigrationStatus(CmdArgList args, ConnectionContext* cntx) {
  CmdArgParser parser(args);
  auto* rb = static_cast<RedisReplyBuilder*>(cntx->reply_builder());

  if (parser.HasNext()) {
    auto [host_ip, port] = parser.Next<std::string_view, uint16_t>();
    if (auto err = parser.Error(); err)
      return rb->SendError(err->MakeReply());

    lock_guard lk(migration_mu_);
    // find incoming slot migration
    for (const auto& m : incoming_migrations_jobs_) {
      const auto& info = m->GetInfo();
      if (info.host == host_ip && info.port == port)
        return rb->SendSimpleString(state_to_str(m->GetState()));
    }
    // find outgoing slot migration
    for (const auto& [_, info] : outgoing_migration_jobs_) {
      if (info->GetHostIp() == host_ip && info->GetPort() == port)
        return rb->SendSimpleString(state_to_str(info->GetState()));
    }
  } else if (auto arr_size = incoming_migrations_jobs_.size() + outgoing_migration_jobs_.size();
             arr_size != 0) {
    rb->StartArray(arr_size);
    const auto& send_answer = [rb](std::string_view direction, std::string_view host, uint16_t port,
                                   auto state) {
      auto str = absl::StrCat(direction, " ", host, ":", port, " ", state_to_str(state));
      rb->SendSimpleString(str);
    };
    lock_guard lk(migration_mu_);
    for (const auto& m : incoming_migrations_jobs_) {
      const auto& info = m->GetInfo();
      send_answer("in", info.host, info.port, m->GetState());
    }
    for (const auto& [_, info] : outgoing_migration_jobs_) {
      send_answer("out", info->GetHostIp(), info->GetPort(), info->GetState());
    }
    return;
  }
  return rb->SendSimpleString(state_to_str(MigrationState::C_NO_STATE));
}

void ClusterFamily::DflyMigrate(CmdArgList args, ConnectionContext* cntx) {
  ToUpper(&args[0]);
  string_view sub_cmd = ArgS(args, 0);
  args.remove_prefix(1);
  if (sub_cmd == "CONF") {
    MigrationConf(args, cntx);
  } else if (sub_cmd == "FLOW") {
    DflyMigrateFlow(args, cntx);
  } else if (sub_cmd == "FULL-SYNC-CUT") {
    DflyMigrateFullSyncCut(args, cntx);
  } else {
    cntx->SendError(facade::UnknownSubCmd(sub_cmd, "DFLYMIGRATE"), facade::kSyntaxErrType);
  }
}

ClusterSlotMigration* ClusterFamily::AddMigration(std::string host_ip, uint16_t port,
                                                  std::vector<ClusterConfig::SlotRange> slots) {
  lock_guard lk(migration_mu_);
  for (const auto& mj : incoming_migrations_jobs_) {
    if (auto info = mj->GetInfo(); info.host == host_ip && info.port == port) {
      return nullptr;
    }
  }
  return incoming_migrations_jobs_
      .emplace_back(make_unique<ClusterSlotMigration>(std::string(host_ip), port,
                                                      &server_family_->service(), std::move(slots)))
      .get();
}

void ClusterFamily::MigrationConf(CmdArgList args, ConnectionContext* cntx) {
  VLOG(1) << "Create slot migration config";
  CmdArgParser parser{args};
  auto port = parser.Next<uint16_t>();

  std::vector<ClusterConfig::SlotRange> slots;
  do {
    auto [slot_start, slot_end] = parser.Next<SlotId, SlotId>();
    slots.emplace_back(SlotRange{slot_start, slot_end});
  } while (parser.HasNext());

  if (auto err = parser.Error(); err)
    return cntx->SendError(err->MakeReply());

  if (!tl_cluster_config) {
    cntx->SendError(kClusterNotConfigured);
    return;
  }

  for (const auto& migration_range : slots) {
    for (auto i = migration_range.start; i <= migration_range.end; ++i) {
      if (!tl_cluster_config->IsMySlot(i)) {
        VLOG(1) << "Invalid migration slot " << i << " in range " << migration_range.start << ':'
                << migration_range.end;
        cntx->SendError("Invalid slots range");
        return;
      }
    }
  }

  auto sync_id = CreateOutgoingMigration(cntx, port, std::move(slots));

  cntx->conn()->SetName("slot_migration_ctrl");
  auto* rb = static_cast<RedisReplyBuilder*>(cntx->reply_builder());
  rb->StartArray(2);
  rb->SendLong(sync_id);
  rb->SendLong(shard_set->size());
  return;
}

uint32_t ClusterFamily::CreateOutgoingMigration(ConnectionContext* cntx, uint16_t port,
                                                std::vector<ClusterConfig::SlotRange> slots) {
  std::lock_guard lk(migration_mu_);
  auto sync_id = next_sync_id_++;
  auto err_handler = [this, sync_id](const GenericError& err) {
    LOG(INFO) << "Slot migration error: " << err.Format();

    // Todo add error processing, stop migration process
    // fb2::Fiber("stop_Migration", &ClusterFamily::StopMigration, this, sync_id).Detach();
  };
  auto info =
      make_shared<OutgoingMigration>(shard_set->size(), cntx->conn()->RemoteEndpointAddress(), port,
                                     std::move(slots), err_handler);
  auto [it, inserted] = outgoing_migration_jobs_.emplace(sync_id, std::move(info));
  CHECK(inserted);
  return sync_id;
}

void ClusterFamily::DflyMigrateFlow(CmdArgList args, ConnectionContext* cntx) {
  CmdArgParser parser{args};
  auto [sync_id, shard_id] = parser.Next<uint32_t, uint32_t>();

  if (auto err = parser.Error(); err) {
    return cntx->SendError(err->MakeReply());
  }

  VLOG(1) << "Create flow sync_id: " << sync_id << " shard_id: " << shard_id;

  cntx->conn()->SetName(absl::StrCat("migration_flow_", sync_id));

  auto info = GetOutgoingMigration(sync_id);
  if (!info)
    return cntx->SendError(kIdNotFound);

  cntx->conn()->Migrate(shard_set->pool()->at(shard_id));

  cntx->SendOk();

  EngineShard* shard = EngineShard::tlocal();
  DCHECK(shard->shard_id() == shard_id);

  info->StartFlow(&shard->db_slice(), sync_id, server_family_->journal(), cntx->conn()->socket());
}

void ClusterFamily::DflyMigrateFullSyncCut(CmdArgList args, ConnectionContext* cntx) {
  CmdArgParser parser{args};
  auto [sync_id, shard_id] = parser.Next<uint32_t, uint32_t>();

  if (auto err = parser.Error(); err) {
    return cntx->SendError(err->MakeReply());
  }

  VLOG(1) << "Full sync cut "
          << " sync_id: " << sync_id << " shard_id: " << shard_id << " shard";

  std::lock_guard lck(migration_mu_);
  auto migration_it =
      std::find_if(incoming_migrations_jobs_.begin(), incoming_migrations_jobs_.end(),
                   [sync_id](const auto& el) { return el->GetSyncId() == sync_id; });

  if (migration_it == incoming_migrations_jobs_.end()) {
    LOG(WARNING) << "Couldn't find migration id";
    return cntx->SendError(kIdNotFound);
  }

  (*migration_it)->SetStableSyncForFlow(shard_id);
  if ((*migration_it)->GetState() == MigrationState::C_STABLE_SYNC) {
    (*migration_it)->Stop();
    LOG(INFO) << "STABLE-SYNC state is set for sync_id " << sync_id;
  }

  cntx->SendOk();
}

shared_ptr<OutgoingMigration> ClusterFamily::GetOutgoingMigration(uint32_t sync_id) {
  unique_lock lk(migration_mu_);
  auto sync_it = outgoing_migration_jobs_.find(sync_id);
  return sync_it != outgoing_migration_jobs_.end() ? sync_it->second : nullptr;
}

using EngineFunc = void (ClusterFamily::*)(CmdArgList args, ConnectionContext* cntx);

inline CommandId::Handler HandlerFunc(ClusterFamily* se, EngineFunc f) {
  return [=](CmdArgList args, ConnectionContext* cntx) { return (se->*f)(args, cntx); };
}

#define HFUNC(x) SetHandler(HandlerFunc(this, &ClusterFamily::x))

namespace acl {
constexpr uint32_t kCluster = SLOW;
// Reconsider to maybe more sensible defaults
constexpr uint32_t kDflyCluster = ADMIN | SLOW;
constexpr uint32_t kReadOnly = FAST | CONNECTION;
constexpr uint32_t kReadWrite = FAST | CONNECTION;
constexpr uint32_t kDflyMigrate = ADMIN | SLOW | DANGEROUS;
}  // namespace acl

void ClusterFamily::Register(CommandRegistry* registry) {
  registry->StartFamily();
  *registry << CI{"CLUSTER", CO::READONLY, -2, 0, 0, acl::kCluster}.HFUNC(Cluster)
            << CI{"DFLYCLUSTER",    CO::ADMIN | CO::GLOBAL_TRANS | CO::HIDDEN, -2, 0, 0,
                  acl::kDflyCluster}
                   .HFUNC(DflyCluster)
            << CI{"READONLY", CO::READONLY, 1, 0, 0, acl::kReadOnly}.HFUNC(ReadOnly)
            << CI{"READWRITE", CO::READONLY, 1, 0, 0, acl::kReadWrite}.HFUNC(ReadWrite)
            << CI{"DFLYMIGRATE", CO::ADMIN | CO::HIDDEN, -1, 0, 0, acl::kDflyMigrate}.HFUNC(
                   DflyMigrate);
}

}  // namespace dfly
