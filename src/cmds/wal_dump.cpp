#include <iostream>
#include "walog.hpp"

void dump_tx_record(const wal::log_tx_record *rec) {
    std::cout << rec->lsn << ": Tx#" << short_ts(rec->tx_id) << " ";
    switch (rec->tx_cmd) {
    case log_bot:
        std::cout << "BOT"; break;
    case log_commit:
        std::cout << "Commit"; break;
    case log_abort:
        std::cout << "Abort"; break;
    }
    std::cout << std::endl;
}

void dump_node_record(const wal::log_node_record *rec) {
    std::cout << rec->lsn << ": Tx#" << short_ts(rec->tx_id) << " Node @" << rec->oid << ": ";
    if (rec->log_type == log_insert) {
        std::cout << " created { label="
                << rec->after.label << ", from="
                << uint64_to_string(rec->after.from_rship_list) << ", to="
                << uint64_to_string(rec->after.to_rship_list) << ", props="
                << uint64_to_string(rec->after.property_list) << " }";
    }
    else if (rec->log_type == log_update) {
        std::cout << " updated { label="
                << rec->after.label << "(" << rec->before.label << "), from="
                << uint64_to_string(rec->after.from_rship_list) << "(" << uint64_to_string(rec->before.from_rship_list) << "), to="
                << uint64_to_string(rec->after.to_rship_list) << "(" << uint64_to_string(rec->before.to_rship_list) << "), props="
                << uint64_to_string(rec->after.property_list) << "(" << uint64_to_string(rec->before.property_list) << ") }";

    }
    else if (rec->log_type == log_delete) {
        std::cout << "deleted.";
    }

    std::cout << std::endl;
}

void dump_rship_record(const wal::log_rship_record *rec) {
    std::cout << rec->lsn << ": Tx#" << short_ts(rec->tx_id) << " Relationship @" << rec->oid;
    if (rec->log_type == log_insert) {
        std::cout << " created { label="
                << rec->after.label << " }";
    }
    else if (rec->log_type == log_update) {
        std::cout << " updated { label="
                << rec->after.label << "(" << rec->before.label << ") }";
    }
    else if (rec->log_type == log_delete) {
        std::cout << "deleted.";
    }

    std::cout << std::endl;
}

void dump_dict_record(const wal::log_dict_record *rec) {
    std::cout << rec->lsn << ": Tx#" << short_ts(rec->tx_id) << " Dictionary: " 
        << rec->code << " -> " << rec->str << std::endl;
}

void dump_checkpoint_record(const wal::log_checkpoint_record *rec) {
    std::cout << rec->lsn << ": Checkpoint" << std::endl;
}

int main(int argc, char **argv) {
    if (argc != 2) {
        std::cerr << "Usage: wal_dump <log_file>" << std::endl;
        exit(-1);
    }

    std::string path = argv[1];
    wa_log log(path);

    for(auto li = log.log_begin(); li != log.log_end(); ++li) {
        if (li.log_type() == log_tx) {
            dump_tx_record(li.get<wal::log_tx_record>());
        }
        else if (li.log_type() == log_chkpt) {
            dump_checkpoint_record(li.get<wal::log_checkpoint_record>());
        }
        else {
            switch (li.obj_type()) {
                case log_node:
                    dump_node_record(li.get<wal::log_node_record>()); break;
                case log_rship:
                    dump_rship_record(li.get<wal::log_rship_record>()); break;
                case log_dict:
                    dump_dict_record(li.get<wal::log_dict_record>()); break;
                default:
                    break;
            }
        }
    }
}