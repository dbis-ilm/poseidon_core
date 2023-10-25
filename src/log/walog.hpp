/*
 * Copyright (C) 2019-2023 DBIS Group - TU Ilmenau, All Rights Reserved.
 *
 * This file is part of the Poseidon package.
 *
 * Poseidon is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Poseidon is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Poseidon. If not, see <http://www.gnu.org/licenses/>.
 */
#ifndef walog_hpp_
#define walog_hpp_

#include <cstdio>
#include "defs.hpp"
#include "transaction.hpp"
#include "properties.hpp"
#include "common_log.hpp"
#include "nodes.hpp"
#include "relationships.hpp"

namespace wal {

/**
 * A placeholder for log records - not instantiated.
 */
struct log_dummy {
    uint8_t log_type : 3; // log_entry_type
    uint8_t obj_type : 3; // log_object_type
    uint64_t lsn;         // log sequence number
    xid_t tx_id;          // id of the transaction
    uint64_t prev_offset; // offset (from the beginning of the file) of the previous log entry of the same transaction
};

/**
 * A log record for transaction commands (BOT, commit, abort).
 */
struct log_tx_record {
    log_tx_record(uint64_t seq, xid_t tx, log_tx_type cmd) : log_type(log_tx), obj_type(log_none), lsn(seq), tx_id(tx), prev_offset(0), tx_cmd(cmd) {}

    uint8_t log_type : 3; // log_entry_type
    uint8_t obj_type : 3; // log_object_type
    uint64_t lsn;         // log sequence number
    xid_t tx_id;          // id of the transaction
    uint64_t prev_offset; // offset (from the beginning of the file) of the previous log entry of the same transaction
    uint8_t tx_cmd;       // log_tx_type
};

/**
 * A log record for modifying nodes.
 */
struct log_node_record {
    log_node_record(log_entry_type le, offset_t o) : log_type(le), obj_type(log_node), lsn(0), tx_id(0), oid(o) {}

    uint8_t log_type : 3; // log_entry_type
    uint8_t obj_type : 3; // log_object_type
    uint64_t lsn;         // log sequence number
    xid_t tx_id;          // id of the updating transaction
    uint64_t prev_offset; // offset (from the beginning of the file) of the previous log entry of the same transaction
    offset_t oid;         // the id (node_id, rship_id, property_set) of the object
    struct {
        dcode_t label;             // the node label
        offset_t from_rship_list, to_rship_list, property_list;
    } before;
    struct after {
        dcode_t label;             // the node label
        offset_t from_rship_list, to_rship_list, property_list;
    } after;
};

/**
 * A log record for modifying relationships.
 */
struct log_rship_record {
    log_rship_record(log_entry_type le, offset_t o) : log_type(le), obj_type(log_rship), lsn(0), tx_id(0), oid(o) {}

    uint8_t log_type : 3; // log_entry_type
    uint8_t obj_type : 3; // log_object_type
    uint64_t lsn;         // log sequence number
    xid_t tx_id;          // id of the updating transaction
    uint64_t prev_offset; // offset (from the beginning of the file) of the previous log entry of the same transaction
    offset_t oid;         // the id (node_id, rship_id, property_set) of the object
    struct {
        dcode_t label;             // the relationship label
        offset_t src_node, dest_node, next_src_rship, next_dest_rship;
    } before;
    struct {
        dcode_t label;             // the relationship label
        offset_t src_node, dest_node, next_src_rship, next_dest_rship;
    } after;
};

/**
 * A log record for adding strings to the dictionary.
 */
struct log_dict_record {
    log_dict_record(dcode_t c, const std::string& s) : log_type(log_insert), obj_type(log_dict), lsn(0), code(c) {
        strcpy(str, s.c_str());
    }

    uint8_t log_type : 3; // log_entry_type
    uint8_t obj_type : 3; // log_object_type
    uint64_t lsn;         // log sequence number
    xid_t tx_id;          // id of the updating transaction
    uint64_t prev_offset; // offset (from the beginning of the file) of the previous log entry of the same transaction
    dcode_t code;
    char str[1024];
};

/**
 * A log record for checkpoints.
 */
struct log_checkpoint_record {
    log_checkpoint_record(uint64_t l) : log_type(log_chkpt), obj_type(log_none), lsn(l), tx_id(0), prev_offset(0) {}

    uint8_t log_type : 3; // log_entry_type
    uint8_t obj_type : 3; // log_object_type
    uint64_t lsn;         // log sequence number
    xid_t tx_id;          // id of the transaction
    uint64_t prev_offset; // offset (from the beginning of the file) of the previous log entry of the same transaction
};

/**
 * Construct a log record for inserting a node based on the dirty node data.
 */
log_node_record create_insert_node_record(const dirty_node_ptr& nptr);

/**
 * Construct a log record for deleting a node based on the old node data.
 */
log_node_record create_delete_node_record(const node& n);

/**
 * Construct a log record for updating a node based on the old node and the dirty node data.
 */
log_node_record create_update_node_record(const node& n_old, const dirty_node_ptr& n_new);

/**
 * Construct a log record for inserting a relationship based on the dirty relationship data.
 */
log_rship_record create_insert_rship_record(const dirty_rship_ptr& rptr);

/**
 * Construct a log record for deleting a relationship based on the old relationship data.
 */
log_rship_record create_delete_rship_record(const relationship& r);

/**
 * Construct a log record for updating a relationship based on the old relationship and the dirty relationship data.
 */
log_rship_record create_update_rship_record(const relationship& r_old, const dirty_rship_ptr& r_new);

}

/**
 * wa_log implements the file-based write-ahead log for Poseidon.
*/
class wa_log {
public:
    /**
    * An iterator for traversing the log file.
    */
    class log_iter {
    public:
        log_iter(FILE *fptr) : fp_(fptr), current_pos_(0) {
            if (fp_ != nullptr)
                read_log_entry();
        }

        bool operator!=(const log_iter &other) const {
            return fp_ != other.fp_;
        }
    
        log_iter &operator++();

        /**
         * Return the current position in the log file.
         */
        offset_t log_position() const { return current_pos_; }

        /**
         * Return the transaction identifier of the log record.
         */
        xid_t transaction_id() const;

        /**
         * Return the type of the log record.
         */
        log_entry_type log_type() const;

        /**
         * Return the object described by this log record.
         */
        log_object_type obj_type() const;

        /**
         * Return the log sequence number of the log record.
         */
        u_int64_t lsn() const;

        /**
         * Return the actual log entry (node, relationship, tx).
         */
        template <typename T> const T *get() const { return (T *)(&entry_); }

    private:
        bool read_log_entry();

        FILE *fp_;
        uint8_t entry_[sizeof(wal::log_dict_record)];
        offset_t current_pos_;
    };

  /**
    * Construct a new and empty log or open an existing one.
    */
    wa_log(const std::string& fname);
 
   /**
    * Destructor
    */
    ~wa_log();

    /**
     * Close the log file, called automatically in the desctrutor. 
     */
    void close(bool trunc = false);

    /**
     * Reset the file pointer to the beginning of the log file.
     */
    void rewind();


   /**
    * This method should be called when a new transaction starts. It reserves a new log and 
    * returns its id which should be used for this transaction.
    */
    void transaction_begin(xid_t txid);

   /**
    * This method is called after the transaction was finished with commit.
    */
    void transaction_commit(xid_t txid);

   /**
    * This method is called after the transaction was aborted.
    */
    void transaction_abort(xid_t txid);

   /**
    * Append a log entry to the given log. log_entry is the address of a log record as defined above.
    */
    void append(void *log_entry, uint32_t lsize);

    void append(xid_t tx_id, wal::log_node_record &log_entry);
    void append(xid_t tx_id, wal::log_rship_record &log_entry);
    void append(xid_t tx_id, wal::log_dict_record &log_entry);

    // void append(xid_t tx_id, wal::log_property_record &log_entry) { log_entry.tx_id = tx_id; append(static_cast<void *>(&log_entry), sizeof(log_entry)); }

    /**
     * Write a checkpoint entry to the log, i.e. a subsequent recovery
     * starts from this point. A checkpoint should guarantee that all
     * updates done before are already written to disk.
     */
    void checkpoint();

   /**
    * Return iterators for traversing the log.
    */
    log_iter log_begin() { rewind(); return log_iter(log_fp_); }
    log_iter log_end() { return log_iter(nullptr); }

    /**
     * Return the typed log record stored at the current position.
     */
    template <typename T> T *get_record(offset_t pos) { fetch_record(pos); return (T *)(&buf_); }

   /**
    * Print the content of the log for debugging purposes.
    */
    void dump();

private:
    void fetch_record(offset_t pos);

    struct file_header {
        char fid_[4] = { 'P', 'S', 'L', 'G' };  // file identifier
        //char padding_[4];
        uint64_t last_lsn_;                     // items stored in the file (nodes, rships, properties)
    } header_;


    uint64_t next_lsn();
    std::map<xid_t, uint64_t> last_offsets_;
    FILE* log_fp_; 
    uint8_t buf_[sizeof(wal::log_rship_record)];

};

#endif