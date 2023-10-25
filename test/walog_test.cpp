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

#define CATCH_CONFIG_MAIN // This tells Catch to provide a main() - only do
                          // this in one cpp file

#include <catch2/catch_test_macros.hpp>
#include "config.h"
#include "defs.hpp"

#include "walog.hpp"

TEST_CASE("creating a log and appending some entries", "[walog]") {
    remove("file.wal");
    wa_log log("file.wal");

    xid_t txid = 42;
    log.transaction_begin(txid);

    wal::log_node_record r1(log_insert, 1);
    r1.after = { 1, UNKNOWN, UNKNOWN, UNKNOWN };
    log.append(txid, r1);

    wal::log_node_record r2(log_insert, 2);
    r2.after = { 2, UNKNOWN, UNKNOWN, UNKNOWN };
    log.append(txid, r2);

    wal::log_node_record r3(log_insert, 3);
    r3.after = { 3, UNKNOWN, UNKNOWN, UNKNOWN };
    log.append(txid, r3);

    wal::log_rship_record r4(log_insert, 1);
    r4.after = { 4, 1, 2, UNKNOWN, UNKNOWN };
    log.append(txid, r4);

    wal::log_rship_record r5(log_insert, 2);
    r5.after = { 4, 3, 2, UNKNOWN, UNKNOWN };
    log.append(txid, r5);

    wal::log_node_record r6(log_update, 1);
    r6.before = { 1, UNKNOWN, UNKNOWN, UNKNOWN };
    r6.after = { 1, 1, UNKNOWN, UNKNOWN };
    log.append(txid, r6);

    wal::log_node_record r7(log_update, 2);
    r7.before = { 2, UNKNOWN, UNKNOWN, UNKNOWN };
    r7.after = { 2, UNKNOWN, 1, UNKNOWN};
    log.append(txid, r7);

    log.transaction_commit(txid);

    log.close();

    wa_log log2("file.wal");
    log2.rewind();
    int nlogs = 0, ninserts = 0, nupdates = 0, ntx = 0;

    for(auto li = log2.log_begin(); li != log2.log_end(); ++li) {
        nlogs++;
        std::cout << "lsn#" << li.lsn() << ", prev=" << li.get<wal::log_dummy>()->prev_offset << std::endl;
        REQUIRE(li.transaction_id() == txid);
        REQUIRE(li.lsn() == (uint64_t)nlogs);
        
        switch (li.log_type()) {
        case log_tx:
            ntx++;
            break;
        case log_insert:
                ninserts++;
                break;
        case log_update:
                nupdates++;
                break;
        default:
                break;
        }           
    }
    REQUIRE(ninserts == 5);
    REQUIRE(nupdates == 2);
    REQUIRE(ntx == 2);
    REQUIRE(nlogs == 9);

    log2.close();
    remove("file.wal");
}

TEST_CASE("creating a log with two transactions and traverse the records backwards", "[walog]") {
    remove("file2.wal");
    wa_log log("file2.wal");

    xid_t txid = 42;
    log.transaction_begin(txid);

    wal::log_node_record r1(log_insert, 1);
    r1.after = { 1, UNKNOWN, UNKNOWN, UNKNOWN };
    log.append(txid, r1);

    wal::log_node_record r2(log_insert, 2);
    r2.after = { 2, UNKNOWN, UNKNOWN, UNKNOWN };
    log.append(txid, r2);

    wal::log_node_record r3(log_insert, 3);
    r3.after = { 3, UNKNOWN, UNKNOWN, UNKNOWN };
    log.append(txid, r3);

    xid_t txid2 = 69;
    log.transaction_begin(txid2);

    wal::log_rship_record r4(log_insert, 1);
    r4.after = { 4, 1, 2, UNKNOWN, UNKNOWN };
    log.append(txid, r4);

    wal::log_rship_record r5(log_insert, 2);
    r5.after = { 4, 3, 2, UNKNOWN, UNKNOWN };
    log.append(txid, r5);

    wal::log_node_record r10(log_insert, 10);
    r10.after = { 21, UNKNOWN, UNKNOWN, UNKNOWN };
    log.append(txid2, r10);

    wal::log_node_record r11(log_insert, 11);
    r11.after = { 22, UNKNOWN, UNKNOWN, UNKNOWN };
    log.append(txid2, r11);

    wal::log_node_record r6(log_update, 1);
    r6.before = { 1, UNKNOWN, UNKNOWN, UNKNOWN };
    r6.after = { 1, 1, UNKNOWN, UNKNOWN };
    log.append(txid, r6);

    wal::log_node_record r7(log_update, 2);
    r7.before = { 2, UNKNOWN, UNKNOWN, UNKNOWN };
    r7.after = { 2, UNKNOWN, 1, UNKNOWN};
    log.append(txid, r7);

    log.transaction_commit(txid);

    log.transaction_abort(txid2);

    log.close();

    wa_log log2("file2.wal");
    log2.rewind();

    offset_t last_entry = 0;
    for(auto li = log2.log_begin(); li != log2.log_end(); ++li) {
        if (li.transaction_id() == txid2)
            last_entry = li.log_position();
    }

    auto rec1 = log2.get_record<wal::log_tx_record>(last_entry);
    REQUIRE(rec1->log_type == (uint8_t)log_tx);
    REQUIRE(rec1->tx_id == txid2);

    auto rec2 = log2.get_record<wal::log_node_record>(rec1->prev_offset);
    REQUIRE(rec2->log_type == (uint8_t)log_insert);
    REQUIRE(rec2->obj_type == (uint8_t)log_node);
    REQUIRE(rec2->after.label == 22);
    REQUIRE(rec2->tx_id == txid2);

    auto rec3 = log2.get_record<wal::log_node_record>(rec2->prev_offset);
    REQUIRE(rec3->log_type == (uint8_t)log_insert);
    REQUIRE(rec3->obj_type == (uint8_t)log_node);
    REQUIRE(rec3->after.label == 21);
    REQUIRE(rec3->tx_id == txid2);

    auto rec4 = log2.get_record<wal::log_tx_record>(rec3->prev_offset);
    REQUIRE(rec4->log_type == log_tx);
    REQUIRE(rec4->tx_id == txid2);

    log2.close();
    remove("file2.wal");
}
