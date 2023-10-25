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
#ifndef common_log_hpp_
#define common_log_hpp_

#include "defs.hpp"

/**
 * The different kinds of log entries: transaction control, insert, update, delete, and checkpoint.
 */
enum log_entry_type { log_tx = 1, log_insert = 2, log_update = 3, log_delete = 4, log_chkpt = 5 };

/**
 * The different objects (nodes, relationships, property_set, dictionary updates) represented
 * by the log entries.
 */
enum log_object_type { log_none = 1, log_node = 2, log_rship = 3, log_property = 4, log_dict = 5 };

/**
 * The different entry types of a transaction: begin, commit, abort.
 */
enum log_tx_type { log_bot = 1, log_commit = 2, log_abort = 3 };

#endif