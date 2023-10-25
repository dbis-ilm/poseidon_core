/*
 * Copyright (C) 2019-2020 DBIS Group - TU Ilmenau, All Rights Reserved.
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

#ifndef btree_hpp_
#define btree_hpp_

#include "defs.hpp"

#include "pfbtree.hpp"
#include "imbtree.hpp"

/**
 * Paged-file B+-tree implementation.
 */
using pf_btree_impl = pfbtree::BPTree<uint64_t, offset_t, 65500, 65500>; // 50, 50
using pf_btree_ptr = std::shared_ptr<pf_btree_impl>;

inline pf_btree_ptr make_pf_btree(bufferpool& pool, uint64_t file_id) { return std::make_shared<pf_btree_impl>(pool, file_id); }

/**
 * In-memory B+-tree implementation.
 */
using im_btree_impl = imbtree::BPTree<uint64_t, offset_t, 126, 10>; // 50, 50
using im_btree_ptr = std::shared_ptr<im_btree_impl>;

inline im_btree_ptr make_im_btree() { return std::make_shared<im_btree_impl>(); }

/**
 * Typedef used for index identifiers.
 */
using index_id = boost::variant<boost::blank, 
                                pf_btree_ptr, 
                                im_btree_ptr
>;

#endif
