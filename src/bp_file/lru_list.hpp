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
#ifndef lru_list_hpp_
#define lru_list_hpp_

#include <cstdint>

#include "defs.hpp"

class lru_list {
public:
    struct node {
        uint64_t pid;
        node *prev, *next;

        node(uint64_t p) : pid(p), prev(nullptr), next(nullptr) {}
        ~node() = default;
    };

    class iterator {
    private:
        friend class lru_list;

        node *node_;
        iterator(node *n) : node_(n) {}

    public:
        ~iterator() = default;

        bool operator!=(const iterator &other) const { return node_ != other.node_; }

        uint64_t operator*() const { return node_->pid; }

        iterator &operator++() {
            node_ = node_->next;
            return *this;
        }
    };

    lru_list();
    ~lru_list();

    iterator begin() { return iterator(head_->next); }
    iterator end() { return iterator(tail_); }

    node* add_to_lru(uint64_t pid);
    node* add_to_mru(uint64_t pid);
    node* lru();
    
    void move_to_mru(node *n);
    uint64_t remove_lru_node();
    void remove(node *n);

    std::size_t size() const { return size_; }
    void clear();

private:
    node *head_, // LRU
         *tail_; // MRU
    std::size_t size_;
};

#endif