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

#ifndef ts_list_hpp_
#define ts_list_hpp_

#include <list>
#include <thread>

/**
 * A wrapper around std::list to make it thread-safe. The main purpose of this class is to provide
 * a mutex for each class instance which can be used to make access to the list thread-safe.
 */
template <typename T> struct ts_list {
    using const_iterator = typename std::list<T>::const_iterator;
    using iterator = typename std::list<T>::iterator;
 
    std::list<T> the_list_;
    std::mutex mtx_;

    ts_list() = default;
    ts_list(const ts_list& ts) : the_list_(ts.the_list_) {}

    ~ts_list() = default;

    inline decltype(auto) empty() const { return the_list_.empty(); }

    inline decltype(auto) begin() { return the_list_.begin(); }
    inline decltype(auto) end() { return the_list_.end(); }

    inline decltype(auto) front() { return the_list_.front(); }

    inline decltype(auto) size() const { return the_list_.size(); }

    template <class Predicate>
    void remove_if (Predicate pred) { the_list_.remove_if(pred); }

    inline void push_front(T&& val) { the_list_.push_front(std::move(val)); }
    inline void pop_front() { the_list_.pop_front(); }

    inline iterator erase(const_iterator first, const_iterator last) { return the_list_.erase(first, last); }
};

#endif