/*
 * Copyright (C) 2019-2021 DBIS Group - TU Ilmenau, All Rights Reserved.
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

#ifndef profiling_hpp_
#define profiling_hpp_

#include <cstdint>
#include <chrono>
#include <iostream>
#include "defs.hpp"

#ifdef QOP_PROFILING // Because profiling causes overhead we include it only if needed

/**
 * A set of profiling metrics for query operators. This struct is embedded in all query operators
 * and used to collect some execution metrics.
 */
struct prof_metrics {
    uint64_t in_records_;  // number of input records for this operator
    uint64_t out_records_; // number of records produced by this operator
    std::chrono::duration<double> proc_time_; // the total processing time (currently, it includes the time of the subscribers, too)
    std::chrono::time_point<std::chrono::steady_clock> start_; //
    std::mutex m_;
    
    /**
     * Constructor for initializing the structure.
     */
    prof_metrics() : in_records_(0), out_records_(0), proc_time_(0.0) {}

    /**
     * This function is called at the beginning of the processing method
     * of each operator. It increments the input counter and saves the 
     * current time. 
     * NOTE: this function shouldn't be used directly but only via PROF_PRE. 
     */
    void pre_hook(bool in);

    /**
     * This function is called at the end of the processing method
     * of each operator. It increments the output counter and updates the 
     * processing time. 
     * NOTE: this function shouldn't be used directly but only via PROF_POST(n). 
     */
    void post_hook(uint64_t n);

    /**
     * A simple function for printing the metrics value.
     */
    std::ostream& dump(std::ostream& os) const; 
};

inline std::ostream& operator<< (std::ostream& os, const prof_metrics& pm) { return pm.dump(os); }

#define PROF_DATA prof_metrics pfm_
#define PROF_PRE0 pfm_.pre_hook(false)
#define PROF_PRE pfm_.pre_hook(true)
#define PROF_POST(n) pfm_.post_hook(n)
#define PROF_DUMP pfm_
#define PROF_ACCESSOR inline const prof_metrics& profiling_data() const { return pfm_; }

#else // !QOP_PROFILING

#define PROF_DATA
#define PROF_PRE
#define PROF_POST(n)
#define PROF_DUMP ""
#define PROF_ACCESSOR 

#endif

#endif