#ifndef POSEIDON_CORE_GROUPER_HPP
#define POSEIDON_CORE_GROUPER_HPP
#include <vector>
#include <string>
#include <unordered_map>
#include <set>
#include "qop.hpp"

  /**
   * grouper implements a helper class to process group_by and aggregations. It stores
   * the intermediate results of groups and aggregations.
   * Each group_by operator works on individual grouper objects.
   */
class grouper {
    /**
     * The actual group count
     */
    unsigned grp_cnt_;

    /**
     *  Current position of the aggregation
     */
    int aggr_grp_cnt_;

    /**
     * The actual stored grouped tuples
     */
    std::vector<result_set> grps_;

    /**
     * The set of group keys
     */
    std::vector<std::string> grpkey_set_;

    /**
     * A map that assignes the grouping keys to the appropriate tuple sets in grps_
     */
    std::unordered_map<std::string, unsigned> grpkey_map_;

    /**
     * The position of the group elements in the tuple
     */
    std::set<unsigned> pos_set_;

    /**
     * Stores the finished aggregations
     */
    result_set intermediate_rs_;

    /**
     * A helper objects to store the current tuple for processing
     */
    qr_tuple current_tp_;


    /**
     * Flag to indicate if the total count was already calculated
     */
    bool total_grp_cnt_calc;

    /**
     * The total group count
     */
    unsigned total_grp_cnt;

    /**
     * Flag to indicate of the count was already calculated
     */
    bool grp_cnt_int;

    /**
     * The total group count for integers
     */
    unsigned tota_grp_cnt_int;
    
public:
    grouper() : grp_cnt_(0), aggr_grp_cnt_(-1), grps_({}), grpkey_map_({}), grp_cnt_int(false), tota_grp_cnt_int(0) {}

    void add_to_group(std::string key, qr_tuple qr, std::set<unsigned> pos);
    void finish(result_set* rs);
    qr_tuple* demat_tuple(int index);
    unsigned get_rs_count();

    void init_grp_aggr();
    unsigned get_group_cnt();
    unsigned get_total_group_cnt();
    unsigned get_group_sum_int(int pos);
    double get_group_sum_double(int pos);
    uint64_t get_group_sum_uint(int pos);

    void clear();
};

#endif
