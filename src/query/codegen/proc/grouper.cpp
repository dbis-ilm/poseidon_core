#include "grouper.hpp"

std::mutex group_mtx; 
void grouper::add_to_group(std::string key, qr_tuple qr, std::set<unsigned> pos_set) {  
    std::lock_guard<std::mutex> lck(group_mtx);
    
    pos_set_ = pos_set;
    const auto itr = grpkey_map_.find(key);
    if(itr != grpkey_map_.end()) {
        grps_[itr->second].append(qr);
    } else {
        grpkey_map_.emplace(key, grp_cnt_);
        grpkey_set_.push_back(key);
        result_set rs;
        grps_.push_back(rs);
        grps_[grp_cnt_++].append(qr);
    }
}

void grouper::finish(result_set* rs) {
    std::lock_guard<std::mutex> lck(group_mtx);
    rs->data.clear();
    for(auto &grp : grpkey_set_) {
        qr_tuple res;
        auto gpos = grpkey_map_[grp];
        auto tpl = grps_[gpos].data.front();
        
        for(auto pos : pos_set_) {
            res.push_back(tpl[pos]);
        }

        intermediate_rs_.append(res);
    }
}

qr_tuple* grouper::demat_tuple(int index) {
    std::lock_guard<std::mutex> lck(group_mtx);
    current_tp_ = intermediate_rs_.data.front();
    intermediate_rs_.data.pop_front();

    return &current_tp_;
}

unsigned grouper::get_rs_count() {
    return intermediate_rs_.data.size();
}

void grouper::init_grp_aggr() {
    aggr_grp_cnt_++;
}

unsigned grouper::get_group_cnt() {
    auto &grp_data = grps_[aggr_grp_cnt_].data;
    return grp_data.size(); 
}

unsigned grouper::get_total_group_cnt() {
    total_grp_cnt = 0;
    for(auto & g : grps_) {
        total_grp_cnt += g.data.size();
    }
    
    total_grp_cnt_calc = true;
    return total_grp_cnt;
}

unsigned grouper::get_group_sum_int(int pos) {
    tota_grp_cnt_int = 0;
    auto &grp_data = grps_[aggr_grp_cnt_].data;
    int i = 0;
    for(auto &tpl : grp_data) {
        tota_grp_cnt_int += boost::get<int>(tpl[pos]);
    }
    
    return tota_grp_cnt_int;
}

double grouper::get_group_sum_double(int pos) {
    auto &grp_data = grps_[aggr_grp_cnt_].data;
    double gsum = 0;
    for(auto &tpl : grp_data) {
        gsum += boost::get<double>(tpl[pos]);
    }
    return gsum;
}

uint64_t grouper::get_group_sum_uint(int pos) {
    auto &grp_data = grps_[aggr_grp_cnt_].data;
    uint64_t gsum = 0;
    for(auto &tpl : grp_data) {
        gsum += boost::get<uint64_t>(tpl[pos]);
    }
    return gsum;
}
