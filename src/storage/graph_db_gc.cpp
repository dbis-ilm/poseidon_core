#include "graph_db.hpp"

void graph_db::vacuum(xid_t tx) {
    std::unique_lock<std::mutex> l(*gcm_);
    auto items = 0;
    auto iter = garbage_->begin();
    bool finished = iter == garbage_->end();
    if (!finished) 
        spdlog::debug("GC: starting vacuum ...");

    while (!finished) {
        bool erased = false;
        auto& gitem = *iter;
        if (gitem.itype == gc_item::gc_node) {
            auto& n = nodes_->get(gitem.oid);

            n.gc(oldest_xid_);
            if (!n.has_dirty_versions()) {
                // wwe assume that we already have checked that n can be removed
                // assert(n.from_rship_list == UNKNOWN && n.to_rship_list == UNKNOWN);
                spdlog::debug("GC: delete node #{}", gitem.oid);
                // remove the node physically
                nodes_->remove(gitem.oid);
                iter = garbage_->erase(iter);
                erased = true;
                items++;
            }
        }
        else {
            auto& rship = rships_->get(gitem.oid);

            rship.gc(oldest_xid_);
            if (!rship.has_dirty_versions()) {
                auto& n1 = nodes_->get(rship.src_node);
                if (n1.from_rship_list == gitem.oid) {
                    n1.from_rship_list = rship.next_src_rship;
                    spdlog::debug("F      update node {} --> rel = {} -> {}", 
                        rship.src_node, n1.from_rship_list, rship.next_src_rship);
                }
                else {
                    // set the field next_src_rship of the previous relationship to our next_src_rship
                    auto prev_id = n1.from_rship_list;
                    while (prev_id != UNKNOWN) {
                        auto &p_relship = rships_->get(prev_id);
                        if (p_relship.next_src_rship == gitem.oid) {
                            spdlog::debug("F      update previous relationship {} of node {} to {}", 
                                prev_id, rship.src_node, rship.next_src_rship);
                            p_relship.next_src_rship = rship.next_src_rship;
                            break;    
                        }
                        prev_id = p_relship.next_src_rship;
                    }
                }

                auto& n2 = nodes_->get(rship.dest_node);
                if (n2.to_rship_list == gitem.oid) {
                    spdlog::debug("T      update node {} --> rel = {} -> {}", rship.dest_node, 
                        n2.to_rship_list, rship.next_dest_rship);
                    n2.to_rship_list = rship.next_dest_rship;
                }
                else {
                    // set the field next_dest_rship of the previous relationship to our next_dest_rship
                    auto prev_id = n2.to_rship_list;
                    while (prev_id != UNKNOWN) {
                        auto &p_relship = rships_->get(prev_id);
                        if (p_relship.next_dest_rship == gitem.oid) {
                            spdlog::debug("T      update previous relationship {} of node {} to {}", 
                                prev_id, rship.dest_node, rship.next_dest_rship);
                            p_relship.next_dest_rship = rship.next_dest_rship;
                            break;    
                        }
                        prev_id = p_relship.next_dest_rship;
                    }
                }

                spdlog::debug("GC: delete rship #{}", gitem.oid);
                // remove the relationship physically
                rships_->remove(gitem.oid);
                iter = garbage_->erase(iter);
                erased = true;
                items++;
            }
        }
        if (!erased)
            iter++;
        finished = iter == garbage_->end();
    }

    if (items > 0) 
        spdlog::debug("GC: deleted {} items", items);
}
