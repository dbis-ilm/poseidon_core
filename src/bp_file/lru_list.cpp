 #include "lru_list.hpp"

lru_list::lru_list() : size_(0) {
    head_ = new node(UNKNOWN);
    tail_ = new node(UNKNOWN);
    head_->next = tail_;
    tail_->prev = head_;
}

lru_list::~lru_list() {
    delete head_;
    delete tail_;
}

lru_list::node* lru_list::add_to_mru(uint64_t pid) {
    auto n = new node(pid);
    n->next = tail_;
    n->prev = tail_->prev;
    tail_->prev->next = n;
    tail_->prev = n;
    size_++;
    return n;
}
    
lru_list::node* lru_list::add_to_lru(uint64_t pid) {
    auto n = new node(pid);
    n->prev = head_;
    n->next = head_->next;
    head_->next->prev = n;
    head_->next = n;
    size_++;
    return n;
}

lru_list::node* lru_list::lru() {
    return head_->next;
}
   
void lru_list::move_to_mru(lru_list::node *n) {
    auto pn = n->prev;
    auto nn = n->next;
    pn->next = nn;
    nn->prev = pn;

    n->next = tail_;
    n->prev = tail_->prev;
    tail_->prev->next = n;
    tail_->prev = n; 
}

uint64_t lru_list::remove_lru_node() {
    if (size_ == 0)
        return UNKNOWN;

    auto n = head_->next;
    n->next->prev = head_;
    head_->next = n->next;

    auto pid = n->pid;
    delete n;
    size_--;
    return pid;
}

void lru_list::remove(lru_list::node *n) {
    auto pn = n->prev;
    auto nn = n->next;
    pn->next = nn;
    nn->prev = pn;
    size_--;
}

void lru_list::clear() {
    auto n = head_->next;
    while (n != tail_) {
        auto nn = n->next;
        delete n;
        n = nn;
    }
    size_ = 0;
}
