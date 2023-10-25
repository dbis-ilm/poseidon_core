#include "p_context.hpp"
#include <llvm/Support/TargetSelect.h>

dcode_t PContext::get_dcode(std::string &key) {
    return gdb_->get_code(key);
}

DataLayout PContext::get_data_layout() {
    return module_->getDataLayout();
}

FunctionCallee PContext::extern_func(std::string fct_name) {
    return module_->getOrInsertFunction(fct_name, function_types[fct_name]);
}

LLVMContext &PContext::getContext() {
    return *ctx_;
}

IRBuilder<> &PContext::getBuilder() {
    return *Builder;
}

Module &PContext::getModule() {
    return *module_;
}

std::unique_ptr<Module> PContext::moveModule() {
    return std::move(module_);
}

void PContext::createNewModule() {
    module_ = std::make_unique<Module>("QOP", *ctx_);
}

PContext::PContext(graph_db_ptr gdb) : gdb_(gdb) {
    ctx_ = std::make_unique<LLVMContext>();
    module_ = std::make_unique<Module>("QOP", *ctx_);
    Builder = std::make_unique<IRBuilder<>>(*ctx_);

    voidTy = Type::getVoidTy(*ctx_);
    boolTy = Type::getInt1Ty(*ctx_);
    int8Ty = Type::getInt8Ty(*ctx_);
    int8PtrTy = Type::getInt8PtrTy(*ctx_);
    int24Ty = IntegerType::get(*ctx_, 24);
    int24PtrTy = PointerType::getUnqual(int24Ty);
    int32Ty = Type::getInt32Ty(*ctx_);
    int32PtrTy = Type::getInt32PtrTy(*ctx_);
    int64Ty = Type::getInt64Ty(*ctx_);
    int64PtrTy = Type::getInt64PtrTy(*ctx_);
    doubleTy = Type::getDoubleTy(*ctx_);
    doublePtrTy = Type::getDoublePtrTy(*ctx_);

//++++++++++++++++++ CONSTANT TYPES ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
    UNKNOWN_ID = ConstantInt::get(int64Ty, std::numeric_limits<uint64_t>::max());
    LLVM_TWO = ConstantInt::get(int64Ty, 2);
    LLVM_ONE = ConstantInt::get(int64Ty, 1);
    LLVM_ZERO = ConstantInt::get(int64Ty, 0);
    MAX_PITEM_CNT = ConstantInt::get(int64Ty, 3);

//++++++++++++++++++ DATA_STRUCTURES +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
//++++++++++++++++++ QR_LIST +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

    queryContextTy = StructType::create(*ctx_, "jit_args");
    
//++++++++++++++++++ NODE TYPE +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
    opaqueListTy = StructType::create(*ctx_, "opaque_list");
    nodeAtomicIdTy = StructType::create(*ctx_, "node_atomic_id");
    nodeTxnBaseTy = StructType::create(*ctx_, "node_txn_base");
    typecodeArrTy = ArrayType::get(int8Ty, 7);
    nodeTy = StructType::create(*ctx_,
                                "node"); // {int8PtrTy, int8PtrTy, int64Ty, int64Ty, int64Ty, int64Ty, int64Ty}
    nodePtrTy = PointerType::get(nodeTy, 0);
    nodeItTy = StructType::create(*ctx_, {nodeTy}, "node_iterator");
    nodeItPtrTy = PointerType::get(nodeItTy, 0);

    qrResultTy = StructType::create(*ctx_, "query_result");
    qrResultPtrTy = PointerType::get(qrResultTy, 0);

    queryArgTy = ArrayType::get(int64PtrTy, 64);
//++++++++++++++++++ RSHIP TYPE ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
    rshipTy = StructType::create(*ctx_, "relationship");
    rshipPtrTy = PointerType::getUnqual(rshipTy);
    rshipAtomicIdTy = StructType::create(*ctx_, "rship_atomic_id");
    rshipTxnBaseTy = StructType::create(*ctx_, "rship_txn_base");

//++++++++++++++++++ PITEM TYPE ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
    pitemValueArrTy = ArrayType::get(int8Ty, 8);
    pitemTy = StructType::create(*ctx_, "p_item");
    pitemPtrTy = PointerType::get(pitemTy, 0);

    pSetRawArrTy = ArrayType::get(pitemTy, 3);
    pItemListTy = StructType::create(*ctx_, "p_item_list");
    propertySetTy = StructType::create(*ctx_, "property_set");
    propertySetPtrTy = PointerType::get(propertySetTy, 0);

//++++++++++++++++++ GRAPH DB TYPE +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
    graphDbTy = StructType::create(*ctx_, "graph_db"); // TODO: set body
    graphDbSharedTy = StructType::create(*ctx_, "graph_db_shared_ptr");
    graphDbSharedPtrTy = PointerType::get(graphDbSharedTy, 0);
    graphDbPtrTy = PointerType::get(graphDbTy, 0);

    graphDbCVecTy = StructType::create(*ctx_, "chunk_vec_iter");
    queryContextTy = StructType::create(*ctx_, "queryContext");
    queryTimePointTy = StructType::create(*ctx_, "queryTimePoint");
//++++++++++++++++++ NODE SCAN FCT +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
    nodeConsumerFctTy = FunctionType::get(Type::getVoidTy(*ctx_), {nodePtrTy}, false);
    nodeConsumerFctPtrTy = PointerType::getUnqual(nodeConsumerFctTy);
    scanNodesByLabelTy = FunctionType::get(Type::getVoidTy(*ctx_),
                                           {int8PtrTy, int8PtrTy, nodeConsumerFctPtrTy}, false);
    gdb_get_node_by_id_type = FunctionType::get(nodePtrTy, {int8PtrTy, int64Ty}, false);

    queryResultConsumerFctTy = FunctionType::get(Type::getVoidTy(*ctx_), {qrResultPtrTy}, false);

    consumerDummyCall = FunctionType::get(Type::getVoidTy(*ctx_), {qrResultPtrTy}, false);

//++++++++++++++++++ RELATIONSHIP SCAN FCT +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
    rshipConsumerFctTy = FunctionType::get(Type::getVoidTy(*ctx_), {rshipPtrTy}, false);
    rshipConsumerFctPtrTy = PointerType::getUnqual(rshipConsumerFctTy);

    consumerFctTy = FunctionType::get(Type::getVoidTy(*ctx_), {int8PtrTy, int64Ty, int64PtrTy, int64PtrTy, int64Ty, int64PtrTy, int64PtrTy, int64Ty}, false);
    callMapTy = ArrayType::get(consumerFctTy->getPointerTo(), 32);
    callMapPtrTy = callMapTy->getPointerTo();

    foreachRshipFctTy = FunctionType::get(Type::getVoidTy(*ctx_), {int8PtrTy, int64Ty, int64PtrTy, int64PtrTy, int64Ty, int64PtrTy, callMapPtrTy},
                                          false);
    countPotentialOHopFctTy = FunctionType::get(int64Ty, {int8PtrTy, int64Ty}, false);
    retrieveFEVqueryFctTy = FunctionType::get(int8PtrTy, {}, false);
    fevQueueEmptyFctTy = FunctionType::get(boolTy, {int8PtrTy}, false);
    insertInFEVQueueFctTy = FunctionType::get(voidTy, {int8PtrTy, int64Ty, int64Ty}, false);

    feFromVarFctTy = FunctionType::get(voidTy, {int8PtrTy, int32Ty, nodePtrTy, int64Ty, int64Ty}, false);
    getNextRshipFctTy = FunctionType::get(rshipPtrTy, {}, false);
    fevListEndFctTy = FunctionType::get(boolTy, {}, false);

//++++++++++++++++++ FILTER FCT ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
    filterConsumerFctTy = FunctionType::get(Type::getVoidTy(*ctx_), {int8PtrTy, int64Ty, int64PtrTy, int64PtrTy, int64Ty, int64PtrTy},
                                            false);

//++++++++++++++++++ EXPAND FCT +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
    expandConsumerFctTy = FunctionType::get(Type::getVoidTy(*ctx_), {int8PtrTy, int64Ty, int64PtrTy, int64PtrTy, int64Ty, int64PtrTy},
                                            false);
//++++++++++++++++++ JOIN FCT ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
//    joinConsumerFctTy = FunctionType::get(Type::getVoidTy(*ctx_), {int8PtrTy, int64Ty, queryResultListPtrTy},
//                                          false);
    joinInsertLeftFctTy = FunctionType::get(Type::getVoidTy(*ctx_), {int64PtrTy, int64PtrTy},
                                            false); // TODO: insert name argument to identify correct join table
//    joinConsumeLeftFctTy = FunctionType::get(queryResultListPtrTy, {},
//                                            false); // TODO: insert name argument to identify correct join table

    projectConsumerFctTy = FunctionType::get(Type::getVoidTy(*ctx_), {int8PtrTy, int64Ty, int64PtrTy, int64PtrTy, int64Ty, int64PtrTy},
                                             false);

    collectConsumerFctTy = FunctionType::get(Type::getVoidTy(*ctx_), {int8PtrTy, int64Ty, int64PtrTy, int64PtrTy, int64Ty, int64PtrTy, callMapPtrTy}, false);

    applyNodeProjectionFctTy = FunctionType::get(Type::getVoidTy(*ctx_), {int8PtrTy, int8PtrTy, int64Ty, int64PtrTy, int64PtrTy}, false);
    applyRshipProjectionFctTy = FunctionType::get(Type::getVoidTy(*ctx_), {int8PtrTy, int8PtrTy, int64Ty, int64PtrTy, int64PtrTy}, false);
//++++++++++++++++++ START FCT +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

    // gdb, first, last, tx, oid, typevec, resultset, callmap, finish, result_offset
    //startFctTy = FunctionType::get(Type::getVoidTy(*ctx_), {int8PtrTy, int64Ty, int64Ty, int8PtrTy, int64Ty, int64PtrTy, int64PtrTy, int64PtrTy, finishFctTy->getPointerTo(), int64Ty, queryArgTy->getPointerTo()}, false);
    
    
    collectFctTy = FunctionType::get(Type::getVoidTy(*ctx_), {int8PtrTy, int64PtrTy, int64PtrTy, int64Ty, int64PtrTy}, false);
    limitFctTy = FunctionType::get(Type::getVoidTy(*ctx_), {int8PtrTy, int64PtrTy, int64PtrTy, int64Ty, int64PtrTy}, false);


    call_consumer_ty = FunctionType::get(Type::getVoidTy(*ctx_), {int64PtrTy, int8PtrTy, int64Ty, int64PtrTy, int64PtrTy, int64Ty, int64PtrTy, int64PtrTy}, false);

    indexGetNodeTy = FunctionType::get(nodePtrTy, {int8PtrTy, int8PtrTy, int8PtrTy, int64Ty}, false);

//++++++++++++++++++ CONSTANTS +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
    constZero = ConstantInt::get(int8Ty, 0, false);
    constOne = ConstantInt::get(int8Ty, 1, false);

//++++++++++++++++++ DATA_STRUCTURES_FUNCTIONS +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
//++++++++++++++++++ UTILITY +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
    vec_end_reached = FunctionType::get(boolTy, {int8PtrTy, nodeItPtrTy}, false);
    vec_get_begin = FunctionType::get(nodeItPtrTy, {int8PtrTy, int64Ty, int64Ty}, false);
    vec_get_next = FunctionType::get(nodeItPtrTy, {nodeItPtrTy}, false);

    apply_pexpr_ty = FunctionType::get(int64Ty, {int8PtrTy, int8PtrTy, int64Ty, int64PtrTy, int64Ty, int64PtrTy, int64PtrTy}, false);

    res_arr_type = ArrayType::get(int64PtrTy, 64);

    get_join_vec_arr_ty = FunctionType::get(int64PtrTy, {int64PtrTy, int64Ty}, false);
    get_join_vec_size_ty = FunctionType::get(int64Ty, {int64PtrTy}, false);

    mat_reg_val_ty = FunctionType::get(voidTy, {int8PtrTy, int64PtrTy, int64Ty}, false);
    collect_reg_ty = FunctionType::get(voidTy, {int8PtrTy, int64PtrTy, boolTy}, false);

    obtain_mat_tuple_ty = FunctionType::get(int8PtrTy, {}, false);
    reg_to_qres_ty = FunctionType::get(int8PtrTy, {nodePtrTy}, false);
    mat_node_ty = FunctionType::get(voidTy, {nodePtrTy, int8PtrTy}, false);
    mat_rship_ty = FunctionType::get(voidTy, {rshipPtrTy, int8PtrTy}, false);
    collect_tuple_join_ty = FunctionType::get(voidTy, {int8PtrTy, int64Ty, int8PtrTy}, false);
    node_to_description_ty = FunctionType::get(int8PtrTy, {int8PtrTy}, false);

    get_join_tp_at_ty = FunctionType::get(int8PtrTy, {int8PtrTy, int64Ty, int64Ty}, false);
    get_node_res_at_ty = FunctionType::get(nodePtrTy, {int8PtrTy, int64Ty}, false);
    get_rship_res_at_ty = FunctionType::get(rshipPtrTy, {int8PtrTy, int64Ty}, false);
    get_mat_res_size_ty = FunctionType::get(int64Ty, {int8PtrTy, int64Ty}, false);

    get_node_grpkey_ty = FunctionType::get(voidTy, {nodePtrTy, int64Ty}, false);
    get_rship_grpkey_ty = FunctionType::get(voidTy, {rshipPtrTy, int64Ty}, false);
    get_int_grpkey_ty = FunctionType::get(voidTy, {int64Ty, int64Ty}, false);
    get_string_grpkey_ty = FunctionType::get(voidTy, {int64PtrTy, int64Ty}, false);
    get_time_grpkey_ty = FunctionType::get(voidTy, {int64PtrTy, int64Ty}, false);
    add_to_group_ty = FunctionType::get(voidTy, {int8PtrTy}, false);
    finish_group_by_ty = FunctionType::get(voidTy, {int8PtrTy, int64PtrTy}, false);

    grp_demat_at_ty = FunctionType::get(int8PtrTy, {int8PtrTy, int64Ty}, false);
    clear_mat_tuple_ty = FunctionType::get(voidTy, {}, false);
    get_grp_rs_count_ty = FunctionType::get(int64Ty, {int8PtrTy}, false);
    int_to_reg_ty = FunctionType::get(int64Ty, {int8PtrTy, int64Ty}, false);
    str_to_reg_ty = FunctionType::get(int64Ty, {int8PtrTy, int64Ty}, false);
    node_to_reg_ty = FunctionType::get(nodePtrTy, {int8PtrTy, int64Ty}, false);
    rship_to_reg_ty = FunctionType::get(rshipPtrTy, {int8PtrTy, int64Ty}, false);
    time_to_reg_ty = FunctionType::get(int64Ty, {int8PtrTy, int64Ty}, false);

    init_grp_aggr_ty = FunctionType::get(voidTy, {int8PtrTy}, false);
    get_group_cnt_ty = FunctionType::get(int64Ty, {int8PtrTy}, false);
    get_total_group_cnt_ty = FunctionType::get(int64Ty, {int8PtrTy}, false);
    get_group_sum_int_ty = FunctionType::get(int64Ty, {int8PtrTy, int64Ty}, false);
    get_group_sum_double_ty = FunctionType::get(doubleTy, {int8PtrTy, int64Ty}, false);
    get_group_sum_uint_ty = FunctionType::get(int64Ty, {int8PtrTy, int64Ty}, false);

    append_to_tuple_ty = FunctionType::get(voidTy, {int8PtrTy}, false);
    get_qr_tuple_ty = FunctionType::get(int8PtrTy, {}, false);

    insert_join_id_input_ty = FunctionType::get(voidTy, {int8PtrTy, int64Ty, int64Ty}, false);
    get_join_id_at_ty = FunctionType::get(int64Ty, {int8PtrTy, int64Ty, int64Ty}, false);

    collect_tuple_hash_join_ty = FunctionType::get(voidTy, {int8PtrTy, int64Ty, int64Ty, int8PtrTy}, false);
    insert_join_bucket_input_ty = FunctionType::get(voidTy, {int8PtrTy, int64Ty, int64Ty, int64Ty}, false);
    get_hj_input_size_ty = FunctionType::get(int64Ty, {int8PtrTy, int64Ty, int64Ty}, false);
    get_hj_input_id_ty = FunctionType::get(int64Ty, {int8PtrTy, int64Ty, int64Ty, int64Ty}, false);
    get_query_result_ty = FunctionType::get(int8PtrTy, {int8PtrTy, int64Ty, int64Ty, int64Ty}, false);


//++++++++++++++++++ DICT FUNCTIONS ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
    lookup_label_type = FunctionType::get(int32Ty, {int8PtrTy, int8PtrTy}, false);
    lookup_dcode_type = FunctionType::get(int8PtrTy, {int8PtrTy, int32Ty}, false);

//++++++++++++++++++ GDB FUNCTIONS +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
    gdb_get_nodes_type = FunctionType::get(int8PtrTy, {int8PtrTy}, false);
    get_node_from_it_type = FunctionType::get(nodePtrTy, {nodeItPtrTy}, false);
    gdb_get_rship_by_id_type = FunctionType::get(rshipPtrTy, {int8PtrTy, int64Ty}, false);
    gdb_get_dcode_type = FunctionType::get(int64Ty, {int8PtrTy, int8PtrTy}, false);

    create_node_type = FunctionType::get(nodePtrTy, {int8PtrTy, int8PtrTy, int64PtrTy}, false);
    create_rship_type = FunctionType::get(rshipPtrTy, {int8PtrTy, int8PtrTy, nodePtrTy, nodePtrTy, int64PtrTy}, false);


    foreach_variable_from_type = FunctionType::get(voidTy, {int8PtrTy, int32Ty, int64Ty, int64Ty, consumerFctTy->getPointerTo(),
                                                            int64Ty, int64PtrTy, int64PtrTy, int64Ty, int64PtrTy, int64PtrTy, int64Ty}, false);

    node_has_property_ty = FunctionType::get(int64Ty, {int8PtrTy, nodePtrTy, int8PtrTy}, false);
    rship_has_property_ty = FunctionType::get(int64Ty, {int8PtrTy, rshipPtrTy, int8PtrTy}, false);
    apply_has_property_ty = FunctionType::get(voidTy, {int64Ty, int8PtrTy, int8PtrTy, int64PtrTy}, false );
    persis_tuple_type = FunctionType::get(voidTy, {int8PtrTy, int8PtrTy}, false);
//++++++++++++++++++ PROPERTY FUNCTIONS ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
    pset_get_item_at_type = FunctionType::get(propertySetPtrTy, {int8PtrTy, int64Ty}, false);


//++++++++++++++++++ TX FUNCTIONS ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
    getTxFctTy = FunctionType::get(int64Ty, {int8PtrTy}, false);
    getValidNodeFctTy = FunctionType::get(nodePtrTy, {int8PtrTy, nodePtrTy, int8PtrTy}, false);
    notifyFctTy = FunctionType::get(voidTy, {int8PtrTy}, false);

//++++++++++++++++++ BODY DEFINITIONS + ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
    queryContextTy->setBody({int8PtrTy, int64Ty, int64Ty, int8PtrTy}); // gdb, start, end, tx, result_set, arguments
    queryContextPtrTy = PointerType::get(queryContextTy, 0);
    queryTimePointTy->setBody(int64Ty);

    get_now_ty = FunctionType::get(queryTimePointTy, {}, false);
    add_time_diff_ty = FunctionType::get(voidTy, {queryContextPtrTy, int64Ty, queryTimePointTy, queryTimePointTy}, false);

    startFctTy = FunctionType::get(Type::getVoidTy(*ctx_), {queryContextPtrTy, queryArgTy->getPointerTo(), int64PtrTy}, false);
    finishFctTy = FunctionType::get(Type::getVoidTy(*ctx_), {queryContextPtrTy, queryArgTy->getPointerTo(), int64PtrTy}, false);
    
    nodeAtomicIdTy->setBody(int64Ty);
    rshipAtomicIdTy->setBody(int64Ty);
    nodeTxnBaseTy->setBody({nodeAtomicIdTy, int64Ty, int64Ty, int64Ty, int8PtrTy, });
    nodeTy->setBody({nodeTxnBaseTy, int64Ty, int64Ty, int64Ty, int64Ty, int32Ty}); // TODO: check type size

    rshipTxnBaseTy->setBody({rshipAtomicIdTy, int64Ty, int64Ty, int64Ty, int8PtrTy});
    rshipTy->setBody({rshipTxnBaseTy, int64Ty, int64Ty, int64Ty, int64Ty, int64Ty, int64Ty, int32Ty});

    pitemTy->setBody({pitemValueArrTy, int32Ty, int8Ty}); // value, key, flags
    pItemListTy->setBody(pSetRawArrTy);
    propertySetTy->setBody({int64Ty, int64Ty, pItemListTy, int8Ty}); // next, owner, items, flags

    graphDbTy->setBody({int8PtrTy, int8PtrTy, int8PtrTy});
    graphDbSharedTy->setBody({int8PtrTy, int8PtrTy});

    graphDbCVecTy->setBody({int8PtrTy, int64Ty});

    function_types["call_consumer_function"] = call_consumer_ty;

    function_types["pset_get_item_at"] = pset_get_item_at_type;
    function_types["gdb_get_dcode"] = gdb_get_dcode_type;
    function_types["rship_by_id"] = gdb_get_rship_by_id_type;
    function_types["get_node_from_it"] = get_node_from_it_type;
    function_types["gdb_get_nodes"] = gdb_get_nodes_type;

    function_types["gdb_get_rships"] = gdb_get_nodes_type;
    function_types["get_rship_from_it"] = get_node_from_it_type;
    function_types["dict_lookup_label"] = lookup_label_type;
    function_types["dict_lookup_dcode"] = lookup_dcode_type;
    function_types["get_vec_next"] = vec_get_next;
    function_types["get_vec_begin"] = vec_get_begin;
    function_types["vec_end_reached"] = vec_end_reached;
    function_types["get_vec_next_r"] = vec_get_next;
    function_types["get_vec_begin_r"] = vec_get_begin;
    function_types["vec_end_reached_r"] = vec_end_reached;
    
    function_types["scan_nodes_by_label"] = scanNodesByLabelTy;
    function_types["pset_get_item_at"] = pset_get_item_at_type;
    function_types["node_by_id"] = gdb_get_node_by_id_type;
    function_types["collect"] = collectFctTy;
    function_types["join_insert_left"] = joinInsertLeftFctTy;
    function_types["join_consume_left"] = joinConsumeLeftFctTy;
    function_types["get_tx"] = getTxFctTy;
    function_types["get_valid_node"] = getValidNodeFctTy;
    function_types["apply_pexpr"] = apply_pexpr_ty;
    function_types["print_int"] = FunctionType::get(Type::getVoidTy(*ctx_), {boolTy}, false);
    function_types["check_qr"] = FunctionType::get(Type::getVoidTy(*ctx_), {int64PtrTy}, false);
    function_types["get_join_vec_size"] = get_join_vec_size_ty;
    function_types["get_join_vec_arr"] = get_join_vec_arr_ty;
    function_types["create_node"] = create_node_type;
    function_types["create_rship"] = create_rship_type;
    function_types["foreach_variable_from"] = foreach_variable_from_type;
    function_types["mat_reg_value"] = mat_reg_val_ty;
    function_types["collect_tuple"] = collect_reg_ty;
    function_types["obtain_mat_tuple"] = obtain_mat_tuple_ty;
    function_types["reg_to_qres"] = reg_to_qres_ty;
    function_types["mat_node"] = mat_node_ty;
    function_types["mat_rship"] = mat_rship_ty;
    function_types["collect_tuple_join"] = collect_tuple_join_ty;
    function_types["node_to_description"] = node_to_description_ty;

    function_types["get_join_tp_at"] = get_join_tp_at_ty;
    function_types["get_node_res_at"] = get_node_res_at_ty;
    function_types["get_rship_res_at"] = get_rship_res_at_ty;
    function_types["get_mat_res_size"] = get_mat_res_size_ty;

    function_types["index_get_node"] = indexGetNodeTy;

    function_types["apply_pexpr_node"] = applyNodeProjectionFctTy;
    function_types["apply_pexpr_rship"] = applyRshipProjectionFctTy;

    function_types["count_potential_o_hop"] = countPotentialOHopFctTy;
    function_types["retrieve_fev_queue"] = retrieveFEVqueryFctTy;
    function_types["fev_queue_empty"] = fevQueueEmptyFctTy;
    function_types["insert_fev_rship"] = insertInFEVQueueFctTy;

    function_types["foreach_from_variable_rship"] = feFromVarFctTy;
    function_types["get_next_rship_fev"] = getNextRshipFctTy;
    function_types["fev_list_end"] = fevListEndFctTy;

    function_types["get_node_grpkey"] = get_node_grpkey_ty;
    function_types["get_rship_grpkey"] = get_rship_grpkey_ty;
    function_types["get_int_grpkey"] = get_int_grpkey_ty;
    function_types["get_string_grpkey"] = get_string_grpkey_ty;
    function_types["get_time_grpkey"] = get_time_grpkey_ty;
    function_types["add_to_group"] = add_to_group_ty;
    function_types["finish_group_by"] = finish_group_by_ty;

    function_types["grp_demat_at"] = grp_demat_at_ty;
    function_types["clear_mat_tuple"] = clear_mat_tuple_ty;
    function_types["get_grp_rs_count"] = get_grp_rs_count_ty;
    function_types["int_to_reg"] = int_to_reg_ty;
    function_types["str_to_reg"] = str_to_reg_ty;
    function_types["node_to_reg"] = node_to_reg_ty;
    function_types["rship_to_reg"] = rship_to_reg_ty;
    function_types["time_to_reg"] = time_to_reg_ty;

    function_types["init_grp_aggr"] = init_grp_aggr_ty;
    function_types["get_group_cnt"] = get_group_cnt_ty;
    function_types["get_total_group_cnt"] = get_total_group_cnt_ty;
    function_types["get_group_sum_int"] = get_group_sum_int_ty;
    function_types["get_group_sum_double"] = get_group_sum_double_ty;
    function_types["get_group_sum_uint"] = get_group_sum_uint_ty;

    function_types["append_to_tuple"] = append_to_tuple_ty;
    function_types["get_qr_tuple"] = get_qr_tuple_ty;

    function_types["insert_join_id_input"] = insert_join_id_input_ty;
    function_types["get_join_id_at"] = get_join_id_at_ty;

    function_types["collect_tuple_hash_join"] = collect_tuple_hash_join_ty;
    function_types["insert_join_bucket_input"] = insert_join_bucket_input_ty;
    function_types["get_hj_input_size"] = get_hj_input_size_ty;
    function_types["get_hj_input_id"] = get_hj_input_id_ty;
    function_types["get_query_result"] = get_query_result_ty;

    function_types["node_has_property"] = node_has_property_ty;
    function_types["rship_has_property"] = rship_has_property_ty;
    function_types["apply_has_property"] = apply_has_property_ty;

    function_types["get_now"] = get_now_ty;
    function_types["add_time_diff"] = add_time_diff_ty;

    function_types["end_notify"] = notifyFctTy;
    function_types["persist_tuple"] = persis_tuple_type;
}

BasicBlock *
PContext::while_loop_condition(Function *parent, Value *cond_lhs, Value *cond_rhs, WHILE_COND cond_op,
                     BasicBlock *endBB, const std::function<void(BasicBlock *, BasicBlock *)> &loop_body) {
    BasicBlock *condition = BasicBlock::Create(getContext(), "while_condition");
    BasicBlock *body = BasicBlock::Create(getContext(), "while_body");
    BasicBlock *body_epilog = BasicBlock::Create(getContext(), "while_body_epilog");
    //BasicBlock *end = BasicBlock::Create(getContext(), "while_end");

    // branch to the loop header for condition evaluation
    Builder->CreateBr(condition);

    // insert the condition header to the block list of the function
    parent->getBasicBlockList().push_back(condition);
    Builder->SetInsertPoint(condition);
    {
        // load both operands to evaluate the condition
        auto lhs = Builder->CreateLoad(cond_lhs);
        auto rhs = Builder->CreateLoad(cond_rhs);

        // choose evaluation instruction based on WHILE_COND and branch to the next block
        // branch to end when condition is not fulfilled
        switch (cond_op) {
            case EQ: {
                auto cnd = Builder->CreateICmpEQ(lhs, rhs, "cond_equal");
                Builder->CreateCondBr(cnd, body, endBB);
                break;
            }

            case UEQ: {
                auto cnd = Builder->CreateICmpEQ(lhs, rhs, "cond_unequal");
                Builder->CreateCondBr(cnd, endBB, body);
                break;
            }

            case LT: {
                auto cnd = Builder->CreateICmpULT(lhs, rhs, "cond_less_than");
                Builder->CreateCondBr(cnd, body, endBB);
                break;
            }

            case LE: {
                auto cnd = Builder->CreateICmpULE(lhs, rhs, "cond_less_equal");
                Builder->CreateCondBr(cnd, body, endBB);
                break;
            }

            case GT: {
                auto cnd = Builder->CreateICmpUGT(lhs, rhs, "cond_greater_than");
                Builder->CreateCondBr(cnd, body, endBB);
                break;
            }

            case GE: {
                auto cnd = Builder->CreateICmpUGE(lhs, rhs, "cond_greater_equal");
                Builder->CreateCondBr(cnd, body, endBB);
                break;
            }

        }
    }

    // insert loop body block to block list of function
    parent->getBasicBlockList().push_back(body);
    Builder->SetInsertPoint(body);
    {
        // process the actual loop body
        loop_body(body, body_epilog);

        //Builder->CreateBr(body_epilog);
    }

    // branch, from outside to epilog, if body leaved
    parent->getBasicBlockList().push_back(body_epilog);
    Builder->SetInsertPoint(body_epilog);
    {
        Builder->CreateBr(condition);
    }

    return condition;
}


BasicBlock *PContext::while_loop(Function *parent,
                       FunctionCallee get_begin, FunctionCallee get_next, FunctionCallee reached_end,
                       Value *it_alloca, Value *cond_param,
                       BasicBlock *nextBB, const std::function<void(BasicBlock *, BasicBlock *)> &loop_body,
                       BasicBlock *loop_body_condition) {

    BasicBlock *condition = BasicBlock::Create(*ctx_, "while_condition");
    BasicBlock *body = BasicBlock::Create(*ctx_, "while_body");
    BasicBlock *body_epilog = BasicBlock::Create(*ctx_, "while_body_epilog");
    BasicBlock *end = BasicBlock::Create(*ctx_, "while_end");

    auto cmp_false = ConstantInt::get(boolTy, 0);
    Builder->CreateBr(condition);

    parent->getBasicBlockList().push_back(condition);
    Builder->SetInsertPoint(condition);
    {
        //auto it = Builder->CreateLoad(it_alloca);
        auto end_cond = Builder->CreateCall(reached_end, {cond_param, it_alloca});
        auto cond = Builder->CreateICmpEQ(end_cond, cmp_false);
        Builder->CreateCondBr(cond, body, end);
    }

    parent->getBasicBlockList().push_back(body);
    Builder->SetInsertPoint(body);
    {
        loop_body(body, body_epilog);

        if (cond_param == nullptr)
            Builder->CreateBr(body_epilog);
    }

    parent->getBasicBlockList().push_back(body_epilog);
    Builder->SetInsertPoint(body_epilog);
    {
        //auto it = Builder->CreateLoad(it_alloca);
        it_alloca = Builder->CreateCall(get_next, {it_alloca});
        //Builder->CreateStore(nit, it_alloca);

        auto end_cond = Builder->CreateCall(reached_end, {cond_param, it_alloca});
        auto cond = Builder->CreateICmpEQ(end_cond, cmp_false);
        Builder->CreateCondBr(cond, body, end);
    }

    parent->getBasicBlockList().push_back(end);
    Builder->SetInsertPoint(end);
    {
        Builder->CreateBr(nextBB);
    }

    return body_epilog;
}

BasicBlock * PContext::while_rship_exist(Function *parent, Value *gdb, Value *node, BasicBlock *nextBB, int nodeGEPidx, int rshipGEPidx, BasicBlock *endBB, const std::function<void (BasicBlock *, BasicBlock *, Value *)> &loop_body) {
    BasicBlock *condition = BasicBlock::Create(*ctx_, "while_condition");
    BasicBlock *body = BasicBlock::Create(*ctx_, "while_body");
    BasicBlock *body_epilog = BasicBlock::Create(*ctx_, "while_body_epilog");
    BasicBlock *init_rship = BasicBlock::Create(*ctx_, "init_rship");
    BasicBlock *next_rship = BasicBlock::Create(*ctx_, "next_rship");
    BasicBlock *end = BasicBlock::Create(*ctx_, "while_end");

    FunctionCallee rship_by_id = extern_func("rship_by_id");
    Value *rship_id = Builder->CreateLoad(Builder->CreateStructGEP(node, nodeGEPidx));
    auto UNKNOWN_REL_ID = ConstantInt::get(int64Ty, std::numeric_limits<int64_t>::max());
    Value *rship;
    //auto cmp_false = ConstantInt::get(boolTy, 0);
    Builder->CreateBr(condition);

    parent->getBasicBlockList().push_back(condition);
    Builder->SetInsertPoint(condition);
    {
        //auto it = Builder->CreateLoad(it_alloca);
        auto cond = Builder->CreateICmpULT(rship_id, UNKNOWN_REL_ID);
        Builder->CreateCondBr(cond, init_rship, end);
    }

    parent->getBasicBlockList().push_back(init_rship);
    Builder->SetInsertPoint(init_rship);
    {
        //auto it = Builder->CreateLoad(it_alloca);
        rship = Builder->CreateCall(rship_by_id, {gdb, rship_id});
        Builder->CreateBr(body);
    }

    parent->getBasicBlockList().push_back(body);
    Builder->SetInsertPoint(body);
    {
        loop_body(body, body_epilog, rship);

        //Builder->CreateBr(body_epilog);
    }

    parent->getBasicBlockList().push_back(body_epilog);
    Builder->SetInsertPoint(body_epilog);
    {
        //auto it = Builder->CreateLoad(it_alloca);
        rship_id = Builder->CreateLoad(Builder->CreateStructGEP(rship, rshipGEPidx));
        //Builder->CreateStore(nit, it_alloca);

        auto end_cond = Builder->CreateICmpULT(rship_id, UNKNOWN_REL_ID);
        //auto cond = Builder->CreateICmpEQ(end_cond, cmp_false);
        Builder->CreateCondBr(end_cond, next_rship, end);
    }

    parent->getBasicBlockList().push_back(next_rship);
    Builder->SetInsertPoint(next_rship);
    {
        auto nrship = Builder->CreateLoad(Builder->CreateCall(rship_by_id, {gdb, rship_id}));
        Builder->CreateStore(nrship, rship);
        Builder->CreateBr(body);
    }

    parent->getBasicBlockList().push_back(end);
    Builder->SetInsertPoint(end);
    {
        Builder->CreateBr(nextBB);
    }

    return body_epilog;
}

Value *PContext::node_cmp_label(Value *node, Value *label_code) {
    auto node_lc = getBuilder().CreateLoad(
            getBuilder().CreateStructGEP(node, 5));
    return getBuilder().CreateICmpEQ(node_lc, label_code);
};

Value *PContext::rship_cmp_label(Value *rship, Value *label_code) {
    auto rship_lcode = getBuilder().CreateLoad(
            getBuilder().CreateStructGEP(rship, 7));
    return getBuilder().CreateICmpEQ(rship_lcode, label_code);
}

Value *PContext::extract_arg_label(int op_id, Value *gdb, Value *arg_desc) {
    FunctionCallee dict_lookup_label = extern_func("dict_lookup_label");
    auto opid = ConstantInt::get(int64Ty, op_id);
    auto str = getBuilder().CreateLoad(Builder->CreateInBoundsGEP(arg_desc, {LLVM_ZERO, opid}));
    // 2.2 call extern function
    return Builder->CreateCall(dict_lookup_label, {gdb, Builder->CreateBitCast(str, int8PtrTy)});
}
