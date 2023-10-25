#include <llvm/ExecutionEngine/Orc/CompileUtils.h>
#include <llvm/ExecutionEngine/Orc/ExecutionUtils.h>
#include <llvm/ExecutionEngine/Orc/ThreadSafeModule.h>
#include <llvm/ExecutionEngine/Orc/Core.h>
#include <llvm/ExecutionEngine/SectionMemoryManager.h>
#include <llvm/IR/Mangler.h>
#include <llvm/Support/Debug.h>
#include <llvm/Support/raw_ostream.h>
#include <llvm/Bitcode/BitcodeWriter.h>
#include <llvm/IR/LegacyPassManager.h>
#include <llvm/IR/PassManager.h>
#include <llvm/Support/Debug.h>
#include <llvm/Support/raw_ostream.h>
#include <llvm/Transforms/InstCombine/InstCombine.h>
#include <llvm/Transforms/Scalar.h>
#include <llvm/Transforms/Scalar/GVN.h>
#include <llvm/Transforms/Utils.h>
#include <llvm/Transforms/IPO.h>
#include <llvm/Transforms/AggressiveInstCombine/AggressiveInstCombine.h>
#include <llvm/Transforms/Vectorize.h>
#include <llvm/CodeGen/Passes.h>
#include <llvm/Analysis/CallGraphSCCPass.h>
#include <cstdio>
#include "p_jit.hpp"
#include "global_definitions.hpp"

using namespace llvm;
using namespace llvm::orc;

Expected<ThreadSafeModule>
ir_optimizer::operator()(ThreadSafeModule TSM,
                      const MaterializationResponsibility &) {
    Module &M = *TSM.getModuleUnlocked();

    //M.dump();

    legacy::FunctionPassManager FPM(&M);
    FPM.add(createPromoteMemoryToRegisterPass());
    FPM.add(createCFGSimplificationPass());
    FPM.add(createLCSSAPass());
    FPM.add(createLoopDeletionPass());
    FPM.add(createDeadStoreEliminationPass());
    
    FPM.add(createInstructionCombiningPass());
    B.populateFunctionPassManager(FPM);
    FPM.doInitialization();

    for (Function &F : M)
        FPM.run(F);
    FPM.doFinalization();

    legacy::PassManager MPM;
    B.populateModulePassManager(MPM);
    
std::error_code EC;
llvm::raw_fd_ostream OS("module", EC, llvm::sys::fs::F_None);
WriteBitcodeToFile(M, OS);
OS.flush();

    return std::move(TSM);
}

p_jit::p_jit(ExitOnError ExitOnErr)
        : ctx(std::make_unique<LLVMContext>()),
          ES(std::make_unique<ExecutionSession>()),
          TM(createTargetMachine(ExitOnErr)),
          E_ERR(ExitOnErr),
          //GDBListener(JITEventListener::createGDBRegistrationListener()),
          ObjLinkingLayer(*ES, createMemoryManagerFtor()),
#if USE_PMDK
          ObjCache(std::make_unique<PJitObjectCache>("/mnt/pmem0/jit_cache")),
#else
        ObjCache(std::make_unique<PJitObjectCache>()),
#endif
#if USE_CACHE
          CompileLayer(*ES, ObjLinkingLayer, std::make_unique<SimpleCompiler>(*TM, ObjCache.get())),
#else
          CompileLayer(*ES, ObjLinkingLayer, std::make_unique<SimpleCompiler>(*TM)),
#endif
          OptimizeLayer(*ES, CompileLayer),
          MainJD(ES->createBareJITDylib("main")) {

        auto dl = getDataLayout();
                cantFail(DynamicLibrarySearchGenerator::GetForCurrentProcess(
        dl.getGlobalPrefix()));
            
        SymbolMap M;
        MangleAndInterner Mangle(*ES, dl);

        // Register every symbol that can be accessed from the JIT'ed code.
        M[Mangle("vec_end_reached")] = JITEvaluatedSymbol(
                pointerToJITTargetAddress(&vec_end_reached), JITSymbolFlags::Exported);
        M[Mangle("get_vec_begin")] = JITEvaluatedSymbol(
                pointerToJITTargetAddress(&get_vec_begin), JITSymbolFlags::Exported);
        M[Mangle("get_vec_next")] = JITEvaluatedSymbol(
                pointerToJITTargetAddress(&get_vec_next), JITSymbolFlags::Exported);
        M[Mangle("dict_lookup_label")] = JITEvaluatedSymbol(
                pointerToJITTargetAddress(&dict_lookup_label), JITSymbolFlags::Exported);
        M[Mangle("gdb_get_nodes")] = JITEvaluatedSymbol(
                pointerToJITTargetAddress(&gdb_get_nodes), JITSymbolFlags::Exported);
        M[Mangle("node_by_id")] = JITEvaluatedSymbol(
                pointerToJITTargetAddress(&node_by_id), JITSymbolFlags::Absolute);
        M[Mangle("get_node_from_it")] = JITEvaluatedSymbol(
                pointerToJITTargetAddress(&get_node_from_it), JITSymbolFlags::Exported);
        M[Mangle("gdb_get_rships")] = JITEvaluatedSymbol(
                pointerToJITTargetAddress(&gdb_get_rships), JITSymbolFlags::Exported);
        M[Mangle("get_rship_from_it")] = JITEvaluatedSymbol(
                pointerToJITTargetAddress(&get_rship_from_it), JITSymbolFlags::Exported);
        M[Mangle("get_vec_begin_r")] = JITEvaluatedSymbol(
                pointerToJITTargetAddress(&get_vec_begin_r), JITSymbolFlags::Exported);
        M[Mangle("get_vec_next_r")] = JITEvaluatedSymbol(
                pointerToJITTargetAddress(&get_vec_next_r), JITSymbolFlags::Exported);
        M[Mangle("vec_end_reached_r")] = JITEvaluatedSymbol(
                pointerToJITTargetAddress(&vec_end_reached_r), JITSymbolFlags::Exported);
        M[Mangle("rship_by_id")] = JITEvaluatedSymbol(
                pointerToJITTargetAddress(&rship_by_id), JITSymbolFlags::Exported);
        M[Mangle("gdb_get_dcode")] = JITEvaluatedSymbol(
                pointerToJITTargetAddress(&gdb_get_dcode), JITSymbolFlags::Exported);
        M[Mangle("pset_get_item_at")] = JITEvaluatedSymbol(
                pointerToJITTargetAddress(&pset_get_item_at), JITSymbolFlags::Exported);
        M[Mangle("get_tx")] = JITEvaluatedSymbol(
                pointerToJITTargetAddress(&get_tx), JITSymbolFlags::Exported);
        M[Mangle("get_valid_node")] = JITEvaluatedSymbol(
                pointerToJITTargetAddress(&get_valid_node), JITSymbolFlags::Exported);
        M[Mangle("apply_pexpr")] = JITEvaluatedSymbol(
                pointerToJITTargetAddress(&apply_pexpr), JITSymbolFlags::Exported);
        M[Mangle("dict_lookup_dcode")] = JITEvaluatedSymbol(
                pointerToJITTargetAddress(&lookup_dc), JITSymbolFlags::Exported);
        M[Mangle("create_node")] = JITEvaluatedSymbol(
                pointerToJITTargetAddress(&create_node_func), JITSymbolFlags::Exported);
        M[Mangle("create_ship")] = JITEvaluatedSymbol(
                pointerToJITTargetAddress(&create_rship_func), JITSymbolFlags::Exported);
        M[Mangle("mat_reg_value")] = JITEvaluatedSymbol(
                pointerToJITTargetAddress(&mat_reg_value), JITSymbolFlags::Exported);
        M[Mangle("collect_tuple")] = JITEvaluatedSymbol(
                pointerToJITTargetAddress(&collect_tuple), JITSymbolFlags::Exported);
        M[Mangle("obtain_mat_tuple")] = JITEvaluatedSymbol(
                pointerToJITTargetAddress(&obtain_mat_tuple), JITSymbolFlags::Exported);
        M[Mangle("reg_to_qres")] = JITEvaluatedSymbol(
                pointerToJITTargetAddress(&reg_to_qres), JITSymbolFlags::Exported);
        M[Mangle("mat_node")] = JITEvaluatedSymbol(
                pointerToJITTargetAddress(&mat_node), JITSymbolFlags::Exported);
        M[Mangle("mat_rship")] = JITEvaluatedSymbol(
                pointerToJITTargetAddress(&mat_rship), JITSymbolFlags::Exported);
        M[Mangle("collect_tuple_join")] = JITEvaluatedSymbol(
                pointerToJITTargetAddress(&collect_tuple_join), JITSymbolFlags::Exported);
        M[Mangle("get_join_tp_at")] = JITEvaluatedSymbol(
                pointerToJITTargetAddress(&get_join_tp_at), JITSymbolFlags::Exported);
        M[Mangle("get_node_res_at")] = JITEvaluatedSymbol(
                pointerToJITTargetAddress(&get_node_res_at), JITSymbolFlags::Exported);
        M[Mangle("get_rship_res_at")] = JITEvaluatedSymbol(
                pointerToJITTargetAddress(&get_rship_res_at), JITSymbolFlags::Exported);
        M[Mangle("get_mat_res_size")] = JITEvaluatedSymbol(
                pointerToJITTargetAddress(&get_mat_res_size), JITSymbolFlags::Exported);
        M[Mangle("index_get_node")] = JITEvaluatedSymbol(
                pointerToJITTargetAddress(&index_get_node), JITSymbolFlags::Exported);
        M[Mangle("apply_pexpr_node")] = JITEvaluatedSymbol(
                pointerToJITTargetAddress(&apply_pexpr_node), JITSymbolFlags::Exported);
        M[Mangle("apply_pexpr_rship")] = JITEvaluatedSymbol(
                pointerToJITTargetAddress(&apply_pexpr_rship), JITSymbolFlags::Exported);
        M[Mangle("retrieve_fev_queue")] = JITEvaluatedSymbol(
                pointerToJITTargetAddress(&retrieve_fev_queue), JITSymbolFlags::Exported);            
        M[Mangle("insert_fev_rship")] = JITEvaluatedSymbol(
                pointerToJITTargetAddress(&insert_fev_rship), JITSymbolFlags::Exported);            
        M[Mangle("foreach_from_variable_rship")] = JITEvaluatedSymbol(
                pointerToJITTargetAddress(&foreach_from_variable_rship), JITSymbolFlags::Exported);            
        M[Mangle("get_next_rship_fev")] = JITEvaluatedSymbol(
                pointerToJITTargetAddress(&get_next_rship_fev), JITSymbolFlags::Exported);            
        M[Mangle("fev_list_end")] = JITEvaluatedSymbol(
                pointerToJITTargetAddress(&fev_list_end), JITSymbolFlags::Exported);  
        M[Mangle("fev_list_end")] = JITEvaluatedSymbol(
                pointerToJITTargetAddress(&fev_list_end), JITSymbolFlags::Exported);            
        M[Mangle("get_node_grpkey")] = JITEvaluatedSymbol(
                pointerToJITTargetAddress(&get_node_grpkey), JITSymbolFlags::Exported);  
        M[Mangle("get_rship_grpkey")] = JITEvaluatedSymbol(
                pointerToJITTargetAddress(&get_rship_grpkey), JITSymbolFlags::Exported);  
        M[Mangle("get_int_grpkey")] = JITEvaluatedSymbol(
                pointerToJITTargetAddress(&get_int_grpkey), JITSymbolFlags::Exported);  
        M[Mangle("get_string_grpkey")] = JITEvaluatedSymbol(
                pointerToJITTargetAddress(&get_string_grpkey), JITSymbolFlags::Exported);  
        M[Mangle("get_time_grpkey")] = JITEvaluatedSymbol(
                pointerToJITTargetAddress(&get_time_grpkey), JITSymbolFlags::Exported);
        M[Mangle("add_to_group")] = JITEvaluatedSymbol(
                pointerToJITTargetAddress(&add_to_group), JITSymbolFlags::Exported);  
        M[Mangle("finish_group_by")] = JITEvaluatedSymbol(
                pointerToJITTargetAddress(&finish_group_by), JITSymbolFlags::Exported); 
        M[Mangle("clear_mat_tuple")] = JITEvaluatedSymbol(
                pointerToJITTargetAddress(&clear_mat_tuple), JITSymbolFlags::Exported);
        M[Mangle("int_to_reg")] = JITEvaluatedSymbol(
                pointerToJITTargetAddress(&int_to_reg), JITSymbolFlags::Exported);
        M[Mangle("str_to_reg")] = JITEvaluatedSymbol(
                pointerToJITTargetAddress(&str_to_reg), JITSymbolFlags::Exported);
        M[Mangle("node_to_reg")] = JITEvaluatedSymbol(
                pointerToJITTargetAddress(&node_to_reg), JITSymbolFlags::Exported);
        M[Mangle("rship_to_reg")] = JITEvaluatedSymbol(
                pointerToJITTargetAddress(&rship_to_reg), JITSymbolFlags::Exported);
        M[Mangle("time_to_reg")] = JITEvaluatedSymbol(
                pointerToJITTargetAddress(&time_to_reg), JITSymbolFlags::Exported);  
        M[Mangle("grp_demat_at")] = JITEvaluatedSymbol(
                pointerToJITTargetAddress(&grp_demat_at), JITSymbolFlags::Exported);
        M[Mangle("get_grp_rs_count")] = JITEvaluatedSymbol(
                pointerToJITTargetAddress(&get_grp_rs_count), JITSymbolFlags::Exported);  
        M[Mangle("get_group_cnt")] = JITEvaluatedSymbol(
                pointerToJITTargetAddress(&get_group_count), JITSymbolFlags::Exported);
        M[Mangle("init_grp_aggr")] = JITEvaluatedSymbol(
                pointerToJITTargetAddress(&init_grp_aggr), JITSymbolFlags::Exported);
        M[Mangle("get_group_sum_int")] = JITEvaluatedSymbol(
                pointerToJITTargetAddress(&get_group_sum_int), JITSymbolFlags::Exported);
        M[Mangle("get_group_sum_double")] = JITEvaluatedSymbol(
                pointerToJITTargetAddress(&get_group_sum_double), JITSymbolFlags::Exported);
        M[Mangle("get_group_sum_uint")] = JITEvaluatedSymbol(
                pointerToJITTargetAddress(&get_group_sum_uint), JITSymbolFlags::Exported);
        M[Mangle("get_total_group_cnt")] = JITEvaluatedSymbol(
                pointerToJITTargetAddress(&get_total_group_count), JITSymbolFlags::Exported);
        M[Mangle("append_to_tuple")] = JITEvaluatedSymbol(
                pointerToJITTargetAddress(&append_to_tuple), JITSymbolFlags::Exported);
        M[Mangle("get_qr_tuple")] = JITEvaluatedSymbol(
                pointerToJITTargetAddress(&get_qr_tuple), JITSymbolFlags::Exported);
        M[Mangle("insert_join_id_input")] = JITEvaluatedSymbol(
                pointerToJITTargetAddress(&insert_join_id_input), JITSymbolFlags::Exported);
        M[Mangle("get_join_id_at")] = JITEvaluatedSymbol(
                pointerToJITTargetAddress(&get_join_id_at), JITSymbolFlags::Exported);
        M[Mangle("collect_tuple_hash_join")] = JITEvaluatedSymbol(
                pointerToJITTargetAddress(&collect_tuple_hash_join), JITSymbolFlags::Exported);
        M[Mangle("insert_join_bucket_input")] = JITEvaluatedSymbol(
                pointerToJITTargetAddress(&insert_join_bucket_input), JITSymbolFlags::Exported);
        M[Mangle("get_hj_input_size")] = JITEvaluatedSymbol(
                pointerToJITTargetAddress(&get_hj_input_size), JITSymbolFlags::Exported);
        M[Mangle("get_hj_input_id")] = JITEvaluatedSymbol(
                pointerToJITTargetAddress(&get_hj_input_id), JITSymbolFlags::Exported);
        M[Mangle("get_query_result")] = JITEvaluatedSymbol(
                pointerToJITTargetAddress(&get_query_result), JITSymbolFlags::Exported);
        M[Mangle("node_has_property")] = JITEvaluatedSymbol(
                pointerToJITTargetAddress(&node_has_property), JITSymbolFlags::Exported);
        M[Mangle("rship_has_property")] = JITEvaluatedSymbol(
                pointerToJITTargetAddress(&rship_has_property), JITSymbolFlags::Exported);
        M[Mangle("apply_has_property")] = JITEvaluatedSymbol(
                pointerToJITTargetAddress(&apply_has_property), JITSymbolFlags::Exported);
        M[Mangle("get_now")] = JITEvaluatedSymbol(
                pointerToJITTargetAddress(&get_now), JITSymbolFlags::Exported);
        M[Mangle("add_time_diff")] = JITEvaluatedSymbol(
                pointerToJITTargetAddress(&add_time_diff), JITSymbolFlags::Exported);
        M[Mangle("end_notify")] = JITEvaluatedSymbol(
                pointerToJITTargetAddress(&end_notify), JITSymbolFlags::Exported);
        M[Mangle("node_to_description")] = JITEvaluatedSymbol(
                pointerToJITTargetAddress(&node_to_description), JITSymbolFlags::Exported);
        M[Mangle("print_int")] = JITEvaluatedSymbol(
                pointerToJITTargetAddress(&print_int), JITSymbolFlags::Exported);

        ExitOnErr(MainJD.define(absoluteSymbols(M)));
}

p_jit::~p_jit() {
    if(auto err = ES->endSession())
        ES->reportError(std::move(err));
}


Error p_jit::addModule(std::unique_ptr<Module> M) {
#if USE_CACHE
    auto obj = ObjCache->getCachedObject(*M);
    if(!obj) {
        M.~unique_ptr();
        return obj.takeError();
    }


    if(obj->hasValue()) {
        M.~unique_ptr();
        return ObjLinkingLayer.add(*ES->getJITDylibByName("Main"), std::move(obj->getValue()));
    }
#endif

    OptimizeLayer.setTransform(ir_optimizer(3));
    
    auto RT = MainJD.getDefaultResourceTracker();

    return OptimizeLayer.add(RT, ThreadSafeModule(std::move(M), ctx));

    //return OptimizeLayer.add(*ES->getJITDylibByName("Main"), ThreadSafeModule(std::move(M), ctx), K);
    //cantFail(CompileLayer.add(*ES->getJITDylibByName("Main"), ThreadSafeModule(std::move(M), ctx)));
}


std::unique_ptr<TargetMachine> p_jit::createTargetMachine(llvm::ExitOnError ExitOnErr) {
    auto JTMP = ExitOnErr(JITTargetMachineBuilder::detectHost());
	
    auto tm = JTMP.createTargetMachine();
    if(tm) {
        tm->get()->setFastISel(true);
	}
    return ExitOnErr(std::move(tm));
}

using GetMemoryManagerFunction =
RTDyldObjectLinkingLayer::GetMemoryManagerFunction;

GetMemoryManagerFunction p_jit::createMemoryManagerFtor() {
    return []() -> GetMemoryManagerFunction::result_type {
        return std::make_unique<SectionMemoryManager>();
    };
}

std::string p_jit::mangle(llvm::StringRef UnmangledName) {
    std::string MangledName;
    {
        DataLayout DL = getDataLayout();
        raw_string_ostream MangledNameStream(MangledName);
        Mangler::getNameWithPrefix(MangledNameStream, UnmangledName, DL);
    }
    return MangledName;
}

Error p_jit::applyDataLayout(llvm::Module &M) {
    DataLayout DL = TM->createDataLayout();
    if(M.getDataLayout().isDefault())
        M.setDataLayout(DL);

    if(M.getDataLayout() != DL)
        return make_error<StringError>(
                "Added modules have incompatible data layouts",
                inconvertibleErrorCode());
    return Error::success();
}

Expected<JITTargetAddress> p_jit::getFunctionAddr(llvm::StringRef Name) {
    SymbolStringPtr NamePtr = ES->intern(mangle(Name));
    JITDylibSearchOrder JDs{{&MainJD, JITDylibLookupFlags::MatchAllSymbols}};
    Expected<JITEvaluatedSymbol> S = ES->lookup(JDs, NamePtr);
    if(!S)
        return S.takeError();
    JITTargetAddress A = S->getAddress();

    return A;
}

std::unique_ptr<DynamicLibrarySearchGenerator> p_jit::createHostProcessResolver() {
    char Prefix = TM->createDataLayout().getGlobalPrefix();
    auto R = DynamicLibrarySearchGenerator::GetForCurrentProcess(Prefix);
    if(!R) {
        ES->reportError(R.takeError());
        return nullptr;
    }

    if(!*R) {
        return nullptr;
    }

    return std::move(*R);
}
