  # ========================================================================== #
  # Peristent Memory Development Kit (pmem.io)                                 #
  # ========================================================================== #

if(USE_PMDK MATCHES ON)
 message(STATUS "Using PMDK")
find_package(PkgConfig REQUIRED)
  message(STATUS "Searching for Intel PMDK")
  find_path(PMDK_INCLUDE_DIR libpmem.h)
  pkg_check_modules(PMDK REQUIRED libpmemobj++>=1.5)
  set(PMDK_INCLUDE_DIRS ${PMDK_INCLUDE_DIRS} ${PMDK_INCLUDE_DIR})
  if(NOT PMDK_INCLUDE_DIRS OR "${PMDK_INCLUDE_DIRS}" STREQUAL "")
    message(FATAL_ERROR "ERROR: libpmem include directory not found.")
  endif()
  message(STATUS "  libpmem.h found in ${PMDK_INCLUDE_DIRS}")
  add_definitions( "-DUSE_PMDK" )
  mark_as_advanced(PMDK_LIBRARIES PMDK_INCLUDE_DIRS)
endif()


if(USE_PMDK_TX_FA  MATCHES ON )

   if(NOT USE_PMDK MATCHES ON)	
	 message(FATAL_ERROR "Flag: USE_PMDK_TX_FA is ON. Hence USE_PMDK should also be turned ON. " 
	         "If USE_PMDK_TX_FA not needed, then turn it OFF explicitly or remove CMakeCache ")
    endif()

     message(STATUS "Using PMDK Transactions for FA")
     add_definitions("-DUSE_PMDK_TXN_FA")
endif()
