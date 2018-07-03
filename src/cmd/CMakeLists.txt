INCLUDE_DIRECTORIES (
  ${CMAKE_SOURCE_DIR}/src/cmd/base
  ${CMAKE_SOURCE_DIR}/src/cmd/restore
  ${CMAKE_SOURCE_DIR}/src/cmd/kits
  ${CMAKE_SOURCE_DIR}/src/cmd/webserver
  ${CMAKE_SOURCE_DIR}/src/cmd/loginspect
  ${CMAKE_SOURCE_DIR}/config
  ${CMAKE_SOURCE_DIR}/src/sm
  ${CMAKE_SOURCE_DIR}/src/common
)
# to look for generated headers
INCLUDE_DIRECTORIES (
  ${CMAKE_BINARY_DIR}/config
  ${CMAKE_BINARY_DIR}/config
  ${CMAKE_BINARY_DIR}/src/sm
  ${CMAKE_BINARY_DIR}/src/common
)

add_subdirectory(base)
add_subdirectory(restore)
add_subdirectory(kits)
add_subdirectory(webserver)
add_subdirectory(loginspect)

add_executable(zapps main.cpp)

if (USE_LEVELDB)
   # TODO this should not be hard coded!
   # add_library(leveldb STATIC "/home/csauer/dev/leveldb/build/libleveldb.a")
   set (LEVELDB_LIB "/home/csauer/dev/leveldb/build/libleveldb.a")
   # target_link_libraries(leveldb /home/csauer/dev/leveldb/build/libleveldb.a)
else ()
   set (LEVELDB_LIB "/home/csauer/dev/leveldb/build/libleveldb.a")
endif ()

set(zapps_LIBS
    # zapps components
    zapps_base
    kits
    restore
    loginspect
    # Zero/Shore libraries
    sm
    common
    # third-party dependencies
    pthread
    ${LEVELDB_LIB}
)

if (USE_LEVELDB)
   # set(zapps_LIBS ${zapps_LIBS} leveldb)
endif(USE_LEVELDB)

target_link_libraries(zapps ${zapps_LIBS})
