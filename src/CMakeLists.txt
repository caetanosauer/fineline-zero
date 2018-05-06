macro(INSTALL_GENERATED_H)
  set(ig_tmp)
  foreach(tmp ${ARGV})
    list(APPEND ig_tmp ${CMAKE_CURRENT_BINARY_DIR}/${tmp})
  endforeach(tmp)
  install(FILES ${ig_tmp} DESTINATION include)
endmacro(INSTALL_GENERATED_H)

INCLUDE_DIRECTORIES (
  ${CMAKE_SOURCE_DIR}/config
  ${CMAKE_SOURCE_DIR}/src/common
  ${CMAKE_SOURCE_DIR}/src/sm
  ${CMAKE_SOURCE_DIR}/src/cmd
  ${CMAKE_SOURCE_DIR}/finelog/src
)
# to look for generated headers
INCLUDE_DIRECTORIES (
  ${CMAKE_BINARY_DIR}/config
  ${CMAKE_BINARY_DIR}/src/common
  ${CMAKE_BINARY_DIR}/src/sm
  ${CMAKE_BINARY_DIR}/src/cmd
  ${CMAKE_SOURCE_DIR}/finelog/src
)

add_subdirectory(common)
add_subdirectory(sm)
add_subdirectory(cmd)
