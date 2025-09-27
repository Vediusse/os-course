add_executable(vtsh main.c)
target_include_directories(vtsh PUBLIC .)
target_link_libraries(vtsh PRIVATE libvtsh)

add_executable(proc_clone proc_clone.c)
add_executable(cpu cpu-calc-crc.c)
add_executable(ema-traverse-graph ema-traverse-graph.c)
