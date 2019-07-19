ADD_EXECUTABLE (simple-test
	main.cpp
)

TARGET_LINK_LIBRARIES (simple-test
	clickhouse-cpp-lib
)

cotire(simple-test)

ADD_EXECUTABLE (csimple-test
	cmain.c 
)

TARGET_LINK_LIBRARIES(csimple-test
	clickhouse-cpp-lib
)

cotire(csimple-test)