# Find all the Python files with test_* in them
FILE(GLOB PY_TEST_FILES "test*.py")

FUNCTION(makePyTest filename isSafemode)
    GET_FILENAME_COMPONENT(test_name ${filename} NAME_WE)
    SET(ctestName "PY_${test_name}")
    SET(commonEnv "REDIS_MODULE_PATH=$<TARGET_FILE:redisearch>;EXT_TEST_PATH=$<TARGET_FILE:example_extension>")
    set(commonEnv "${commonEnv};RMTEST_CONFIG_DIR=${PROJECT_BINARY_DIR}")
    # SET(commonEnv "${commonEnv};PYTHONPATH=${CURRENT_SOURCE_DIR}")
    
    IF(isSafemode)
        SET(ctestName "${ctestName}_SAFEMODE")
        SET(commonEnv "${commonEnv};RS_TEST_SAFEMODE=1")
    ENDIF(isSafemode)

    ADD_TEST(NAME "${ctestName}"
        COMMAND "python" "-m" "unittest" "${test_name}"
        WORKING_DIRECTORY "${CMAKE_CURRENT_SOURCE_DIR}")

    SET_TESTS_PROPERTIES("${ctestName}" PROPERTIES ENVIRONMENT "${commonEnv}")
    MESSAGE("Addest test ${ctestName}")
ENDFUNCTION(makePyTest)

FOREACH(n ${PY_TEST_FILES})
    makePyTest(${n} 0)
    makePyTest(${n} 1)
ENDFOREACH()
