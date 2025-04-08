add_executable(epoch_frame_cal_test
cboe_calendar_test.cpp
market_calendar_test.cpp
nyse_calendar_test.cpp
ice_calendar_test.cpp
cme_equity_calendar_test.cpp
cme_agriculture_calendar_test.cpp
cme_bond_calendar_test.cpp
cme_globex_fx_calendar_test.cpp
cme_globex_fixed_income_calendar_test.cpp
cme_globex_equities_calendar_test.cpp
cme_globex_crypto_calendar_test.cpp cme_globex_energy_and_metals_calendar_test.cpp
cme_globex_grains_calendar_test.cpp
fx_calendar_test.cpp
crypto_calendar_test.cpp
)

target_link_libraries(epoch_frame_cal_test
        PRIVATE
        Catch2::Catch2 Catch2::Catch2WithMain trompeloeil::trompeloeil
        PUBLIC epoch_frame)

target_include_directories(epoch_frame_cal_test
        PRIVATE ../../src)

target_compile_definitions(epoch_frame_cal_test PRIVATE -DEPOCH_FRAME_S3_TEST_BUCKET="epoch-test-files/epoch_frame")
