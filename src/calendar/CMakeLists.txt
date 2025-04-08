add_subdirectory(calendars)
add_subdirectory(holidays)

target_sources(epoch_frame PRIVATE calendar_utils.cpp market_calendar.cpp calendar_factory.cpp)
