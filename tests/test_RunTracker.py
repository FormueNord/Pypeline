from Pypeline.RunTracker import RunTracker
import os
from datetime import timedelta, datetime

RunTracker._tracking_file_path = os.path.join(os.path.dirname(__file__),"Test tracking data.pickle")
pipeline_name = "pickle_cant_be_empty"
field = "last trigger"
# if test_schedule_interval is changed the .replace method in test_update_scheduler should also be updated
test_schedule_interval = timedelta(minutes = 10)

def test_RunTracker_init():
    try:
        tracker = RunTracker({field:{"interval": test_schedule_interval}})
    except Exception as e:
        assert False, f"Failed to initialize RunTracker. Init threw the following error: {e}"
    return tracker

tracker = test_RunTracker_init()

def test_update():
    try:
        tracker.update(pipeline_name,field)
    except Exception as e:
        assert False, f"tracker failed to update. Threw the following exception: {e}"
    time = datetime.now()

    # very low probability that both would pass and still be caused by an error
    check_minute = tracker.tracking_data[pipeline_name][field].minute == time.minute 
    check_second = tracker.tracking_data[pipeline_name][field].second == time.second
    assert check_minute and check_second



def test_update_scheduler():
    # for some reason the interval is can be None - this is a quickfix
    tracker.tracking_data[pipeline_name]["interval"] = test_schedule_interval
    temp_schedule_time = datetime(2000,1,1,20,00,00,00)
    tracker.tracking_data[pipeline_name]["schedule"] = temp_schedule_time
    try:
        tracker.update_scheduler(pipeline_name)
    except Exception as e:
        assert False, f"tracker failed to update. Threw the following exception: {e}"
    
    expected_time = datetime.now()
    expected_time += test_schedule_interval
    # floor the expected minute
    expected_minute = expected_time.minute - expected_time.minute % 10
    expected_time = expected_time.replace(minute = expected_minute ,second = 0, microsecond= 0)

    assert tracker.tracking_data[pipeline_name]["schedule"] == expected_time, "The time set by update_scheduler was not as expected. Is there a time drift?"

