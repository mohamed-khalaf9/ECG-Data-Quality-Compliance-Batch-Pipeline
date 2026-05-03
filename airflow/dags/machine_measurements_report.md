# Machine Measurements Profiling Report

## Overview
- **Rows:** 50000
- **Columns:** 33

**Data types:**
 subject_id       int64
study_id         int64
cart_id          int64
ecg_time        object
report_0        object
report_1        object
report_2        object
report_3        object
report_4        object
report_5        object
report_6        object
report_7        object
report_8        object
report_9        object
report_10       object
report_11       object
report_12       object
report_13       object
report_14       object
report_15       object
report_16      float64
report_17      float64
bandwidth       object
filtering       object
rr_interval      int64
p_onset          int64
p_end            int64
qrs_onset        int64
qrs_end          int64
t_end            int64
p_axis           int64
qrs_axis         int64
t_axis           int64
dtype: object

## subject_id
- **nulls:** 0
- **format valid (digits only):** True
- **duplicates count:** 39717

## study_id
- **nulls:** 0
- **format valid (digits only):** True
- **unique check:** True
- **duplicates count:** 0
- **unique ratio:** 1.0

## cart_id
- **nulls:** 0
- **format valid (digits only):** True
- **unique machines available:** 150

## ecg_time
- **nulls after parsing:** 0
- **format valid:** True
- **cart_id + ecg_time duplicate scans:** 149

## reports
**Null coverage per report column:**
report_0         0
report_1     12568
report_2     17189
report_3     25437
report_4     35899
report_5     42415
report_6     46471
report_7     48559
report_8     49514
report_9     49839
report_10    49937
report_11    49973
report_12    49989
report_13    49991
report_14    49997
report_15    49999
report_16    50000
report_17    50000
dtype: int64

## bandwidth
- **nulls:** 0
- **format valid:** True
- **unique values:** 3
**Top 5 values:**
 bandwidth
0.005-150 Hz     39773
0.0005-150 Hz     7073
0.05-150 Hz       3154
Name: count, dtype: int64

## filtering
- **nulls:** 0
- **unique values:** 4
**Top 5 values:**
 filtering_clean
60 hz notch baseline filter    42308
<not specified>                 6807
baseline filter                  756
50 hz notch baseline filter      129
Name: count, dtype: int64

## measurements (overview)

### rr_interval
- **nulls:** 0
- **min:** 0
- **max:** 29999
- **negative values:** 0

### p_onset
- **nulls:** 0
- **min:** 0
- **max:** 29999
- **negative values:** 0

### p_end
- **nulls:** 0
- **min:** 0
- **max:** 30944
- **negative values:** 0

### qrs_onset
- **nulls:** 0
- **min:** 0
- **max:** 29999
- **negative values:** 0

### qrs_end
- **nulls:** 0
- **min:** 0
- **max:** 55040
- **negative values:** 0

### t_end
- **nulls:** 0
- **min:** 280
- **max:** 29999
- **negative values:** 0

### p_axis
- **nulls:** 0
- **min:** -21846
- **max:** 32767

### qrs_axis
- **nulls:** 0
- **min:** -180
- **max:** 29999

### t_axis
- **nulls:** 0
- **min:** -32768
- **max:** 32767

## logical order checks
- **sequence violations:** 14716

## derived duration checks
- **negative p_duration:** 5
- **negative qrs_duration:** 3
- **negative qt_proxy:** 0
- **p_duration > rr_interval:** 6772
- **qrs_duration > rr_interval:** 14
- **qt_proxy > rr_interval:** 12
