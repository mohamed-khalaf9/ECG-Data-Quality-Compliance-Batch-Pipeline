# Clean Machine Measurements Profiling Report

## Overview
- **Rows:** 35184
- **Columns:** 36

**Data types:**
 subject_id        int64
study_id          int64
cart_id           int64
ecg_time          int64
report_0         object
report_1         object
report_2         object
report_3         object
report_4         object
report_5         object
report_6         object
report_7         object
report_8         object
report_9         object
report_10        object
report_11        object
report_12        object
report_13        object
report_14        object
report_15        object
report_16       float64
report_17       float64
bandwidth        object
filtering        object
rr_interval       int64
p_onset           int64
p_end             int64
qrs_onset         int64
qrs_end           int64
t_end             int64
p_axis            int64
qrs_axis          int64
t_axis            int64
p_duration        int64
qrs_duration      int64
qt_proxy          int64
dtype: object

## subject_id
- **nulls:** 0
- **format valid:** True

## study_id
- **nulls:** 0
- **format valid:** True
- **unique:** True
- **duplicates:** 0

## cart_id
- **nulls:** 0
- **format valid:** True
- **unique machines:** 149

## ecg_time
- **nulls after parsing:** 0
- **format valid:** True
- **cart_id + ecg_time duplicates:** 0

## [reports]
**Null coverage per report column:**
report_0         0
report_1     10277
report_2     10599
report_3     17425
report_4     24956
report_5     29969
report_6     32864
report_7     34310
report_8     34891
report_9     35084
report_10    35153
report_11    35169
report_12    35176
report_13    35180
report_14    35182
report_15    35183
report_16    35184
report_17    35184
dtype: int64

## [bandwidth]
- **nulls:** 0
- **format valid:** True
- **unique values:** 3

## [filtering]
- **nulls:** 0
- **unique values:** 3

## [measurements checks]

### rr_interval
- **nulls:** 0
- **is numeric:** True
- **min:** 295
- **max:** 29999
- **negative values:** 0

### p_onset
- **nulls:** 0
- **is numeric:** True
- **min:** 0
- **max:** 29999
- **negative values:** 0

### p_end
- **nulls:** 0
- **is numeric:** True
- **min:** 48
- **max:** 29999
- **negative values:** 0

### qrs_onset
- **nulls:** 0
- **is numeric:** True
- **min:** 68
- **max:** 29999
- **negative values:** 0

### qrs_end
- **nulls:** 0
- **is numeric:** True
- **min:** 160
- **max:** 29999
- **negative values:** 0

### t_end
- **nulls:** 0
- **is numeric:** True
- **min:** 380
- **max:** 29999
- **negative values:** 0

### p_axis
- **nulls:** 0
- **is numeric:** True
- **min:** -180
- **max:** 29999

### qrs_axis
- **nulls:** 0
- **is numeric:** True
- **min:** -180
- **max:** 29999

### t_axis
- **nulls:** 0
- **is numeric:** True
- **min:** -180
- **max:** 29999

## logical order checks
- **sequence violations:** 0

## derived checks
- **negative p_duration:** 0
- **negative qrs_duration:** 0
- **negative qt_proxy:** 0
- **p_duration > rr_interval:** 0
- **qrs_duration > rr_interval:** 0
- **qt_proxy > rr_interval:** 0
