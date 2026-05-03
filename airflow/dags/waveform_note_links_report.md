# Waveform Note Links Profiling Report

## Overview
- **Rows:** 50000
- **Columns:** 6

**Data types:**
 subject_id        int64
study_id          int64
waveform_path    object
note_id          object
note_seq          int64
charttime        object
dtype: object

## subject_id
- **nulls:** 0
- **format valid (digits only):** True
- **duplicates:** 41206

## study_id
- **nulls:** 0
- **format valid (digits only):** True
- **unique check:** False
- **duplicates count:** 3
- **unique ratio:** 0.9999

## subject_id + study_id
- **duplicate pairs:** 3
- **unique ratio:** 0.9999

## waveform_path
- **nulls:** 0
- **format valid:** True
- **unique check:** False
- **duplicates count:** 3
- **unique ratio:** 0.9999
- **study_id in path:** True

## note_id
- **nulls:** 0
- **unique:** True
- **format valid:** True
- **duplicates count:** 0
- **unique ratio:** 1.0000

## note_seq
- **nulls:** 0
- **min:** 2
- **max:** 196

## charttime
- **nulls after parsing:** 0
- **format valid:** True
