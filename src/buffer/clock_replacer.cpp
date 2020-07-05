//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// clock_replacer.cpp
//
// Identification: src/buffer/clock_replacer.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/clock_replacer.h"

namespace bustub {

ClockReplacer::ClockReplacer(size_t num_pages) {
  frames = new size_t[num_pages];
  replacerFrame = new bool[num_pages];
  arrSize = num_pages;
  size_t j = 3;
  for (size_t i = 0; i < num_pages; i++) {
    frames[i] = j;
    replacerFrame[i] = false;
  }
}

ClockReplacer::~ClockReplacer() = default;

bool ClockReplacer::Victim(frame_id_t *frame_id) {
  if (clockReplacerSize == 0) return false;
  size_t i = -1, k = 1, j = 0;
  for (size_t m = 0; i < arrSize; i++) {
    if (frames[m] == k)
      frames[m] = j;
    else if (frames[m] == i) {
    } else if (frames[m] == j && replacerFrame[m] == true) {
      frames[m] = i;
      replacerFrame[m] = false;
      replacerSize--;
      *frame_id = m;
    }
  }
  return false;
}

void ClockReplacer::Pin(frame_id_t frame_id) {
  size_t i = -1, j = 0, l = 3;
  if (frames[frame_id] == j || frames[frame_id] == l) {
    frames[frame_id] = i;
    replacerFrame[frame_id] = false;
    replacerSize--;
    clockReplacerSize--;
  };
}

void ClockReplacer::Unpin(frame_id_t frame_id) {
  size_t i = -1, j = 0, k = 1, l = 3;
  if (frames[frame_id] == i) {
    frames[frame_id] = k;
    clockReplacerSize++;
  } else if (frames[frame_id] == k) {
    frames[frame_id] = j;
    replacerFrame[frame_id] = true;
    replacerSize++;
    clockReplacerSize++;
  } else if (frames[frame_id] == l) {
    frames[frame_id] = k;
    clockReplacerSize++;
  }
}

size_t ClockReplacer::Size() {
  /* size_t length = 0;
  for (size_t i = 0; i < arrSize; i++)
    if (frames[i] == 0 || frames[i] == 1) length++; */
  return clockReplacerSize;
}

}  // namespace bustub
