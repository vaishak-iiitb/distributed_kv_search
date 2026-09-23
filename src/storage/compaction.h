#pragma once

#include <string>
#include <vector>

// Simplest possible compaction strategy (design doc §1.6, step 6):
// merge every given SSTable into one new file. This is the "merge-all"
// scheme, not leveled compaction — see the accompanying doc for why
// tombstones can only be safely dropped unconditionally in this scheme,
// not once real leveled compaction exists.
class Compactor {
 public:
  // Merges `input_paths`, given OLDEST FIRST, into a single new SSTable
  // at `output_path`. For each key, keeps only the version from the
  // newest input file that contains it; tombstones for keys whose
  // newest version is a delete are dropped from the output entirely,
  // since this merge consumes every known SSTable (see section 4).
  //
  // If `delete_inputs_on_success` is true and the merge succeeds, the
  // original input files are removed from disk — only do this once
  // you're confident output_path is durably written (matches §1.3's
  // "delete WAL only once flush is durable" caution, same principle
  // applied to compaction).
  //
  // Returns false (and leaves all inputs untouched) if the merge or
  // write fails for any reason.
  static bool CompactAll(const std::vector<std::string>& input_paths,
                          const std::string& output_path,
                          bool delete_inputs_on_success = false);

  // Simple trigger check: true once `current_file_count` has crossed
  // `threshold`. Deliberately trivial — the interesting logic lives in
  // CompactAll, not in deciding when to call it.
  static bool ShouldCompact(size_t current_file_count, size_t threshold);
};