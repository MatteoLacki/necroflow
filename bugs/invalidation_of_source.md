
  Necroflow's node fingerprint (_rule_hash/provenance_hash in fingerprints.py) is built from: the rule name, the resolved command string
  (e.g. venvs/common/bin/ms1_annotate_candidates <args>), the declared input/output types, the config values, and the parent lineage's own 
  hashes. It does not hash the Python source files that command ends up executing.

  So git/timstofu/src/timstofu/candidate_postprocessing/annotate.py is never a tracked dependency of the annotate_precursor_clusters node at
  all — it's invisible to the fingerprint system, same as any other library code the resolved binary happens to import. The skill doc calls
  this out directly: "External file referenced by a path config value. The fingerprint hashes the path string, never the file content." Same
  principle applies one level further out here — the command references a script, and the script's own source isn't hashed either.

  Why did it pick up changes earlier in the session, then? Not because necroflow detected the edits — because I ran uv pip install -e to
  refresh the venvs/common console-script stubs after deleting the peak-splitting CLI files. That regenerated the installed stub files, which
  apparently is visible to whatever the command/environment identity captures. Today I edited source directly without reinstalling, so
  nothing looked different to necroflow.

  Practically: since it's an editable install, the code that would run is already the fixed version — necroflow just doesn't know to
  invalidate its cache automatically. To force a real rerun exercising today's edits, I'd use --invalidate annotate_precursor_clusters
  (cascades to descendants). Want me to do that now?
