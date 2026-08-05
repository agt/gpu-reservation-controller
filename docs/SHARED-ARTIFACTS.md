# Shared artifacts (gpu-reservation-app ↔ gpu-reservation-controller)

Four files are maintained as **byte-identical copies** in both repositories.
They are the seam between the two processes: change one copy and the other is
wrong, silently, until someone happens to diff them.

That is not hypothetical. Before this manifest existed the convention was stated
in prose and guarded by a test named
`test_dictionary_and_helper_are_in_sync_with_the_sibling_repo` whose body only
asserted the local files *existed* — its own docstring conceded the byte
comparison was "a review-time obligation". All three documents had drifted:

| artifact | drift found |
|---|---|
| `LOG-FIELDS.md` | **both directions** — the app copy was missing `dropped`/`task`/`holder`/`age_s` (real controller fields), the controller copy was missing `rev`/`bootstrap`/`date`/`days` and still described the removed JWT auth model |
| `RESERVATION-API.md` | controller copy 15 days stale across 29 hunks: JWT auth, no `status=recent`, no `include_consumed_cancellations`, wrong `include_teammates` scoping |
| `SCHEDULING.md` | controller copy 23 days stale, documenting a `management_buffer`, a `min_su_per_gpu_hour` floor and `kind='reclaim'` — **none of which exist** anywhere in the app's code or migrations |

Note that the `LOG-FIELDS.md` drift was bidirectional, which is why a one-way
copy is the wrong repair: overwriting the controller's copy with the app's would
have deleted four real controller fields and broken the controller's *own*
`test_log_grammar.py`, which asserts every emitted field is documented.

## The manifest

`SHARED-ARTIFACTS.sha256` records the SHA-256 of each artifact's canonical
content. Both repositories carry an identical manifest, and
`tests/test_shared_artifacts.py` in each repo fails when a local copy no longer
matches it.

The manifest keys on the **logical name**, not the path, which is what let the
app reorganise its documentation without either repo's manifest changing: only
this table and each repo's `_ARTIFACTS` map had to follow.

| logical name | path in gpu-reservation-app | path in gpu-reservation-controller |
|---|---|---|
| `log_fields.py` | `app/log_fields.py` | `app/log_fields.py` |
| `LOG-FIELDS.md` | `docs/contracts/LOG-FIELDS.md` | `docs/LOG-FIELDS.md` |
| `RESERVATION-API.md` | `docs/contracts/RESERVATION-API.md` | `docs/RESERVATION-API.md` |
| `SCHEDULING.md` | `docs/contracts/SCHEDULING.md` | `docs/SCHEDULING.md` |

The app's manifest now lives at `docs/contracts/SHARED-ARTIFACTS.sha256`; the
controller's stays at `docs/SHARED-ARTIFACTS.sha256`. The bodies are identical —
only the two files' locations differ.

## Changing a shared artifact

1. Edit the file in whichever repo you are working in.
2. Run `python scripts/sync_shared_artifacts.py --update` there. The suite now
   passes locally and the manifest carries the new hash.
3. Copy the **same** file and the **same** manifest into the sibling repo, and
   land both changes together.

Step 2 is what makes the omission loud: you cannot edit one of these files and
have your own repo's tests pass without also touching the manifest, and the
manifest diff is a reviewer-visible reminder that a sibling PR is owed.

`python scripts/sync_shared_artifacts.py --check` is what the test runs;
`--print` lists the current hashes for pasting into the sibling repo.

## Why not a submodule or a generated copy?

A git submodule would enforce this perfectly but costs every clone, CI job and
contributor a submodule step for four files. Making one repo the generator and
the other a build artifact means the controller can no longer be read
standalone, which is the property that makes `docs/RESERVATION-API.md` useful to
someone debugging the controller in isolation. The manifest keeps both repos
independently readable and self-checking; the residual gap — someone updating
both file *and* manifest in one repo and never opening the other — is visible in
review rather than invisible.
