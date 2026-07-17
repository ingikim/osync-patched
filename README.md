# osync-patched

Personal patched fork of [Osync (Self-Hosted)](https://github.com/KORThomasJeong/Osync-p) — a self-hosted, end-to-end encrypted vault sync plugin for Obsidian by Thomas Jeong (MIT licensed).

**This is a private-use fork.** If you found this repo looking for the plugin, use the upstream: https://github.com/KORThomasJeong/Osync-p

## Patches on top of upstream

1. **rebase-client re-seal fix** — on `rebase-client` conflict resolution, the pending mutation's `encryptedMetadata` is re-encrypted against the new `baseRevision` AAD context. Upstream carries the old ciphertext onto the new revision, which makes the mutation permanently undecryptable (push breaks silently, pull crash-loops).
2. **pull dirty-scan guard** — `findConflictingPendingMutation` skips (instead of aborting on) dirty entries whose metadata cannot be decrypted, so one corrupt local row cannot wedge the entire pull cycle.
3. **push self-heal** — an undecryptable pending mutation is re-queued from the on-disk file when possible, or dropped, instead of permanently blocking the push queue.

## Versions

- `v2.3.1` — the battle-tested June 2026 hot-patched build (upstream 2.2.3 base + patches 1–3). Version number is set above upstream's 2.3.0 so the community-store copy is never offered as an "update" over this build.
- `v2.3.2+` (when present) — patches 1–3 ported onto upstream 2.3.0 source (includes upstream's remote poison quarantine and sync-error escalator), built from source in this repo.

## Install (via BRAT)

1. Install "BRAT" from the Obsidian community plugin store.
2. BRAT → Add beta plugin → `ingikim/osync-patched`.

## License

MIT — see [LICENSE](LICENSE). Original work © 2026 Synch, © 2026 Thomas Jeong. Patches © 2026 Ingi Kim.
