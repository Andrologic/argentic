# Changelog

## Unreleased

- Argentic runtime exposes the selected project files with their original bytes
  and executable flags, and maps assembled JSON pointers to component files.
- Hosts can write a deterministic AgSDL v0.5.0 snapshot archive without Git
  history from an opened project folder or archive.
- Hosts can interrupt and resume an open Codex work, and pause and resume an
  AgSDL 0.4.0 run. Inspection reports holds and pauses, including their retained
  state after an interrupted run. Flow approval deadlines continue during pause.
- Resuming a paused run preserves the original evidence for deferred branch
  failures, including approval timeouts. Interrupting Codex preserves observed
  automatic plans and records accepted action answers that the turn never received.
