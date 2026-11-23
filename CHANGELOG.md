# Changelog

## Unreleased

- Argentic runtime exposes the selected project files with their original bytes
  and executable flags, and maps assembled JSON pointers to component files.
- Hosts can write a deterministic AgSDL v0.5.0 snapshot archive without Git
  history from an opened project folder or archive.
- Hosts can interrupt and resume an open Codex work, and pause and resume an
  AgSDL 0.4.0 run. Inspection reports holds and pauses, including their retained
  state after an interrupted run. Flow approval deadlines continue during pause.
- Native admission reconciles lost commit acknowledgements for operator
  actions and deliveries before dispatch and work finalization. Resuming a
  paused run preserves arrival order for steer fallbacks and uses the original
  evidence when routing deferred failures, including failed steers and approval
  timeouts.
- Interrupting Codex preserves the first automatic plan reported through
  the end of the turn. The journal preserves action answers and timeouts
  when the hold closes their delivery. Confirmations received during a hold
  retain the steer send's admission checkpoint for replay.
- Stop binds superseded deadlines to physical termination and keeps prior
  results and approval decisions. The agent result and its clock retirement
  reservation commit together. Recovery retains the exact unevaluated clock
  ticket when it fences an interrupted run.
