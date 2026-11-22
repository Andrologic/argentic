# AgSDL v0.5.0 project and archive conformance

`SOURCE` names the tag. The execution contract stays `agsdl-0.4.0`, so the
`cases/` and `runs/` corpora are unchanged from `../conformance-040`. This
directory holds what v0.5.0 adds to `conformance/README.md`:
"Project assembly" and "Archive interchange".

| Path | Origin |
| --- | --- |
| `review-project/` | `examples/review-project/` at the tag, unchanged. |
| `projects/review.json` | `conformance/projects/review.json` at the tag, unchanged. |
| `review-project.agsdl` | Official packer, snapshot only. |
| `review-project-history.agsdl` | Official packer, with a `history.bundle`. |
| `review-notes.agsdl` | Official packer, run on a copy with an unrelated `NOTES.md` beside the manifest. |

The tests are in `src/source/conformance050_tests.rs`.

## Regenerating the archives

Export the tag into a fresh directory, outside this repository, and run its
tool from a working directory that holds copies of the example:

```sh
git -C AgSDL archive v0.5.0 | tar -x -C /tmp/agsdl-050/tag
cd /tmp/agsdl-050/work
cp -r ../tag/examples/review-project review-project
cp -r review-project review-notes && printf 'Unrelated notes.\n' > review-notes/NOTES.md
export PYTHONPATH=/tmp/agsdl-050/tag/tooling
python3 -m agsdl.archive pack review-project review-project.agsdl
python3 -m agsdl.archive pack review-notes review-notes.agsdl
python3 -m agsdl.archive version review-project /tmp/agsdl-050/v.bundle -m "Review example"
python3 -m agsdl.archive pack review-project review-project-history.agsdl --history /tmp/agsdl-050/v.bundle
```

The packer wrote byte-identical `archive.json` inventories for
`review-project.agsdl` and `review-notes.agsdl`.

## Not covered

- The Git profile's offline checkout of an earlier revision. Argentic checks
  the bundle's size and digest and opens the current snapshot without Git.
  It does not import history.
- Running the example. It declares a remote MCP server used by a Codex agent.
  The Argentic profile refuses both before start, and a test pins those
  diagnostics. Execution through all three inputs is covered by the
  `../agsdl-project` fixture.
