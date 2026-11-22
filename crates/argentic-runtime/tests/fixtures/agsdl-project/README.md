# Portable review fixture

The project follows AgSDL's v0.5.0 review example. It uses a local text
resource with a space in its filename and an unused MCP declaration. The
agent and flow use the Argentic profile. The MCP command is fixture data.

Generate both archives with the official v0.5.0 tool, using a fresh bundle
outside the fixture directory. From this directory set `PYTHONPATH` to
`tooling` in the checked-out AgSDL tag and run:

```sh
python3 -m agsdl.archive pack review review.agsdl
python3 -m agsdl.archive version review /tmp/review-versions.bundle -m "Portable review fixture"
python3 -m agsdl.archive pack review review-history.agsdl --history /tmp/review-versions.bundle
```
