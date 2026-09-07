# Pi deployment note

`herdr-relay.service` and `herdr-telegram.service` run `uv run` against **this
working tree**, kept checked out on the deploy branch **`pi-deploy`**.

    pi-deploy = origin/main (upstream, currently 68aaf1c / v0.8.0+)
              + one commit: "fix(relay): stop re-notifying for a blocked pane
                whose TUI is animating"  ->  submitted upstream as
                https://github.com/dcolinmorgan/herdr-remote/pull/65
              + this DEPLOY-NOTE.md

`main` tracks upstream (`dcolinmorgan/herdr-remote` via `upstream`, mirrored to
`origin` = `guidodinello/herdr-remote`).

## Why the fork exists

The blocked-notification flood fix (PR #65). The bug is present in upstream
HEAD; until #65 lands, keep carrying that commit across every upstream bump.
If #65 merges, this branch collapses back to plain `origin/main` and the fork
is only a convenience remote.

## Rollback

`git checkout pi-deploy-v074-rollback` (v0.7.4 + the same fix) and restart both
services.

## Updating to a newer upstream

    git fetch upstream && git rebase origin/main pi-deploy
    PATH=$HOME/.local/bin:$PATH sh tests/run.sh          # node/uv must resolve
    HERDR_RELAY_PORT=8399 uv run relay/herdr_relay.py    # smoke test, ~20s
    systemctl --user restart herdr-relay herdr-telegram  # then watch journals
