# Pi deployment note

`herdr-relay.service` and `herdr-telegram.service` run `uv run` against **this
working tree**. It is intentionally kept checked out on branch **`pi-deploy`**:

    pi-deploy = v0.7.4 base (9bdfe06) + commit 4a5b011
                "fix(relay): stop blocked-pane notification floods from animated TUIs"

`main` tracks upstream (`dcolinmorgan/herdr-remote` via the `upstream` remote,
mirrored to `origin` = `guidodinello/herdr-remote`) and is ~100 commits ahead
(v0.8.0+, large relay rewrite). It is **NOT deployed**.

Do not `git checkout main` here — it silently reverts the flood fix.
To move forward: rebase `pi-deploy` onto `origin/main`, run the test suite
(`uv run --with "python-telegram-bot>=21.0" --with "websockets>=14.0" python -m
unittest discover -s tests`), deploy deliberately, then restart both services.
