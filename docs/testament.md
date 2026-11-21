# The Bughound testament

Bughound exists to take routine testing off LLM agents. By the time an agent needs to know whether an edit broke something, Bughound should have run the relevant checks and prepared a broken-tests report. That saves turns and tokens spent choosing test commands, waiting for runs, and digging through logs.

## From an edit to a targeted check

Continuously watch workspaces where checks are enabled. Keep an index of tests and the implementation files they exercise, and refresh it as tests and source files appear, change, or disappear. When an implementation file changes, wait briefly for the edits to settle, then run the tests linked to that file. A changed test should run itself. Changes to shared setup or suite configuration can justify a wider run.

The watch, scheduling, and reporting loop should work across languages. Each supported language and test runner brings its own rules for finding tests, linking them to source, and reading results. Those links may be incomplete: when Bughound cannot resolve a change to a test, say so. An unmapped change gives no evidence that the code passes.

## Discover checks and let people opt in

Bughound should detect the supported test suites present in each workspace and offer them in settings. Discovery should make setup easier, while running a suite should still require someone to enable it. Today the extension offers explicit workspace switches for Python (pytest) and JavaScript/TypeScript (`node --test`, plus TypeScript type checking when available). Suite autodetection and support for more languages are future work.

## Keep one report of what is still broken

Give each workspace one broken-tests card. When a check fails, add or update its entry with the test name or file and enough detail to act. When that check passes, remove its entry. A green run of an unrelated check should leave other failures visible. Remove the card when no reported failures remain.

Today the card reflects the latest run: a failing run replaces the report and a passing run clears it. Keeping failures across targeted runs is the goal. For example, if a run of the invoice tests fails and the next run of the shipping tests passes, the invoice failure should stay on the card until the invoice test passes.

## Run inside pi-studio today

Bughound is a pi-studio extension. Pi-studio supplies the workspaces and hosts the settings panel and the board where agents see the report. A standalone daemon may make sense later. The work today is to make the watch, check, and report loop useful inside pi-studio.
