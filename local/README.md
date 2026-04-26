# Local-only files

This directory is for **machine-local** scripts, scratch DAX, connection-string
snippets, and other material that must **never** be committed.

Everything under `local/` except this `README.md` is ignored by Git (see the
root `.gitignore`).

For real Power BI benchmarking, prefer environment variables (see root
`.env.example` and the README **Performance** section) or a personal
`.env` / `.envrc` file that stays untracked.
