## Repo Guide (WC26 Terminal)

### Entry points
- `src/main.rs`: TUI rendering + input loop
- `src/state.rs`: core state model + filtering
- `src/feed.rs`: background fetch worker + command handling
- `src/upcoming_fetch.rs` / `src/analysis_fetch.rs`: API fetch + parsing

### Commands
- Build: `cargo build`
- Run: `cargo run --release`
- Format: `cargo fmt --all`
- Check: `cargo check`
- Tests: `cargo test`

### Notes
- Generated artifacts (XLSX exports, `target/`, screenshots, cache files) should stay out of the repo.
