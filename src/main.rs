//! Binary entrypoint - all real logic lives in lib.rs (`cli_main`), so
//! integration tests can exercise the same code paths directly. See
//! lib.rs's module doc comment for why.

fn main() {
    std::process::exit(busbridge::cli_main());
}
