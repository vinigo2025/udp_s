fn main() {
    if let Err(err) = udp_s::run_from_args() {
        eprintln!("fatal: {err}");
        std::process::exit(1); // return
    }
}
