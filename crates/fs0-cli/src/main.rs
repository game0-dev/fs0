use clap::Parser;

#[derive(Debug, Parser)]
#[command(name = "fs0", version, about = "append-only distributed storage")]
struct Cli {}

fn main() {
    let _cli = Cli::parse();
}
