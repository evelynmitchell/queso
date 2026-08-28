//! `queso-postmortem` -- adjudicate a preserved soak failure (issue #73).
//!
//! The Chain-of-Blocks observer reports a divergence as two hashes at one
//! height. That report cannot, on its own, distinguish a real Agreement
//! violation from a mis-reported sample: both look identical. The replicas'
//! durable applied logs can, and `queso-soak --failure-dir` now keeps them.
//!
//! ```sh
//! cargo run -p queso-soak --bin queso-postmortem -- soak-failures/seed-13
//! ```
//!
//! Feed it the divergence report's own numbers to check both sides of it:
//!
//! ```sh
//! cargo run -p queso-soak --bin queso-postmortem -- soak-failures/seed-13 \
//!     --claim 0@2150=0x05b9b10427ccaad1 \
//!     --claim 2@2150=0x0e4ba3bb9d05bb44
//! ```
//!
//! - Both claims confirmed **and** the logs differ at that slot: a genuine
//!   safety violation.
//! - Either claim contradicted: the observability path produced a hash the
//!   replica's own log does not fold to, and consensus is not implicated.
//! - Logs agree while both claims are confirmed: impossible, and if it ever
//!   printed, the chain fold itself is wrong.
//!
//! # Exit codes
//!
//! - `0` -- the preserved logs agree everywhere they overlap.
//! - `1` -- two logs hold different commands at the same slot.
//! - `2` -- nothing to adjudicate: no snapshots, or no two logs overlap.
//!   Distinct from `0` on purpose, because "found no disagreement" and
//!   "had nothing to disagree about" are not the same verdict.

use std::path::PathBuf;
use std::process::ExitCode;

use clap::Parser;

use queso_sim::ids::NodeId;
use queso_soak::postmortem::{Claim, PairVerdict, Postmortem};

#[derive(Parser, Debug)]
#[command(
    name = "queso-postmortem",
    about = "Adjudicate a preserved soak failure against the replicas' durable applied logs"
)]
struct Args {
    /// A preserved data directory: `soak-failures/seed-<n>`, or any
    /// directory holding `node-<id>.durable.bin` snapshots.
    data_dir: PathBuf,

    /// A hash the observer reported, as `<replica>@<n>=<hex>`, e.g.
    /// `0@2150=0x05b9b10427ccaad1`. Repeatable. Each is checked against
    /// that replica's own applied log.
    #[arg(long = "claim", value_parser = parse_claim)]
    claims: Vec<Claim>,

    /// Focus the recorder (ISR) section on this slot instead of the
    /// auto-selected disputed one (issue #84). Without it, the section
    /// covers the earliest slot at which the applied logs differ, falling
    /// back to the observer's disputed height when they agree; with it, a
    /// slot can be interrogated by hand.
    #[arg(long)]
    slot: Option<u64>,
}

/// `<replica>@<n>=<hex>`, parsed whole. Both halves of the `(n, h)` pair
/// come from one argument so a caller cannot supply a height and a hash
/// that were never seen together -- the same mistake `/chain` itself made
/// in #73.
fn parse_claim(text: &str) -> Result<Claim, String> {
    let (replica, rest) = text
        .split_once('@')
        .ok_or_else(|| format!("expected <replica>@<n>=<hex>, got `{text}`"))?;
    let (n, hash) = rest
        .split_once('=')
        .ok_or_else(|| format!("expected <replica>@<n>=<hex>, got `{text}`"))?;
    let replica = replica
        .trim_start_matches('n')
        .parse::<u32>()
        .map_err(|e| format!("bad replica in `{text}`: {e}"))?;
    let n = n
        .parse::<u64>()
        .map_err(|e| format!("bad n in `{text}`: {e}"))?;
    let hash = hash.strip_prefix("0x").unwrap_or(hash);
    let h = u64::from_str_radix(hash, 16).map_err(|e| format!("bad hash in `{text}`: {e}"))?;
    Ok(Claim {
        replica: NodeId(replica),
        n,
        h,
    })
}

fn main() -> ExitCode {
    let args = Args::parse();

    let postmortem = match Postmortem::open(&args.data_dir) {
        Ok(postmortem) => postmortem,
        Err(e) => {
            eprintln!("cannot read {}: {e:#}", args.data_dir.display());
            return ExitCode::from(2);
        }
    };

    print!("{}", postmortem.render_with_slot(&args.claims, args.slot));

    let pairs = postmortem.pairs();
    if pairs
        .iter()
        .any(|(_, _, v)| matches!(v, PairVerdict::Differ { .. }))
    {
        println!(
            "\nVERDICT: the durable applied logs disagree. This is a real Agreement (P1) \
             violation, not an observability artifact."
        );
        return ExitCode::from(1);
    }
    if pairs
        .iter()
        .any(|(_, _, v)| matches!(v, PairVerdict::Agree { .. }))
    {
        println!(
            "\nVERDICT: every preserved log agrees where it overlaps another. Any divergence \
             reported over `/chain` for a slot covered above came from the observability path."
        );
        return ExitCode::SUCCESS;
    }
    println!(
        "\nVERDICT: nothing to adjudicate -- no two preserved logs share a slot. This proves \
         nothing either way."
    );
    ExitCode::from(2)
}
