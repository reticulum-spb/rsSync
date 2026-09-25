use clap::{Args, Parser, Subcommand};
use rrsync::{
    Error, Result,
    config::{Config, address},
    fs::Root,
    sync::Options,
    transport,
};
use std::path::PathBuf;
#[derive(Parser)]
#[command(version, about = "Synchronize directories over Reticulum")]
struct Cli {
    /// Configuration directory (default: ~/.rsSync).
    #[arg(long, global = true, value_name = "DIRECTORY")]
    config: Option<PathBuf>,
    #[arg(short, long, global = true)]
    verbose: bool,
    #[command(subcommand)]
    command: Command,
}
#[derive(Subcommand)]
enum Command {
    /// Publish a directory through an authenticated Reticulum destination.
    Serve { directory: PathBuf },
    /// Create/load the persistent identity and display its ACL address.
    Identity,
    /// Synchronize the contents of a local directory to a remote directory.
    Push {
        #[command(flatten)]
        options: Flags,
        source: PathBuf,
        destination: String,
    },
    /// Synchronize the contents of a remote directory to a local directory.
    Pull {
        #[command(flatten)]
        options: Flags,
        source: String,
        destination: PathBuf,
    },
}
#[derive(Args)]
struct Flags {
    #[arg(long)]
    dry_run: bool,
    #[arg(long)]
    delete: bool,
    #[arg(long)]
    checksum: bool,
}
impl From<Flags> for Options {
    fn from(f: Flags) -> Self {
        Self {
            dry_run: f.dry_run,
            delete: f.delete,
            checksum: f.checksum,
        }
    }
}
fn remote(s: &str) -> Result<([u8; 16], String)> {
    let (id, path) = s
        .split_once(':')
        .ok_or_else(|| Error::Config("remote must be <destination>:<path>".into()))?;
    // A leading slash denotes the export root; wire paths are always relative.
    let path = path.strip_prefix('/').unwrap_or(path);
    if !path.is_empty() {
        rrsync::fs::validate(path)?;
    }
    Ok((address(id)?, path.into()))
}
#[tokio::main]
async fn main() {
    let cli = Cli::parse();
    let filter = if cli.verbose {
        "rrsync=debug,rns_runtime=debug,rns_transport=warn"
    } else {
        "rrsync=info,rns_runtime=warn,rns_transport=warn"
    };
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| filter.into()),
        )
        .with_writer(std::io::stderr)
        .init();
    if let Err(e) = run(cli).await {
        eprintln!("{e}");
        std::process::exit(1);
    }
}
async fn run(cli: Cli) -> Result<()> {
    let dry_run = matches!(&cli.command, Command::Push {options,..} | Command::Pull {options,..} if options.dry_run);
    let implicit = cli.config.is_none();
    let directory = match &cli.config {
        Some(path) => path.clone(),
        None => Config::default_directory()?,
    };
    let config = Config::load_directory(&directory, implicit && !dry_run)?;
    // Initialize the persistent identity alongside config.yaml, except in dry-run.
    transport::identity(&config, dry_run)?;
    match cli.command {
        Command::Identity => {
            let id = transport::identity(&config, false)?;
            println!("Identity: {}", hex::encode(id.hash));
            Ok(())
        }
        Command::Serve { directory } => transport::serve(config, Root::open(&directory)?).await,
        Command::Push {
            options,
            source,
            destination,
        } => {
            let (id, path) = remote(&destination)?;
            transport::client(&config, true, &source, id, path, options.into()).await
        }
        Command::Pull {
            options,
            source,
            destination,
        } => {
            let (id, path) = remote(&source)?;
            transport::client(&config, false, &destination, id, path, options.into()).await
        }
    }
}
