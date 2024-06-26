mod balance;
mod busses;
mod claim;
mod cu_limits;
#[cfg(feature = "admin")]
mod initialize;
mod mine;
mod miner_v2;
mod register;
mod rewards;
mod send_and_confirm;
mod treasury;
#[cfg(feature = "admin")]
mod update_admin;
#[cfg(feature = "admin")]
mod update_difficulty;
mod utils;

use std::sync::Arc;

use clap::{command, Parser, Subcommand};
use miner_v2::MinerV2;
use solana_client::nonblocking::rpc_client::RpcClient;
use solana_sdk::{
    commitment_config::CommitmentConfig,
    signature::{read_keypair_file, Keypair},
};

struct Miner {
    pub keypair_filepath: Option<String>,
    pub priority_fee: u64,
    pub rpc_client: Arc<RpcClient>,
}

#[derive(Parser, Debug)]
#[command(about, version)]
struct Args {
    #[arg(
        long,
        value_name = "NETWORK_URL",
        help = "Network address of your RPC provider",
        global = true
    )]
    rpc: Option<String>,

    #[clap(
        global = true,
        short = 'C',
        long = "config",
        id = "PATH",
        help = "Filepath to config file."
    )]
    pub config_file: Option<String>,

    #[arg(
        long,
        value_name = "KEYPAIR_FILEPATH",
        help = "Filepath to keypair to use",
        global = true
    )]
    keypair: Option<String>,

    #[arg(
        long,
        value_name = "MICROLAMPORTS",
        help = "Number of microlamports to pay as priority fee per transaction",
        default_value = "0",
        global = true
    )]
    priority_fee: u64,

    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand, Debug)]
enum Commands {
    #[command(about = "Fetch the Ore balance of an account")]
    Balance(BalanceArgs),

    #[command(about = "Fetch the distributable rewards of the busses")]
    Busses(BussesArgs),

    #[command(about = "Mine Ore using local compute")]
    Mine(MineArgs),

    #[command(about = "Mine Ore using local compute. Includes additional commands and different send logic.")]
    MineV2(MineV2Args),

    #[command(about = "Claim available mining rewards")]
    Claim(ClaimArgs),

    #[command(about = "Claim available mining rewards. Uses v2 send logic and has a few additional commands.")]
    ClaimV2(ClaimV2Args),

    #[command(about = "Fetch your balance of unclaimed mining rewards")]
    Rewards(RewardsArgs),

    #[command(about = "Fetch the treasury account and balance")]
    Treasury(TreasuryArgs),

    #[command(about = "Log data about the wallets in the supplied directory.")]
    Wallets(WalletsArgs),

    #[command(about = "Send sol from supplied wallet key file, to wallets in supplied directory.")]
    SendSol(SendSolArgs),

    #[cfg(feature = "admin")]
    #[command(about = "Initialize the program")]
    Initialize(InitializeArgs),

    #[cfg(feature = "admin")]
    #[command(about = "Update the program admin authority")]
    UpdateAdmin(UpdateAdminArgs),

    #[cfg(feature = "admin")]
    #[command(about = "Update the mining difficulty")]
    UpdateDifficulty(UpdateDifficultyArgs),
}

#[derive(Parser, Debug)]
struct BalanceArgs {
    #[arg(
        // long,
        value_name = "ADDRESS",
        help = "The address of the account to fetch the balance of"
    )]
    pub address: Option<String>,
}

#[derive(Parser, Debug)]
struct BussesArgs {}

#[derive(Parser, Debug)]
struct RewardsArgs {
    #[arg(
        // long,
        value_name = "ADDRESS",
        help = "The address of the account to fetch the rewards balance of"
    )]
    pub address: Option<String>,
}

#[derive(Parser, Debug)]
struct MineArgs {
    #[arg(
        long,
        short,
        value_name = "THREAD_COUNT",
        help = "The number of threads to dedicate to mining",
        default_value = "1"
    )]
    threads: u64,
    #[arg(
        long,
        short = 's',
        value_name = "SEND_INTERVAL",
        help = "The amount of time to wait between tx sends. 100ms is 10 sends per second.",
        default_value = "1000"
    )]
    send_interval: u64,
}

#[derive(Parser, Debug)]
struct MineV2Args {
    #[arg(
        long,
        short,
        value_name = "THREAD_COUNT",
        help = "The number of threads to dedicate to mining",
        default_value = "1"
    )]
    threads: u64,
    #[arg(
        long,
        short = 's',
        value_name = "SEND_INTERVAL",
        help = "The amount of time to wait between tx sends. 100ms is 10 sends per second.",
        default_value = "1000"
    )]
    send_interval: u64,
    #[arg(
        long,
        short = 's',
        value_name = "SIMULTATION_ATTEMPS",
        help = "The amount of simulation attempts before sending transaction. Useful for debugging ",
        default_value = None,
    )]
    sim_attempts: Option<u64>,
    #[arg(
        long,
        short = 'b',
        value_name = "BATCH_SIZE",
        help = "The batch size of wallets to process and bundle together. Max is 5.",
        default_value = "1"
    )]
    batch_size: u64,
    #[arg(
        long,
        short = 'f',
        value_name = "FEE_PAYER",
        help = "The path to the fee_payer wallet.",
        default_value = None
    )]
    fee_payer: Option<String>,
    #[arg(
        long,
        short = 'w',
        value_name = "MINER_WALLETS",
        help = "The directory/folder with the json wallets. Use solana-keygen to make keys.",
        default_value = None
    )]
    miner_wallets: Option<String>,
}

#[derive(Parser, Debug)]
struct TreasuryArgs {}

#[derive(Parser, Debug)]
struct ClaimArgs {
    #[arg(
        // long,
        value_name = "AMOUNT",
        help = "The amount of rewards to claim. Defaults to max."
    )]
    amount: Option<f64>,

    #[arg(
        // long,
        value_name = "TOKEN_ACCOUNT_ADDRESS",
        help = "Token account to receive mining rewards."
    )]
    beneficiary: Option<String>,
}

#[derive(Parser, Debug)]
struct ClaimV2Args {
    #[arg(
        // long,
        value_name = "AMOUNT",
        help = "The amount of rewards to claim. Defaults to max."
    )]
    amount: Option<f64>,
    #[arg(
        // long,
        short = 'b',
        value_name = "TOKEN_ACCOUNT_ADDRESS",
        help = "Token account to receive mining rewards."
    )]
    beneficiary: Option<String>,
    #[arg(
        long,
        short = 's',
        value_name = "SEND_INTERVAL",
        help = "The amount of time to wait between tx sends. 100ms is 10 sends per second.",
        default_value = "1000"
    )]
    send_interval: u64,
    #[arg(
        long,
        short = 'w',
        value_name = "MINER_WALLETS",
        help = "The directory/folder with the json wallets. Use solana-keygen to make keys.",
        default_value = None
    )]
    miner_wallets: Option<String>,
}


#[derive(Parser, Debug)]
struct WalletsArgs {
    #[arg(
        long,
        short = 'w',
        value_name = "MINER_WALLETS",
        help = "The directory/folder with the json wallets. Use solana-keygen to make keys.",
        default_value = None
    )]
    miner_wallets: Option<String>,
}

#[derive(Parser, Debug)]
struct SendSolArgs {
    #[arg(
        long,
        short = 'p',
        value_name = "SENDER_WALLET",
        help = "The wallet key file to send the sol from.",
    )]
    sender_wallet: String,
    #[arg(
        long,
        value_name = "AMOUNT",
        help = "The amount of lamports to send.",
        default_value = None,
    )]
    amount: Option<u64>,
    #[arg(
        long,
        short = 's',
        value_name = "SEND_INTERVAL",
        help = "The amount of time to wait between tx sends. 100ms is 10 sends per second.",
        default_value = "1000"
    )]
    send_interval: u64,
    #[arg(
        long,
        short = 'w',
        value_name = "MINER_WALLETS",
        help = "The directory/folder with the json wallets to send the sol to. Use solana-keygen to make keys.",
        default_value = None
    )]
    receiving_wallets: Option<String>,
}

#[cfg(feature = "admin")]
#[derive(Parser, Debug)]
struct InitializeArgs {}

#[cfg(feature = "admin")]
#[derive(Parser, Debug)]
struct UpdateAdminArgs {
    new_admin: String,
}

#[cfg(feature = "admin")]
#[derive(Parser, Debug)]
struct UpdateDifficultyArgs {}

#[tokio::main]
async fn main() {
    let args = Args::parse();

    // Load the config file from custom path, the default path, or use default config values
    let cli_config = if let Some(config_file) = &args.config_file {
        solana_cli_config::Config::load(config_file).unwrap_or_else(|_| {
            eprintln!("error: Could not find config file `{}`", config_file);
            std::process::exit(1);
        })
    } else if let Some(config_file) = &*solana_cli_config::CONFIG_FILE {
        solana_cli_config::Config::load(config_file).unwrap_or_default()
    } else {
        solana_cli_config::Config::default()
    };

    // Initialize miner.
    let cluster = args.rpc.unwrap_or(cli_config.json_rpc_url);
    let default_keypair = args.keypair.unwrap_or(cli_config.keypair_path);
    let rpc_client = RpcClient::new_with_commitment(cluster.clone(), CommitmentConfig::confirmed());

    let rpc_client_2 = Arc::new(RpcClient::new_with_commitment(cluster, CommitmentConfig::confirmed()));

    let miner = Arc::new(Miner::new(
        Arc::new(rpc_client),
        args.priority_fee,
        Some(default_keypair),
    ));
    let priority_fee = args.priority_fee;

    // Execute user command.
    match args.command {
        Commands::Balance(args) => {
            miner.balance(args.address).await;
        }
        Commands::Busses(_) => {
            miner.busses().await;
        }
        Commands::Rewards(args) => {
            miner.rewards(args.address).await;
        }
        Commands::Treasury(_) => {
            miner.treasury().await;
        }
        Commands::Mine(args) => {
            miner.mine(args.threads, args.send_interval).await;
        }
        Commands::MineV2(args) => {
            MinerV2::mine(rpc_client_2.clone(), args.threads, args.send_interval, args.batch_size, args.miner_wallets, priority_fee,args.sim_attempts, args.fee_payer).await;
        }
        Commands::Claim(args) => {
            miner.claim(args.beneficiary, args.amount).await;
        }
        Commands::ClaimV2(args) => {
            MinerV2::claim(rpc_client_2.clone(), args.send_interval, args.miner_wallets, args.beneficiary, priority_fee).await;
        }
        Commands::Wallets(args) => {
            MinerV2::wallets(rpc_client_2.clone(), args.miner_wallets).await;
        }
        Commands::SendSol(args) => {
            MinerV2::send_sol(rpc_client_2.clone(), args.sender_wallet, args.receiving_wallets, args.send_interval, args.amount).await;
        }
        #[cfg(feature = "admin")]
        Commands::Initialize(_) => {
            miner.initialize().await;
        }
        #[cfg(feature = "admin")]
        Commands::UpdateAdmin(args) => {
            miner.update_admin(args.new_admin).await;
        }
        #[cfg(feature = "admin")]
        Commands::UpdateDifficulty(_) => {
            miner.update_difficulty().await;
        }
    }
}

impl Miner {
    pub fn new(rpc_client: Arc<RpcClient>, priority_fee: u64, keypair_filepath: Option<String>) -> Self {
        Self {
            rpc_client,
            keypair_filepath,
            priority_fee,
        }
    }

    pub fn signer(&self) -> Keypair {
        match self.keypair_filepath.clone() {
            Some(filepath) => read_keypair_file(filepath).unwrap(),
            None => panic!("No keypair provided"),
        }
    }
}
