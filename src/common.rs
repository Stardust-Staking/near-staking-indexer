use tracing_subscriber::EnvFilter;
use crate::actions::{FullActionRow, FullDataRow, FullEventRow};
use crate::transactions::{AccountTxRow, BlockTxRow, ReceiptTxRow, TransactionRow};

pub fn setup_tracing(default: &str) {
    let mut env_filter = EnvFilter::new(default);

    if let Ok(rust_log) = std::env::var("RUST_LOG") {
        if !rust_log.is_empty() {
            for directive in rust_log.split(',').filter_map(|s| match s.parse() {
                Ok(directive) => Some(directive),
                Err(err) => {
                    eprintln!("Ignoring directive `{}`: {}", s, err);
                    None
                }
            }) {
                env_filter = env_filter.add_directive(directive);
            }
        }
    }

    tracing_subscriber::fmt::Subscriber::builder()
        .with_env_filter(env_filter)
        .with_writer(std::io::stderr)
        .init();
}

pub enum Row {
    TransactionRow(TransactionRow),
    AccountTxRow(AccountTxRow),
    BlockTxRow(BlockTxRow),
    ReceiptTxRow(ReceiptTxRow),
    FullActionRow(FullActionRow),
    FullEventRow(FullEventRow),
    FullDataRow(FullDataRow),
}
