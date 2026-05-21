#![forbid(unsafe_code)]
#![doc = "Microsoft Graph client for Rust."]

pub mod account;
pub mod api;
pub mod autodiscover;
pub mod blob;
pub mod calendar_sync;
pub mod client;
pub mod encoding;
pub mod ews;
pub mod folder_mapper;
pub mod group_sync;
pub mod headers;
pub mod message;
pub mod onedrive;
pub mod parse;
mod preset_colors;
pub mod send;
mod time;
pub mod types;
pub mod webhooks;
