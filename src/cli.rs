/* SPDX-License-Identifier: GPL-3.0-or-later */
/* Copyright © 2026 Eduard Smet */

use std::path::PathBuf;

use clap::{ArgAction, Args, Parser};
use tracing::level_filters::LevelFilter;
use tracing_appender::rolling::Rotation;

#[derive(Clone)]
pub struct CliLogParametersFileRotation(pub Rotation);

impl From<String> for CliLogParametersFileRotation {
    fn from(arg: String) -> Self {
        match arg.to_uppercase().as_str() {
            "MINUTELY" => Self(Rotation::MINUTELY),
            "HOURLY" => Self(Rotation::HOURLY),
            "NEVER" => Self(Rotation::NEVER),
            _ => Self(Rotation::DAILY),
        }
    }
}

#[derive(Parser)]
#[command(about, long_about = None, version, author)]
pub struct Cli {
    #[command(flatten)]
    pub log_parameters: CliLogParameters,

    #[arg(default_value = "./config.yaml", short, long, value_name = "FILE PATH", help = "The path to the program its configuration file", long_help = None)]
    pub config_file: PathBuf,

    #[arg(default_value = "./.env", short, long, value_name = "FILE PATH", help = "The path to an env file, used by the program its configuration file for env var interpolation", long_help = None)]
    pub env_file: PathBuf,

    #[arg(default_value = "./plugins", short, long, value_name = "DIRECTORY PATH", help = "The path to the program its plugin directory", long_help = None)]
    pub plugin_directory: PathBuf,

    #[arg(action=ArgAction::Set, default_value_t = true, short = 'C', long, value_name = "BOOL", help = "Enable the usage of cached plugins", long_help = None, hide_possible_values = true)]
    pub cache: bool,

    #[arg(default_value = "./database", short, long, value_name = "DIRECTORY PATH", help = "The path to the program its database", long_help = None)]
    pub database_directory: PathBuf,

    #[arg(default_value_t = 15, short = 't', long, value_name = "SECONDS", help = "The amount of seconds after which the HTTP client should timeout", long_help = None)]
    pub http_client_timeout_seconds: u64,
}

#[derive(Args)]
pub struct CliLogParameters {
    #[arg(default_value = "INFO", short = 'l', long = "log-stdout-level", value_name = "LEVEL", help = "The level at which the program should log to stdout", long_help = None)]
    pub stdout_level: LevelFilter,

    #[arg(action=ArgAction::Set, default_value_t = true, short = 'a', long = "log-stdout-ansi", value_name = "BOOL", help = "Enable ANSI escape code for the output of the stdout logger", long_help = None, hide_possible_values = true)]
    pub stdout_ansi: bool,

    #[arg(default_value = "INFO", short = 'L', long = "log-file-level", value_name = "LEVEL", help = "The level at which the program should log to a file", long_help = None)]
    pub file_level: LevelFilter,

    #[arg(default_value = "./logs", short = 'D', long = "log-file-directory", value_name = "DIRECTORY PATH", help = "The path to the program its logging directory", long_help = None)]
    pub file_directory: PathBuf,

    #[arg(default_value = "DAILY", short = 'R', long = "log-file-rotation", value_name = "ROTATION", help = "The rotation strategy for log files", long_help = None)]
    pub file_rotation: CliLogParametersFileRotation,

    #[arg(default_value_t = 7, short = 'M', long = "log-file-max", value_name = "COUNT", help = "The maximum amount of log files the program should keep", long_help = None)]
    pub file_max: usize,

    #[arg(default_value = "", short = 'P', long = "log-file-prefix", value_name = "FILE PREFIX", help = "The prefix for log filenames", long_help = None, hide_default_value = true)]
    pub file_prefix: String,

    #[arg(default_value = "log", short = 'S', long = "log-file-suffix", value_name = "FILE SUFFIX", help = "The suffix for log filenames", long_help = None)]
    pub file_suffix: String,

    #[arg(action=ArgAction::Set, default_value_t = false, short = 'A', long = "log-file-ansi", value_name = "BOOL", help = "Enable ANSI escape code for the output of the file logger", long_help = None, hide_possible_values = true)]
    pub file_ansi: bool,
}
