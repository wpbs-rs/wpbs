/* SPDX-License-Identifier: GPL-3.0-or-later */
/* Copyright © 2026 Eduard Smet */

use serde::Deserialize;

#[derive(Default, Deserialize)]
pub struct ConfigJobScheduler {
    #[serde(default)]
    pub enabled: bool,
}
