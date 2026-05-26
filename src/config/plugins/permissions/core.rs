/* SPDX-License-Identifier: GPL-3.0-or-later */
/* Copyright © 2026 Eduard Smet */

use serde::{Deserialize, Serialize};

#[derive(Deserialize, PartialEq, Serialize)]
pub enum PluginPermissionsCore {
    All,
    DependencyFunctions,
    Shutdown,
}
