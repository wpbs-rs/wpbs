/* SPDX-License-Identifier: GPL-3.0-or-later */
/* Copyright © 2026 Eduard Smet */

use crate::runtime::{
    internal::InternalRuntime,
    plugins::wpbs::plugin::job_scheduler_import_types::Host as JobSchedulerImportTypesHost,
};

impl JobSchedulerImportTypesHost for InternalRuntime {}
