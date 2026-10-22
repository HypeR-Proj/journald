# SPDX-FileCopyrightText: 2026 roolrz
# SPDX-License-Identifier: Apache-2.0

$(eval $(call native-rust-app,svc/journal,hyper-journald,svc/journal,svc/journal,development))
