//! Original failure diagnostics for consumers explicitly opting into query
//! details.

use std::fmt::Display;

/// Preserves the original error text without sanitizing, truncating, or
/// classifying it.
pub(super) fn query_log_detail(error: &impl Display) -> String {
    if cfg!(feature = "ducklake-query-error-details") {
        error.to_string()
    } else {
        "query error details disabled".to_owned()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn preserves_original_failure_diagnostics_exactly_when_enabled() {
        let original = "Conversion Error: could not convert string 'O''Brien' to INT32; SQL: \
                        UPDATE \"lake\".\"events\" SET value = 'O''Brien' WHERE id = \
                        '01a085dc-f003-7ee2-9477-31550487b0f1';";
        let actual = query_log_detail(&original);
        if cfg!(feature = "ducklake-query-error-details") {
            assert_eq!(actual, original);
        } else {
            assert_eq!(actual, "query error details disabled");
        }
    }
}
