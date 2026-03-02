//! This module defines the request fingerprint type

use xxhash_rust::xxh3;

///  A request fingerprint computed from the method, route and response's body
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct Fingerprint(pub(crate) u64);

/// Fingerprint computation strategy
pub trait FingerprintStrategy: Send + Sync + 'static {
    /// Compute fingerprint
    fn compute(&self, operation: &str, body: &[u8]) -> Fingerprint;
}

/// The default fingerprint strategy
pub struct DefaultFingerprintStrategy;

impl FingerprintStrategy for DefaultFingerprintStrategy {
    fn compute(&self, operation: &str, body: &[u8]) -> Fingerprint {
        let inner = xxh3::xxh3_64(&[operation.as_bytes(), b":", body].concat());
        Fingerprint(inner)
    }
}

#[cfg(test)]
mod tests {
    use googletest::matchers::{eq, not};
    use googletest::{expect_that, gtest};
    use proptest::strategy::Strategy;
    use proptest::{arbitrary, collection, proptest};

    use super::*;

    proptest! {
        #[gtest]
        fn fingerprint_is_deterministic(
            operation in "\\PC+",
            body in collection::vec(arbitrary::any::<u8>(), 0..4096)
        ) {

            let strat = DefaultFingerprintStrategy;
            let a = strat.compute(&operation, &body);
            let b = strat.compute(&operation, &body);
            expect_that!(a, eq(b));
        }

        #[gtest]
        fn fingerprint_is_sensitive_to_operation(
            (op_a, op_b) in ("\\PC+", "\\PC+")
                .prop_filter(
                    "value must be distinct",
                    |(op_a, op_b)| op_a != op_b
                ),
                body in collection::vec(arbitrary::any::<u8>(), 0..1024)
        ) {
            let strat = DefaultFingerprintStrategy;
            let f1 = strat.compute(&op_a, &body);
            let f2 = strat.compute(&op_b, &body);
            expect_that!(f1, not(eq(f2)));
        }

        #[gtest]
        fn fingerprint_is_sensitive_to_body(
            op in "\\PC+",
            (body_a, body_b) in (
                collection::vec(arbitrary::any::<u8>(), 0..512),
                collection::vec(arbitrary::any::<u8>(), 0..512)
            ).prop_filter(
                "body must be distinct",
                |(a, b)| a != b
            )
        ) {
            let strat = DefaultFingerprintStrategy;
            let f1 = strat.compute(&op, &body_a);
            let f2 = strat.compute(&op, &body_b);
            expect_that!(f1, not(eq(f2)));
        }

    }

    #[gtest]
    fn field_sepration_prevent_collision() {
        let strat = DefaultFingerprintStrategy;
        let f1 = strat.compute("GET/ab", b"");
        let f2 = strat.compute("GET", b"/ab");
        expect_that!(f1, not(eq(f2)));
    }
}
