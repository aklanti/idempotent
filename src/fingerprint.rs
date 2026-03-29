use xxhash_rust::xxh3;

/// A hash of the request operation and body.
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct Fingerprint(pub(crate) u128);

#[cfg(feature = "valkey")]
const _: () = {
    use redis::RedisWrite;
    use redis::ToRedisArgs;
    impl ToRedisArgs for Fingerprint {
        fn write_redis_args<W: ?Sized + RedisWrite>(&self, out: &mut W) {
            self.0.write_redis_args(out);
        }
    }
};

/// Trait for computing request fingerprints.
pub trait FingerprintStrategy: Send + Sync + 'static {
    /// Computes a fingerprint from `operation` and `body`.
    fn compute(&self, operation: &str, body: &[u8]) -> Fingerprint;
}

/// Default strategy using xxHash3.
pub struct DefaultFingerprintStrategy;

impl FingerprintStrategy for DefaultFingerprintStrategy {
    fn compute(&self, operation: &str, body: &[u8]) -> Fingerprint {
        let mut hasher = xxh3::Xxh3::new();

        hasher.update(&(operation.len() as u64).to_le_bytes());
        hasher.update(operation.as_bytes());
        hasher.update(body);
        Fingerprint(hasher.digest128())
    }
}

#[cfg(test)]
mod tests {
    use googletest::expect_that;
    use googletest::gtest;
    use googletest::matchers::eq;
    use googletest::matchers::not;
    use proptest::arbitrary;
    use proptest::collection;
    use proptest::proptest;
    use proptest::strategy::Strategy;

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
