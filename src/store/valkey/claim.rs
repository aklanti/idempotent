use redis::FromRedisValue;
use redis::ParsingError;
use redis::Value;

use crate::fencing_token::FencingToken;

/// A decoded reply from `claim.lua`
pub enum ClaimReply {
    Created { fencing_token: FencingToken },
    InProgress { data: Vec<u8> },
    Complete { data: Vec<u8> },
}

impl FromRedisValue for ClaimReply {
    fn from_redis_value(v: Value) -> Result<Self, ParsingError> {
        let (status, run_id, sequence, data): (String, Option<String>, Option<String>, Vec<u8>) =
            FromRedisValue::from_redis_value(v)?;

        let reply = match status.as_str() {
            "created" => {
                let run_id = run_id
                    .and_then(|hex| u64::from_str_radix(&hex, 16).ok())
                    .ok_or_else(|| ParsingError::from("malformed token run id"))?;
                let sequence = sequence
                    .and_then(|digits| digits.parse().ok())
                    .ok_or_else(|| ParsingError::from("malformed token sequence"))?;
                Self::Created {
                    fencing_token: FencingToken::new(run_id, sequence),
                }
            }

            "in_progress" => Self::InProgress { data },
            "complete" => Self::Complete { data },
            _ => {
                return Err(ParsingError::from("unknown claim status"));
            }
        };

        Ok(reply)
    }
}
