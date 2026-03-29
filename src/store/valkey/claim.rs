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
        let (status, ft, data): (String, i64, Vec<u8>) = FromRedisValue::from_redis_value(v)?;

        let reply = match status.as_str() {
            "created" => {
                let fencing_token = ft
                    .try_into()
                    .map_err(|_| ParsingError::from("negative fencing token"))?;
                Self::Created { fencing_token }
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
