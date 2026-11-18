//! Go encoding/json null behavior for workflow request fields, not shared auth decoding.
use serde::{Deserialize, Deserializer};

pub(crate) fn null_default<'de, D, T>(decoder: D) -> Result<T, D::Error>
where
    D: Deserializer<'de>,
    T: Deserialize<'de> + Default,
{
    Ok(Option::<T>::deserialize(decoder)?.unwrap_or_default())
}
