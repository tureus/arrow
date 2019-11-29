use crate::schema::types::Type;

pub trait RecordSchema {
    fn schema() -> Type;
}
