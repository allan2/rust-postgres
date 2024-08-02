use bytes::BytesMut;
use jiff_01::{
    civil::{Date, DateTime, Time},
    Span, Timestamp, Unit,
};
use postgres_protocol::types;
use std::error::Error;

use crate::{FromSql, IsNull, ToSql, Type};

const fn base() -> DateTime {
    DateTime::constant(2000, 1, 1, 0, 0, 0, 0)
}

/// The number of seconds from 2000-01-01 00:00:00 UTC to the Unix epoch.
const Y2K_EPOCH: i64 = 946684800;

fn base_ts() -> Timestamp {
    Timestamp::new(Y2K_EPOCH, 0).unwrap()
}

impl<'a> FromSql<'a> for DateTime {
    fn from_sql(_: &Type, raw: &[u8]) -> Result<DateTime, Box<dyn Error + Sync + Send>> {
        let t = types::timestamp_from_sql(raw)?;
        Ok(base().checked_add(Span::new().try_microseconds(t)?)?)
    }

    accepts!(TIMESTAMP);
}

impl ToSql for DateTime {
    fn to_sql(&self, _: &Type, w: &mut BytesMut) -> Result<IsNull, Box<dyn Error + Sync + Send>> {
        types::timestamp_to_sql(self.since(base())?.total(Unit::Microsecond)? as i64, w);
        Ok(IsNull::No)
    }

    accepts!(TIMESTAMP);
    to_sql_checked!();
}

impl<'a> FromSql<'a> for Timestamp {
    fn from_sql(_: &Type, raw: &[u8]) -> Result<Timestamp, Box<dyn Error + Sync + Send>> {
        let t = types::timestamp_from_sql(raw)?;
        Ok(base_ts().checked_add(Span::new().try_microseconds(t)?)?)
    }

    accepts!(TIMESTAMPTZ);
}

impl ToSql for Timestamp {
    fn to_sql(&self, _: &Type, w: &mut BytesMut) -> Result<IsNull, Box<dyn Error + Sync + Send>> {
        types::timestamp_to_sql(self.since(base_ts())?.total(Unit::Microsecond)? as i64, w);
        Ok(IsNull::No)
    }

    accepts!(TIMESTAMPTZ);
    to_sql_checked!();
}

impl<'a> FromSql<'a> for Date {
    fn from_sql(_: &Type, raw: &[u8]) -> Result<Date, Box<dyn Error + Sync + Send>> {
        let jd = types::date_from_sql(raw)?;
        Ok(base().date().checked_add(Span::new().try_days(jd)?)?)
    }

    accepts!(DATE);
}

impl ToSql for Date {
    fn to_sql(&self, _: &Type, w: &mut BytesMut) -> Result<IsNull, Box<dyn Error + Sync + Send>> {
        let jd = self.since(base())?.total(Unit::Day)? as i32;
        types::date_to_sql(jd, w);
        Ok(IsNull::No)
    }

    accepts!(DATE);
    to_sql_checked!();
}

impl<'a> FromSql<'a> for Time {
    fn from_sql(_: &Type, raw: &[u8]) -> Result<Time, Box<dyn Error + Sync + Send>> {
        let usec = types::time_from_sql(raw)?;
        Ok(Time::midnight() + Span::new().try_microseconds(usec)?)
    }

    accepts!(TIME);
}

impl ToSql for Time {
    fn to_sql(&self, _: &Type, w: &mut BytesMut) -> Result<IsNull, Box<dyn Error + Sync + Send>> {
        let delta = self.since(Time::midnight())?;
        types::time_to_sql(delta.total(Unit::Microsecond)? as i64, w);
        Ok(IsNull::No)
    }

    accepts!(TIME);
    to_sql_checked!();
}
