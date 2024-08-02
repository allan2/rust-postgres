use jiff_01::{civil::Date, civil::DateTime, civil::Time, Timestamp};
use std::fmt;
use tokio_postgres::types::{self, FromSqlOwned};
use tokio_postgres::Client;

use crate::connect;
use crate::types::test_type;

#[tokio::test]
async fn test_civil_datetime_params() {
    fn make_check(time: &str) -> (Option<DateTime>, &str) {
        (
            Some(DateTime::strptime("'%Y-%m-%d %H:%M:%S.%f'", time).unwrap()),
            time,
        )
    }
    test_type(
        "TIMESTAMP",
        &[
            make_check("'1970-01-01 00:00:00.010000000'"),
            make_check("'1965-09-25 11:19:33.100314000'"),
            make_check("'2010-02-09 23:11:45.120200000'"),
            (None, "NULL"),
        ],
    )
    .await;
}

#[tokio::test]
async fn test_with_special_civil_datetime_params() {
    fn make_check(time: &str) -> (types::Timestamp<DateTime>, &str) {
        (
            types::Timestamp::Value(DateTime::strptime("'%Y-%m-%d %H:%M:%S.%f'", time).unwrap()),
            time,
        )
    }
    test_type(
        "TIMESTAMP",
        &[
            make_check("'1970-01-01 00:00:00.010000000'"),
            make_check("'1965-09-25 11:19:33.100314000'"),
            make_check("'2010-02-09 23:11:45.120200000'"),
            (types::Timestamp::PosInfinity, "'infinity'"),
            (types::Timestamp::NegInfinity, "'-infinity'"),
        ],
    )
    .await;
}

#[tokio::test]
async fn test_timestamp_params() {
    fn make_check(time: &str) -> (Option<Timestamp>, &str) {
        (
            Some(Timestamp::strptime("'%Y-%m-%d %H:%M:%S.%f %z'", time).unwrap()),
            time,
        )
    }
    test_type(
        "TIMESTAMP WITH TIME ZONE",
        &[
            make_check("'1970-01-01 00:00:00.010000000 +0000'"),
            make_check("'1965-09-25 11:19:33.100314000 +0000'"),
            make_check("'2010-02-09 23:11:45.120200000 +0000'"),
            make_check("'2010-11-20 17:11:45.120200000 +0500'"),
            (None, "NULL"),
        ],
    )
    .await;
}

#[tokio::test]
async fn test_with_special_timestamp_params() {
    fn make_check(time: &str) -> (types::Timestamp<Timestamp>, &str) {
        (
            types::Timestamp::Value(
                Timestamp::strptime("'%Y-%m-%d %H:%M:%S.%f %z'", time).unwrap(),
            ),
            time,
        )
    }
    test_type(
        "TIMESTAMP WITH TIME ZONE",
        &[
            make_check("'1970-01-01 00:00:00.010000000 +0000'"),
            make_check("'1965-09-25 11:19:33.100314000 +0000'"),
            make_check("'2010-02-09 23:11:45.120200000 +0000'"),
            (types::Timestamp::PosInfinity, "'infinity'"),
            (types::Timestamp::NegInfinity, "'-infinity'"),
        ],
    )
    .await;
}

#[tokio::test]
async fn test_date_params() {
    fn make_check(time: &str) -> (Option<Date>, &str) {
        (Some(Date::strptime("'%Y-%m-%d'", time).unwrap()), time)
    }
    test_type(
        "DATE",
        &[
            make_check("'1970-01-01'"),
            make_check("'1965-09-25'"),
            make_check("'2010-02-09'"),
            (None, "NULL"),
        ],
    )
    .await;
}

#[tokio::test]
async fn test_with_special_date_params() {
    fn make_check(date: &str) -> (types::Date<Date>, &str) {
        (
            types::Date::Value(Date::strptime("'%Y-%m-%d'", date).unwrap()),
            date,
        )
    }
    test_type(
        "DATE",
        &[
            make_check("'1970-01-01'"),
            make_check("'1965-09-25'"),
            make_check("'2010-02-09'"),
            (types::Date::PosInfinity, "'infinity'"),
            (types::Date::NegInfinity, "'-infinity'"),
        ],
    )
    .await;
}

#[tokio::test]
async fn test_time_params() {
    fn make_check(time: &str) -> (Option<Time>, &str) {
        (Some(Time::strptime("'%H:%M:%S.%f'", time).unwrap()), time)
    }
    test_type(
        "TIME",
        &[
            make_check("'00:00:00.010000000'"),
            make_check("'11:19:33.100314000'"),
            make_check("'23:11:45.120200000'"),
            (None, "NULL"),
        ],
    )
    .await;
}

#[tokio::test]
async fn test_special_params_without_wrapper() {
    async fn assert_overflows<T>(client: &mut Client, val: &str, sql_type: &str)
    where
        T: FromSqlOwned + fmt::Debug,
    {
        let err = client
            .query_one(&*format!("SELECT {}::{}", val, sql_type), &[])
            .await
            .unwrap()
            .try_get::<_, T>(0)
            .unwrap_err();
        // Jiff's Error type has limited introspection so I am being dirty here
        let display_string = format!("{}", err);
        assert!(display_string.contains("is not in the required range of"));
    }

    let mut client = connect("user=postgres").await;

    assert_overflows::<Timestamp>(&mut client, "'-infinity'", "timestamptz").await;
    assert_overflows::<Timestamp>(&mut client, "'infinity'", "timestamptz").await;

    assert_overflows::<DateTime>(&mut client, "'-infinity'", "timestamp").await;
    assert_overflows::<DateTime>(&mut client, "'infinity'", "timestamp").await;

    assert_overflows::<Date>(&mut client, "'-infinity'", "date").await;
    assert_overflows::<Date>(&mut client, "'infinity'", "date").await;
}
