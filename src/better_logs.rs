use chrono::Local;
use std::io::{self, Write};

pub fn __logerr_with_loc(
    file: &str,
    line: u32,
    args: std::fmt::Arguments
) {
    let now       = Local::now();
    let timestamp = now.format("%Y/%m/%d %H:%M:%S");
    let _ = writeln!(
        io::stderr(),
        "</ERROR/> {} [{}:{}] {}",
        timestamp,
        file,
        line,
        args
    );
}

#[macro_export]
macro_rules! logerr {
    ($($arg:tt)*) => {{
        $crate::better_logs::__logerr_with_loc(
            file!(),
            line!(),
            ::std::format_args!($($arg)*),
        )
    }};
}



pub fn __loginfo_with_loc(
    file: &str,
    line: u32,
    args: std::fmt::Arguments
) {
    let now       = Local::now();
    let timestamp = now.format("%Y/%m/%d %H:%M:%S");
    let _ = writeln!(
        io::stdout(),
        "</INFO/> {} [{}:{}] {}",
        timestamp,
        file,
        line,
        args
    );
}

#[macro_export]
macro_rules! loginfo {
    ($($arg:tt)*) => {{
        $crate::better_logs::__loginfo_with_loc(
            file!(),
            line!(),
            ::std::format_args!($($arg)*),
        )
    }};
}
