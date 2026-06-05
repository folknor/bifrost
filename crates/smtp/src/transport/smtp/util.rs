//! Utils for string manipulation

use std::fmt::{Display, Formatter, Result as FmtResult};

/// Encode a string as xtext
#[derive(Debug)]
pub(crate) struct XText<'a>(pub(crate) &'a str);

impl Display for XText<'_> {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        for &byte in self.0.as_bytes() {
            if (b'!'..=b'~').contains(&byte) && byte != b'+' && byte != b'=' {
                write!(f, "{}", byte as char)?;
            } else {
                write!(f, "+{byte:02X}")?;
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::XText;

    #[test]
    fn test() {
        for (input, expect) in [
            ("bjorn", "bjorn"),
            ("bjørn", "bj+C3+B8rn"),
            ("Ø+= ‰", "+C3+98+2B+3D+20+E2+80+B0"),
            ("+", "+2B"),
            ("\0", "+00"),
        ] {
            assert_eq!(format!("{}", XText(input)), (*expect).to_owned());
        }
    }
}
