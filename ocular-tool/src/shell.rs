pub use capulus::shell::shell_quote as sh_quote;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sh_quote_empty() {
        assert_eq!(sh_quote(""), "''");
    }

    #[test]
    fn sh_quote_no_single_quotes() {
        assert_eq!(sh_quote("abc def"), "'abc def'");
    }

    #[test]
    fn sh_quote_with_single_quote() {
        assert_eq!(sh_quote("a'b"), "'a'\"'\"'b'");
    }
}
