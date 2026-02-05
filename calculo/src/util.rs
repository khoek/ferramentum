#[inline(always)]
pub fn strict_mul(lhs: usize, rhs: usize, context: &str) -> usize {
    debug_assert!(
        rhs == 0 || lhs <= usize::MAX / rhs,
        "{} (lhs={}, rhs={})",
        context,
        lhs,
        rhs
    );
    lhs * rhs
}

#[inline(always)]
pub fn strict_sub(lhs: usize, rhs: usize, context: &str) -> usize {
    debug_assert!(lhs >= rhs, "{} (lhs={}, rhs={})", context, lhs, rhs);
    lhs - rhs
}
