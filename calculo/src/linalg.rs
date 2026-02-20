use crate::num::Num;

#[cfg(feature = "dashu")]
use crate::num::DashuRat;
#[cfg(feature = "dashu")]
use dashu_ratio::RBig;

pub trait LinAlgOps<N: Num> {
    fn add_mul_assign(acc: &mut N, factor: &N, rhs: &N);
    fn sub_mul_assign(acc: &mut N, factor: &N, rhs: &N);

    fn dot(lhs: &[N], rhs: &[N]) -> N;

    fn dot2(lhs: &[N], rhs_a: &[N], rhs_b: &[N]) -> (N, N) {
        (Self::dot(lhs, rhs_a), Self::dot(lhs, rhs_b))
    }

    fn axpy_add(dst: &mut [N], factor: &N, rhs: &[N]);
    fn axpy_sub(dst: &mut [N], factor: &N, rhs: &[N]);

    fn scale_assign(dst: &mut [N], factor: &N);
    fn div_assign(dst: &mut [N], divisor: &N);

    fn lin_comb2_into(out: &mut [N], lhs: &[N], lhs_factor: &N, rhs: &[N], rhs_factor: &N);
}

pub struct GenericOps;

impl<N: Num> LinAlgOps<N> for GenericOps {
    #[inline(always)]
    fn add_mul_assign(acc: &mut N, factor: &N, rhs: &N) {
        let tmp = factor.ref_mul(rhs);
        *acc = acc.ref_add(&tmp);
    }

    #[inline(always)]
    fn sub_mul_assign(acc: &mut N, factor: &N, rhs: &N) {
        let tmp = factor.ref_mul(rhs);
        *acc = acc.ref_sub(&tmp);
    }

    #[inline(always)]
    fn dot(lhs: &[N], rhs: &[N]) -> N {
        debug_assert_eq!(lhs.len(), rhs.len(), "dot product dimension mismatch");
        let mut acc = N::zero();
        for (a, b) in lhs.iter().zip(rhs.iter()) {
            Self::add_mul_assign(&mut acc, a, b);
        }
        acc
    }

    #[inline(always)]
    fn dot2(lhs: &[N], rhs_a: &[N], rhs_b: &[N]) -> (N, N) {
        debug_assert_eq!(
            lhs.len(),
            rhs_a.len(),
            "dot product dimension mismatch (lhs vs rhs_a)"
        );
        debug_assert_eq!(
            lhs.len(),
            rhs_b.len(),
            "dot product dimension mismatch (lhs vs rhs_b)"
        );
        let mut acc_a = N::zero();
        let mut acc_b = N::zero();
        for (a, (b, c)) in lhs.iter().zip(rhs_a.iter().zip(rhs_b.iter())) {
            Self::add_mul_assign(&mut acc_a, a, b);
            Self::add_mul_assign(&mut acc_b, a, c);
        }
        (acc_a, acc_b)
    }

    #[inline(always)]
    fn axpy_add(dst: &mut [N], factor: &N, rhs: &[N]) {
        debug_assert_eq!(dst.len(), rhs.len(), "axpy dimension mismatch");
        for (dst_i, rhs_i) in dst.iter_mut().zip(rhs.iter()) {
            Self::add_mul_assign(dst_i, factor, rhs_i);
        }
    }

    #[inline(always)]
    fn axpy_sub(dst: &mut [N], factor: &N, rhs: &[N]) {
        debug_assert_eq!(dst.len(), rhs.len(), "axpy dimension mismatch");
        for (dst_i, rhs_i) in dst.iter_mut().zip(rhs.iter()) {
            Self::sub_mul_assign(dst_i, factor, rhs_i);
        }
    }

    #[inline(always)]
    fn scale_assign(dst: &mut [N], factor: &N) {
        for dst_i in dst.iter_mut() {
            *dst_i = dst_i.ref_mul(factor);
        }
    }

    #[inline(always)]
    fn div_assign(dst: &mut [N], divisor: &N) {
        for dst_i in dst.iter_mut() {
            *dst_i = dst_i.ref_div(divisor);
        }
    }

    #[inline(always)]
    fn lin_comb2_into(out: &mut [N], lhs: &[N], lhs_factor: &N, rhs: &[N], rhs_factor: &N) {
        debug_assert_eq!(out.len(), lhs.len(), "lin_comb2 dimension mismatch");
        debug_assert_eq!(out.len(), rhs.len(), "lin_comb2 dimension mismatch");
        for ((out_i, lhs_i), rhs_i) in out.iter_mut().zip(lhs.iter()).zip(rhs.iter()) {
            let mut acc = lhs_i.ref_mul(lhs_factor);
            Self::add_mul_assign(&mut acc, rhs_factor, rhs_i);
            *out_i = acc;
        }
    }
}

pub struct F64Ops;

impl LinAlgOps<f64> for F64Ops {
    #[inline(always)]
    fn add_mul_assign(acc: &mut f64, factor: &f64, rhs: &f64) {
        *acc = factor.mul_add(*rhs, *acc);
    }

    #[inline(always)]
    fn sub_mul_assign(acc: &mut f64, factor: &f64, rhs: &f64) {
        *acc = (-*factor).mul_add(*rhs, *acc);
    }

    #[inline(always)]
    fn dot(lhs: &[f64], rhs: &[f64]) -> f64 {
        simd::fused_dot(lhs, rhs)
    }

    #[inline(always)]
    fn dot2(lhs: &[f64], rhs_a: &[f64], rhs_b: &[f64]) -> (f64, f64) {
        simd::fused_dot2(lhs, rhs_a, rhs_b)
    }

    #[inline(always)]
    fn axpy_add(dst: &mut [f64], factor: &f64, rhs: &[f64]) {
        simd::fused_add_mul_slice_assign(dst, *factor, rhs);
    }

    #[inline(always)]
    fn axpy_sub(dst: &mut [f64], factor: &f64, rhs: &[f64]) {
        simd::fused_add_mul_slice_assign(dst, -*factor, rhs);
    }

    #[inline(always)]
    fn scale_assign(dst: &mut [f64], factor: &f64) {
        simd::mul_slice_assign(dst, *factor);
    }

    #[inline(always)]
    fn div_assign(dst: &mut [f64], divisor: &f64) {
        simd::div_slice_assign(dst, *divisor);
    }

    #[inline(always)]
    fn lin_comb2_into(
        out: &mut [f64],
        lhs: &[f64],
        lhs_factor: &f64,
        rhs: &[f64],
        rhs_factor: &f64,
    ) {
        simd::lin_comb2_into(out, lhs, *lhs_factor, rhs, *rhs_factor);
    }
}

#[cfg(feature = "rug")]
pub struct RugRatOps;

#[cfg(feature = "rug")]
impl LinAlgOps<crate::num::RugRat> for RugRatOps {
    #[inline(always)]
    fn add_mul_assign(
        acc: &mut crate::num::RugRat,
        factor: &crate::num::RugRat,
        rhs: &crate::num::RugRat,
    ) {
        use rug::Assign;

        let mut mul = rug::Rational::new();
        mul.assign(&factor.0 * &rhs.0);
        acc.0 += &mul;
    }

    #[inline(always)]
    fn sub_mul_assign(
        acc: &mut crate::num::RugRat,
        factor: &crate::num::RugRat,
        rhs: &crate::num::RugRat,
    ) {
        use rug::Assign;

        let mut mul = rug::Rational::new();
        mul.assign(&factor.0 * &rhs.0);
        acc.0 -= &mul;
    }

    #[inline(always)]
    fn dot(lhs: &[crate::num::RugRat], rhs: &[crate::num::RugRat]) -> crate::num::RugRat {
        debug_assert_eq!(lhs.len(), rhs.len(), "dot product dimension mismatch");
        use rug::Assign;

        let one = rug::Integer::from(1);
        let mut acc_i = rug::Integer::new();
        acc_i.assign(0);
        let mut mul_i = rug::Integer::new();
        let mut all_integral = true;
        for (a, b) in lhs.iter().zip(rhs.iter()) {
            if a.0.is_zero() || b.0.is_zero() {
                continue;
            }
            if a.0.denom() != &one || b.0.denom() != &one {
                all_integral = false;
                break;
            }
            mul_i.assign(a.0.numer());
            mul_i *= b.0.numer();
            acc_i += &mul_i;
        }
        if all_integral {
            return crate::num::RugRat(rug::Rational::from(acc_i));
        }

        let mut acc = rug::Rational::new();
        acc.assign(0);
        let mut mul = rug::Rational::new();
        for (a, b) in lhs.iter().zip(rhs.iter()) {
            if a.0.is_zero() || b.0.is_zero() {
                continue;
            }
            mul.assign(&a.0 * &b.0);
            acc += &mul;
        }
        crate::num::RugRat(acc)
    }

    #[inline(always)]
    fn dot2(
        lhs: &[crate::num::RugRat],
        rhs_a: &[crate::num::RugRat],
        rhs_b: &[crate::num::RugRat],
    ) -> (crate::num::RugRat, crate::num::RugRat) {
        debug_assert_eq!(
            lhs.len(),
            rhs_a.len(),
            "dot product dimension mismatch (lhs vs rhs_a)"
        );
        debug_assert_eq!(
            lhs.len(),
            rhs_b.len(),
            "dot product dimension mismatch (lhs vs rhs_b)"
        );
        use rug::Assign;

        let one = rug::Integer::from(1);
        let mut acc_a_i = rug::Integer::new();
        acc_a_i.assign(0);
        let mut acc_b_i = rug::Integer::new();
        acc_b_i.assign(0);
        let mut mul_i = rug::Integer::new();
        let mut all_integral = true;
        'integral: for (a, (b, c)) in lhs.iter().zip(rhs_a.iter().zip(rhs_b.iter())) {
            if a.0.is_zero() {
                continue;
            }
            if a.0.denom() != &one {
                all_integral = false;
                break;
            }
            if !b.0.is_zero() {
                if b.0.denom() != &one {
                    all_integral = false;
                    break 'integral;
                }
                mul_i.assign(a.0.numer());
                mul_i *= b.0.numer();
                acc_a_i += &mul_i;
            }
            if !c.0.is_zero() {
                if c.0.denom() != &one {
                    all_integral = false;
                    break 'integral;
                }
                mul_i.assign(a.0.numer());
                mul_i *= c.0.numer();
                acc_b_i += &mul_i;
            }
        }
        if all_integral {
            return (
                crate::num::RugRat(rug::Rational::from(acc_a_i)),
                crate::num::RugRat(rug::Rational::from(acc_b_i)),
            );
        }

        let mut acc_a = rug::Rational::new();
        acc_a.assign(0);
        let mut acc_b = rug::Rational::new();
        acc_b.assign(0);
        let mut mul = rug::Rational::new();
        for (a, (b, c)) in lhs.iter().zip(rhs_a.iter().zip(rhs_b.iter())) {
            if !a.0.is_zero() && !b.0.is_zero() {
                mul.assign(&a.0 * &b.0);
                acc_a += &mul;
            }
            if !a.0.is_zero() && !c.0.is_zero() {
                mul.assign(&a.0 * &c.0);
                acc_b += &mul;
            }
        }
        (crate::num::RugRat(acc_a), crate::num::RugRat(acc_b))
    }

    #[inline(always)]
    fn axpy_add(
        dst: &mut [crate::num::RugRat],
        factor: &crate::num::RugRat,
        rhs: &[crate::num::RugRat],
    ) {
        use rug::Assign;

        debug_assert_eq!(dst.len(), rhs.len(), "axpy dimension mismatch");
        if factor.0.is_zero() {
            return;
        }

        let mut mul = rug::Rational::new();
        for (dst_i, rhs_i) in dst.iter_mut().zip(rhs.iter()) {
            mul.assign(&factor.0 * &rhs_i.0);
            dst_i.0 += &mul;
        }
    }

    #[inline(always)]
    fn axpy_sub(
        dst: &mut [crate::num::RugRat],
        factor: &crate::num::RugRat,
        rhs: &[crate::num::RugRat],
    ) {
        use rug::Assign;

        debug_assert_eq!(dst.len(), rhs.len(), "axpy dimension mismatch");
        if factor.0.is_zero() {
            return;
        }

        let mut mul = rug::Rational::new();
        for (dst_i, rhs_i) in dst.iter_mut().zip(rhs.iter()) {
            mul.assign(&factor.0 * &rhs_i.0);
            dst_i.0 -= &mul;
        }
    }

    #[inline(always)]
    fn scale_assign(dst: &mut [crate::num::RugRat], factor: &crate::num::RugRat) {
        use rug::Assign;

        if factor.0.is_zero() {
            for dst_i in dst.iter_mut() {
                dst_i.0.assign(0);
            }
            return;
        }
        for dst_i in dst.iter_mut() {
            dst_i.0 *= &factor.0;
        }
    }

    #[inline(always)]
    fn div_assign(dst: &mut [crate::num::RugRat], divisor: &crate::num::RugRat) {
        for dst_i in dst.iter_mut() {
            dst_i.0 /= &divisor.0;
        }
    }

    #[inline(always)]
    fn lin_comb2_into(
        out: &mut [crate::num::RugRat],
        lhs: &[crate::num::RugRat],
        lhs_factor: &crate::num::RugRat,
        rhs: &[crate::num::RugRat],
        rhs_factor: &crate::num::RugRat,
    ) {
        use rug::Assign;

        debug_assert_eq!(out.len(), lhs.len(), "lin_comb2 dimension mismatch");
        debug_assert_eq!(out.len(), rhs.len(), "lin_comb2 dimension mismatch");

        let one = rug::Integer::from(1);
        if lhs_factor.0.denom() == &one && rhs_factor.0.denom() == &one {
            let mut acc_i = rug::Integer::new();
            acc_i.assign(0);
            let mut mul_i = rug::Integer::new();
            let lhsf = lhs_factor.0.numer();
            let rhsf = rhs_factor.0.numer();
            let mut all_integral = true;
            for ((out_i, lhs_i), rhs_i) in out.iter_mut().zip(lhs.iter()).zip(rhs.iter()) {
                if lhs_i.0.denom() != &one || rhs_i.0.denom() != &one {
                    all_integral = false;
                    break;
                }
                acc_i.assign(0);
                if !lhs_i.0.is_zero() {
                    mul_i.assign(lhs_i.0.numer());
                    mul_i *= lhsf;
                    acc_i += &mul_i;
                }
                if !rhs_i.0.is_zero() {
                    mul_i.assign(rhs_i.0.numer());
                    mul_i *= rhsf;
                    acc_i += &mul_i;
                }
                out_i.0.assign(&acc_i);
            }
            if all_integral {
                return;
            }
        }

        let lhs_is_one = lhs_factor.0 == 1;
        let rhs_is_one = rhs_factor.0 == 1;
        let lhs_is_zero = lhs_factor.0.is_zero();
        let rhs_is_zero = rhs_factor.0.is_zero();
        let mut mul = rug::Rational::new();
        for ((out_i, lhs_i), rhs_i) in out.iter_mut().zip(lhs.iter()).zip(rhs.iter()) {
            if lhs_is_zero || lhs_i.0.is_zero() {
                if rhs_is_zero || rhs_i.0.is_zero() {
                    out_i.0.assign(0);
                } else if rhs_is_one {
                    out_i.0.assign(&rhs_i.0);
                } else {
                    out_i.0.assign(&rhs_i.0 * &rhs_factor.0);
                }
                continue;
            }

            if lhs_is_one {
                out_i.0.assign(&lhs_i.0);
            } else {
                out_i.0.assign(&lhs_i.0 * &lhs_factor.0);
            }

            if rhs_is_zero || rhs_i.0.is_zero() {
                continue;
            }
            if rhs_is_one {
                out_i.0 += &rhs_i.0;
            } else {
                mul.assign(&rhs_i.0 * &rhs_factor.0);
                out_i.0 += &mul;
            }
        }
    }
}

#[cfg(feature = "dashu")]
pub struct DashuRatOps;

#[cfg(feature = "dashu")]
impl LinAlgOps<DashuRat> for DashuRatOps {
    #[inline(always)]
    fn add_mul_assign(acc: &mut DashuRat, factor: &DashuRat, rhs: &DashuRat) {
        if factor.0.is_zero() || rhs.0.is_zero() {
            return;
        }
        let mut mul = factor.0.clone();
        mul *= &rhs.0;
        acc.0 += &mul;
    }

    #[inline(always)]
    fn sub_mul_assign(acc: &mut DashuRat, factor: &DashuRat, rhs: &DashuRat) {
        if factor.0.is_zero() || rhs.0.is_zero() {
            return;
        }
        let mut mul = factor.0.clone();
        mul *= &rhs.0;
        acc.0 -= &mul;
    }

    #[inline(always)]
    fn dot(lhs: &[DashuRat], rhs: &[DashuRat]) -> DashuRat {
        debug_assert_eq!(lhs.len(), rhs.len(), "dot product dimension mismatch");

        let mut acc = RBig::ZERO;
        let mut mul = RBig::ZERO;
        for (a, b) in lhs.iter().zip(rhs.iter()) {
            if a.0.is_zero() || b.0.is_zero() {
                continue;
            }
            mul.clone_from(&a.0);
            mul *= &b.0;
            acc += &mul;
        }
        DashuRat(acc)
    }

    #[inline(always)]
    fn dot2(lhs: &[DashuRat], rhs_a: &[DashuRat], rhs_b: &[DashuRat]) -> (DashuRat, DashuRat) {
        debug_assert_eq!(
            lhs.len(),
            rhs_a.len(),
            "dot product dimension mismatch (lhs vs rhs_a)"
        );
        debug_assert_eq!(
            lhs.len(),
            rhs_b.len(),
            "dot product dimension mismatch (lhs vs rhs_b)"
        );

        let mut acc_a = RBig::ZERO;
        let mut acc_b = RBig::ZERO;
        let mut mul = RBig::ZERO;
        for (a, (b, c)) in lhs.iter().zip(rhs_a.iter().zip(rhs_b.iter())) {
            if a.0.is_zero() {
                continue;
            }
            if !b.0.is_zero() {
                mul.clone_from(&a.0);
                mul *= &b.0;
                acc_a += &mul;
            }
            if !c.0.is_zero() {
                mul.clone_from(&a.0);
                mul *= &c.0;
                acc_b += &mul;
            }
        }
        (DashuRat(acc_a), DashuRat(acc_b))
    }

    #[inline(always)]
    fn axpy_add(dst: &mut [DashuRat], factor: &DashuRat, rhs: &[DashuRat]) {
        debug_assert_eq!(dst.len(), rhs.len(), "axpy dimension mismatch");
        if factor.0.is_zero() {
            return;
        }

        let mut mul = RBig::ZERO;
        for (dst_i, rhs_i) in dst.iter_mut().zip(rhs.iter()) {
            if rhs_i.0.is_zero() {
                continue;
            }
            mul.clone_from(&factor.0);
            mul *= &rhs_i.0;
            dst_i.0 += &mul;
        }
    }

    #[inline(always)]
    fn axpy_sub(dst: &mut [DashuRat], factor: &DashuRat, rhs: &[DashuRat]) {
        debug_assert_eq!(dst.len(), rhs.len(), "axpy dimension mismatch");
        if factor.0.is_zero() {
            return;
        }

        let mut mul = RBig::ZERO;
        for (dst_i, rhs_i) in dst.iter_mut().zip(rhs.iter()) {
            if rhs_i.0.is_zero() {
                continue;
            }
            mul.clone_from(&factor.0);
            mul *= &rhs_i.0;
            dst_i.0 -= &mul;
        }
    }

    #[inline(always)]
    fn scale_assign(dst: &mut [DashuRat], factor: &DashuRat) {
        if factor.0.is_one() {
            return;
        }
        if factor.0.is_zero() {
            for dst_i in dst.iter_mut() {
                dst_i.0 = RBig::ZERO;
            }
            return;
        }
        for dst_i in dst.iter_mut() {
            dst_i.0 *= &factor.0;
        }
    }

    #[inline(always)]
    fn div_assign(dst: &mut [DashuRat], divisor: &DashuRat) {
        if divisor.0.is_one() {
            return;
        }
        for dst_i in dst.iter_mut() {
            dst_i.0 /= &divisor.0;
        }
    }

    #[inline(always)]
    fn lin_comb2_into(
        out: &mut [DashuRat],
        lhs: &[DashuRat],
        lhs_factor: &DashuRat,
        rhs: &[DashuRat],
        rhs_factor: &DashuRat,
    ) {
        debug_assert_eq!(out.len(), lhs.len(), "lin_comb2 dimension mismatch");
        debug_assert_eq!(out.len(), rhs.len(), "lin_comb2 dimension mismatch");

        let mut mul = RBig::ZERO;
        for ((out_i, lhs_i), rhs_i) in out.iter_mut().zip(lhs.iter()).zip(rhs.iter()) {
            out_i.0.clone_from(&lhs_i.0);
            out_i.0 *= &lhs_factor.0;
            if rhs_factor.0.is_zero() || rhs_i.0.is_zero() {
                continue;
            }
            mul.clone_from(&rhs_i.0);
            mul *= &rhs_factor.0;
            out_i.0 += &mul;
        }
    }
}

#[inline(always)]
pub fn add_mul_assign<N: Num>(acc: &mut N, factor: &N, rhs: &N) {
    <N::LinAlg as LinAlgOps<N>>::add_mul_assign(acc, factor, rhs);
}

#[inline(always)]
pub fn sub_mul_assign<N: Num>(acc: &mut N, factor: &N, rhs: &N) {
    <N::LinAlg as LinAlgOps<N>>::sub_mul_assign(acc, factor, rhs);
}

#[inline(always)]
pub fn dot<N: Num>(lhs: &[N], rhs: &[N]) -> N {
    <N::LinAlg as LinAlgOps<N>>::dot(lhs, rhs)
}

#[inline(always)]
pub fn dot2<N: Num>(lhs: &[N], rhs_a: &[N], rhs_b: &[N]) -> (N, N) {
    <N::LinAlg as LinAlgOps<N>>::dot2(lhs, rhs_a, rhs_b)
}

#[inline(always)]
pub fn axpy_add<N: Num>(dst: &mut [N], factor: &N, rhs: &[N]) {
    <N::LinAlg as LinAlgOps<N>>::axpy_add(dst, factor, rhs);
}

#[inline(always)]
pub fn axpy_sub<N: Num>(dst: &mut [N], factor: &N, rhs: &[N]) {
    <N::LinAlg as LinAlgOps<N>>::axpy_sub(dst, factor, rhs);
}

#[inline(always)]
pub fn scale_assign<N: Num>(dst: &mut [N], factor: &N) {
    <N::LinAlg as LinAlgOps<N>>::scale_assign(dst, factor);
}

#[inline(always)]
pub fn div_assign<N: Num>(dst: &mut [N], divisor: &N) {
    <N::LinAlg as LinAlgOps<N>>::div_assign(dst, divisor);
}

#[inline(always)]
pub fn lin_comb2_into<N: Num>(out: &mut [N], lhs: &[N], lhs_factor: &N, rhs: &[N], rhs_factor: &N) {
    <N::LinAlg as LinAlgOps<N>>::lin_comb2_into(out, lhs, lhs_factor, rhs, rhs_factor);
}

#[cfg(feature = "simd")]
mod simd {
    use std::sync::OnceLock;

    use pulp::{Simd, WithSimd};

    static PULP_ARCH: OnceLock<pulp::Arch> = OnceLock::new();

    #[inline(always)]
    fn pulp_arch() -> pulp::Arch {
        *PULP_ARCH.get_or_init(pulp::Arch::new)
    }

    pub(super) fn fused_dot(lhs: &[f64], rhs: &[f64]) -> f64 {
        debug_assert_eq!(lhs.len(), rhs.len(), "dot product dimension mismatch");
        pulp_arch().dispatch(FusedDot { lhs, rhs })
    }

    struct FusedDot<'a> {
        lhs: &'a [f64],
        rhs: &'a [f64],
    }

    impl<'a> WithSimd for FusedDot<'a> {
        type Output = f64;

        #[inline(always)]
        fn with_simd<S: Simd>(self, simd: S) -> Self::Output {
            let (lhs_chunks, lhs_tail) = S::as_simd_f64s(self.lhs);
            let (rhs_chunks, rhs_tail) = S::as_simd_f64s(self.rhs);
            debug_assert_eq!(lhs_chunks.len(), rhs_chunks.len());
            debug_assert_eq!(lhs_tail.len(), rhs_tail.len());

            let mut acc = simd.splat_f64s(0.0);
            for (a, b) in lhs_chunks.iter().zip(rhs_chunks.iter()) {
                acc = simd.mul_add_f64s(*a, *b, acc);
            }
            let mut sum = simd.reduce_sum_f64s(acc);
            for (a, b) in lhs_tail.iter().zip(rhs_tail.iter()) {
                sum = a.mul_add(*b, sum);
            }
            sum
        }
    }

    pub(super) fn fused_dot2(lhs: &[f64], rhs_a: &[f64], rhs_b: &[f64]) -> (f64, f64) {
        debug_assert_eq!(lhs.len(), rhs_a.len(), "dot product dimension mismatch");
        debug_assert_eq!(lhs.len(), rhs_b.len(), "dot product dimension mismatch");
        pulp_arch().dispatch(FusedDot2 { lhs, rhs_a, rhs_b })
    }

    struct FusedDot2<'a> {
        lhs: &'a [f64],
        rhs_a: &'a [f64],
        rhs_b: &'a [f64],
    }

    impl<'a> WithSimd for FusedDot2<'a> {
        type Output = (f64, f64);

        #[inline(always)]
        fn with_simd<S: Simd>(self, simd: S) -> Self::Output {
            let (lhs_chunks, lhs_tail) = S::as_simd_f64s(self.lhs);
            let (rhs_a_chunks, rhs_a_tail) = S::as_simd_f64s(self.rhs_a);
            let (rhs_b_chunks, rhs_b_tail) = S::as_simd_f64s(self.rhs_b);
            debug_assert_eq!(lhs_chunks.len(), rhs_a_chunks.len());
            debug_assert_eq!(lhs_chunks.len(), rhs_b_chunks.len());
            debug_assert_eq!(lhs_tail.len(), rhs_a_tail.len());
            debug_assert_eq!(lhs_tail.len(), rhs_b_tail.len());

            let mut acc_a = simd.splat_f64s(0.0);
            let mut acc_b = simd.splat_f64s(0.0);
            for ((lhs_i, rhs_a_i), rhs_b_i) in lhs_chunks
                .iter()
                .zip(rhs_a_chunks.iter())
                .zip(rhs_b_chunks.iter())
            {
                acc_a = simd.mul_add_f64s(*lhs_i, *rhs_a_i, acc_a);
                acc_b = simd.mul_add_f64s(*lhs_i, *rhs_b_i, acc_b);
            }
            let mut sum_a = simd.reduce_sum_f64s(acc_a);
            let mut sum_b = simd.reduce_sum_f64s(acc_b);
            for ((lhs_i, rhs_a_i), rhs_b_i) in lhs_tail
                .iter()
                .zip(rhs_a_tail.iter())
                .zip(rhs_b_tail.iter())
            {
                sum_a = lhs_i.mul_add(*rhs_a_i, sum_a);
                sum_b = lhs_i.mul_add(*rhs_b_i, sum_b);
            }
            (sum_a, sum_b)
        }
    }

    pub(super) fn fused_add_mul_slice_assign(dst: &mut [f64], factor: f64, rhs: &[f64]) {
        debug_assert_eq!(dst.len(), rhs.len(), "axpy dimension mismatch");
        pulp_arch().dispatch(AxpyLhs { dst, factor, rhs })
    }

    struct AxpyLhs<'a> {
        dst: &'a mut [f64],
        factor: f64,
        rhs: &'a [f64],
    }

    impl<'a> WithSimd for AxpyLhs<'a> {
        type Output = ();

        #[inline(always)]
        fn with_simd<S: Simd>(self, simd: S) -> Self::Output {
            let factor = self.factor;
            let factor_v = simd.splat_f64s(factor);

            let (dst_chunks, dst_tail) = S::as_mut_simd_f64s(self.dst);
            let (rhs_chunks, rhs_tail) = S::as_simd_f64s(self.rhs);
            debug_assert_eq!(dst_chunks.len(), rhs_chunks.len());
            debug_assert_eq!(dst_tail.len(), rhs_tail.len());

            for (dst_i, rhs_i) in dst_chunks.iter_mut().zip(rhs_chunks.iter()) {
                *dst_i = simd.mul_add_f64s(factor_v, *rhs_i, *dst_i);
            }
            for (dst_i, rhs_i) in dst_tail.iter_mut().zip(rhs_tail.iter()) {
                *dst_i = factor.mul_add(*rhs_i, *dst_i);
            }
        }
    }

    pub(super) fn mul_slice_assign(dst: &mut [f64], factor: f64) {
        pulp_arch().dispatch(MulSlice { dst, factor })
    }

    struct MulSlice<'a> {
        dst: &'a mut [f64],
        factor: f64,
    }

    impl<'a> WithSimd for MulSlice<'a> {
        type Output = ();

        #[inline(always)]
        fn with_simd<S: Simd>(self, simd: S) -> Self::Output {
            let factor = self.factor;
            let factor_v = simd.splat_f64s(factor);

            let (dst_chunks, dst_tail) = S::as_mut_simd_f64s(self.dst);
            for dst_i in dst_chunks.iter_mut() {
                *dst_i = simd.mul_f64s(*dst_i, factor_v);
            }
            for dst_i in dst_tail.iter_mut() {
                *dst_i *= factor;
            }
        }
    }

    pub(super) fn div_slice_assign(dst: &mut [f64], divisor: f64) {
        pulp_arch().dispatch(DivSlice { dst, divisor })
    }

    struct DivSlice<'a> {
        dst: &'a mut [f64],
        divisor: f64,
    }

    impl<'a> WithSimd for DivSlice<'a> {
        type Output = ();

        #[inline(always)]
        fn with_simd<S: Simd>(self, simd: S) -> Self::Output {
            let divisor = self.divisor;
            let divisor_v = simd.splat_f64s(divisor);

            let (dst_chunks, dst_tail) = S::as_mut_simd_f64s(self.dst);
            for dst_i in dst_chunks.iter_mut() {
                *dst_i = simd.div_f64s(*dst_i, divisor_v);
            }
            for dst_i in dst_tail.iter_mut() {
                *dst_i /= divisor;
            }
        }
    }

    pub(super) fn lin_comb2_into(
        out: &mut [f64],
        lhs: &[f64],
        lhs_factor: f64,
        rhs: &[f64],
        rhs_factor: f64,
    ) {
        debug_assert_eq!(out.len(), lhs.len(), "lin_comb2 dimension mismatch");
        debug_assert_eq!(out.len(), rhs.len(), "lin_comb2 dimension mismatch");
        pulp_arch().dispatch(LinComb2 {
            out,
            lhs,
            lhs_factor,
            rhs,
            rhs_factor,
        })
    }

    struct LinComb2<'a> {
        out: &'a mut [f64],
        lhs: &'a [f64],
        lhs_factor: f64,
        rhs: &'a [f64],
        rhs_factor: f64,
    }

    impl<'a> WithSimd for LinComb2<'a> {
        type Output = ();

        #[inline(always)]
        fn with_simd<S: Simd>(self, simd: S) -> Self::Output {
            let lhs_factor = self.lhs_factor;
            let rhs_factor = self.rhs_factor;
            let lhs_factor_v = simd.splat_f64s(lhs_factor);
            let rhs_factor_v = simd.splat_f64s(rhs_factor);

            let (out_chunks, out_tail) = S::as_mut_simd_f64s(self.out);
            let (lhs_chunks, lhs_tail) = S::as_simd_f64s(self.lhs);
            let (rhs_chunks, rhs_tail) = S::as_simd_f64s(self.rhs);
            debug_assert_eq!(out_chunks.len(), lhs_chunks.len());
            debug_assert_eq!(out_chunks.len(), rhs_chunks.len());
            debug_assert_eq!(out_tail.len(), lhs_tail.len());
            debug_assert_eq!(out_tail.len(), rhs_tail.len());

            for ((out_i, lhs_i), rhs_i) in out_chunks
                .iter_mut()
                .zip(lhs_chunks.iter())
                .zip(rhs_chunks.iter())
            {
                let lhs_scaled = simd.mul_f64s(lhs_factor_v, *lhs_i);
                *out_i = simd.mul_add_f64s(rhs_factor_v, *rhs_i, lhs_scaled);
            }
            for ((out_i, lhs_i), rhs_i) in out_tail
                .iter_mut()
                .zip(lhs_tail.iter())
                .zip(rhs_tail.iter())
            {
                let lhs_scaled = lhs_factor * *lhs_i;
                *out_i = rhs_factor.mul_add(*rhs_i, lhs_scaled);
            }
        }
    }
}

#[cfg(not(feature = "simd"))]
mod simd {
    pub(super) fn fused_dot(lhs: &[f64], rhs: &[f64]) -> f64 {
        debug_assert_eq!(lhs.len(), rhs.len(), "dot product dimension mismatch");
        let mut acc = 0.0;
        for (a, b) in lhs.iter().zip(rhs.iter()) {
            acc = (*a).mul_add(*b, acc);
        }
        acc
    }

    pub(super) fn fused_dot2(lhs: &[f64], rhs_a: &[f64], rhs_b: &[f64]) -> (f64, f64) {
        debug_assert_eq!(lhs.len(), rhs_a.len(), "dot product dimension mismatch");
        debug_assert_eq!(lhs.len(), rhs_b.len(), "dot product dimension mismatch");
        let mut acc_a = 0.0;
        let mut acc_b = 0.0;
        for (a, (b, c)) in lhs.iter().zip(rhs_a.iter().zip(rhs_b.iter())) {
            acc_a = (*a).mul_add(*b, acc_a);
            acc_b = (*a).mul_add(*c, acc_b);
        }
        (acc_a, acc_b)
    }

    pub(super) fn fused_add_mul_slice_assign(dst: &mut [f64], factor: f64, rhs: &[f64]) {
        debug_assert_eq!(dst.len(), rhs.len(), "axpy dimension mismatch");
        for (dst_i, rhs_i) in dst.iter_mut().zip(rhs.iter()) {
            *dst_i = factor.mul_add(*rhs_i, *dst_i);
        }
    }

    pub(super) fn mul_slice_assign(dst: &mut [f64], factor: f64) {
        for dst_i in dst.iter_mut() {
            *dst_i *= factor;
        }
    }

    pub(super) fn div_slice_assign(dst: &mut [f64], divisor: f64) {
        for dst_i in dst.iter_mut() {
            *dst_i /= divisor;
        }
    }

    pub(super) fn lin_comb2_into(
        out: &mut [f64],
        lhs: &[f64],
        lhs_factor: f64,
        rhs: &[f64],
        rhs_factor: f64,
    ) {
        debug_assert_eq!(out.len(), lhs.len(), "lin_comb2 dimension mismatch");
        debug_assert_eq!(out.len(), rhs.len(), "lin_comb2 dimension mismatch");
        for ((out_i, lhs_i), rhs_i) in out.iter_mut().zip(lhs.iter()).zip(rhs.iter()) {
            let lhs_scaled = lhs_factor * *lhs_i;
            *out_i = rhs_factor.mul_add(*rhs_i, lhs_scaled);
        }
    }
}
