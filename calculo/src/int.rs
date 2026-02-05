use std::cmp::Ordering;

use crate::num::{Int, IntError, IntResult};

impl Int for i64 {
    type PivotScratch = ();
    type CmpScratch = ();

    #[inline(always)]
    fn zero() -> Self {
        0
    }

    #[inline(always)]
    fn one() -> Self {
        1
    }

    #[inline(always)]
    fn from_i64(value: i64) -> Self {
        value
    }

    #[inline(always)]
    fn from_u64(value: u64) -> Self {
        value.try_into().expect("u64 value exceeds i64 range")
    }

    #[inline(always)]
    fn is_zero(&self) -> bool {
        *self == 0
    }

    #[inline(always)]
    fn is_positive(&self) -> bool {
        *self > 0
    }

    #[inline(always)]
    fn is_negative(&self) -> bool {
        *self < 0
    }

    #[inline(always)]
    fn neg_mut(&mut self) -> IntResult<()> {
        *self = self.checked_neg().ok_or(IntError::Overflow)?;
        Ok(())
    }

    #[inline(always)]
    fn abs(&self) -> IntResult<Self> {
        self.checked_abs().ok_or(IntError::Overflow)
    }

    #[inline(always)]
    fn abs_mut(&mut self) -> IntResult<()> {
        *self = self.checked_abs().ok_or(IntError::Overflow)?;
        Ok(())
    }

    #[inline(always)]
    fn gcd_assign(&mut self, other: &Self) -> IntResult<()> {
        fn gcd_u64(mut a: u64, mut b: u64) -> u64 {
            while b != 0 {
                let r = a % b;
                a = b;
                b = r;
            }
            a
        }

        let g = gcd_u64(self.unsigned_abs(), other.unsigned_abs());
        let g_i64: i64 = g.try_into().map_err(|_| IntError::Overflow)?;
        *self = g_i64;
        Ok(())
    }

    #[inline(always)]
    fn lcm_assign(&mut self, other: &Self) -> IntResult<()> {
        fn gcd_u64(mut a: u64, mut b: u64) -> u64 {
            while b != 0 {
                let r = a % b;
                a = b;
                b = r;
            }
            a
        }

        let a = self.unsigned_abs();
        let b = other.unsigned_abs();
        if a == 0 || b == 0 {
            *self = 0;
            return Ok(());
        }
        let g = gcd_u64(a, b);
        let l = (a / g) as u128 * (b as u128);
        if l > i64::MAX as u128 {
            return Err(IntError::Overflow);
        }
        *self = l as i64;
        Ok(())
    }

    #[inline(always)]
    fn mul_assign(&mut self, rhs: &Self) -> IntResult<()> {
        let prod = (*self as i128) * (*rhs as i128);
        if prod < i64::MIN as i128 || prod > i64::MAX as i128 {
            return Err(IntError::Overflow);
        }
        *self = prod as i64;
        Ok(())
    }

    #[inline(always)]
    fn div_assign_exact(&mut self, rhs: &Self) -> IntResult<()> {
        *self = <Self as Int>::div_exact(self, rhs)?;
        Ok(())
    }

    #[inline(always)]
    fn bareiss_update(
        aij: &Self,
        ars: &Self,
        ais: &Self,
        arj: &Self,
        det: &Self,
    ) -> IntResult<Self> {
        let det = *det as i128;
        if det == 0 {
            return Err(IntError::DivisionByZero);
        }

        let aij = *aij as i128;
        let ars = *ars as i128;
        let ais = *ais as i128;
        let arj = *arj as i128;

        let numer = aij * ars - ais * arj;
        debug_assert_eq!(numer % det, 0);
        let quo = numer / det;
        if quo < i64::MIN as i128 || quo > i64::MAX as i128 {
            return Err(IntError::Overflow);
        }
        Ok(quo as i64)
    }

    #[inline(always)]
    fn assign_from(dst: &mut Self, src: &Self) {
        *dst = *src;
    }

    #[inline(always)]
    fn bareiss_update_in_place(
        aij: &mut Self,
        ars: &Self,
        ais: &Self,
        arj: &Self,
        det: &Self,
        _scratch: &mut Self::PivotScratch,
    ) -> IntResult<()> {
        let det_i128 = *det as i128;
        if det_i128 == 0 {
            return Err(IntError::DivisionByZero);
        }
        let numer = (*aij as i128) * (*ars as i128) - (*ais as i128) * (*arj as i128);
        debug_assert_eq!(numer % det_i128, 0);
        let quo = numer / det_i128;
        if quo < i64::MIN as i128 || quo > i64::MAX as i128 {
            return Err(IntError::Overflow);
        }
        *aij = quo as i64;
        Ok(())
    }

    #[inline(always)]
    fn div_exact(numer: &Self, denom: &Self) -> IntResult<Self> {
        if *denom == 0 {
            return Err(IntError::DivisionByZero);
        }
        let numer_i128 = *numer as i128;
        let denom_i128 = *denom as i128;
        debug_assert_eq!(numer_i128 % denom_i128, 0);
        let quo = numer_i128 / denom_i128;
        if quo < i64::MIN as i128 || quo > i64::MAX as i128 {
            return Err(IntError::Overflow);
        }
        Ok(quo as i64)
    }

    #[inline(always)]
    fn cmp_product(
        a: &Self,
        b: &Self,
        c: &Self,
        d: &Self,
        _scratch: &mut Self::CmpScratch,
    ) -> Ordering {
        let left = (*a as i128) * (*b as i128);
        let right = (*c as i128) * (*d as i128);
        left.cmp(&right)
    }
}

#[cfg(feature = "rug")]
mod rug {
    use super::{Int, IntError, IntResult};
    use std::cmp::Ordering;

    use ::rug::Assign;
    use ::rug::ops::NegAssign;

    #[derive(Debug)]
    pub struct RugPivotScratch {
        nt: ::rug::Integer,
        ns: ::rug::Integer,
    }

    impl Default for RugPivotScratch {
        fn default() -> Self {
            Self {
                nt: ::rug::Integer::new(),
                ns: ::rug::Integer::new(),
            }
        }
    }

    #[derive(Debug)]
    pub struct RugCmpScratch {
        left: ::rug::Integer,
        right: ::rug::Integer,
    }

    impl Default for RugCmpScratch {
        fn default() -> Self {
            Self {
                left: ::rug::Integer::new(),
                right: ::rug::Integer::new(),
            }
        }
    }

    impl Int for ::rug::Integer {
        type PivotScratch = RugPivotScratch;
        type CmpScratch = RugCmpScratch;

        #[inline(always)]
        fn zero() -> Self {
            ::rug::Integer::from(0)
        }

        #[inline(always)]
        fn one() -> Self {
            ::rug::Integer::from(1)
        }

        #[inline(always)]
        fn from_i64(value: i64) -> Self {
            ::rug::Integer::from(value)
        }

        #[inline(always)]
        fn from_u64(value: u64) -> Self {
            ::rug::Integer::from(value)
        }

        #[inline(always)]
        fn is_zero(&self) -> bool {
            ::rug::Integer::is_zero(self)
        }

        #[inline(always)]
        fn is_positive(&self) -> bool {
            ::rug::Integer::is_positive(self)
        }

        #[inline(always)]
        fn is_negative(&self) -> bool {
            ::rug::Integer::is_negative(self)
        }

        #[inline(always)]
        fn neg_mut(&mut self) -> IntResult<()> {
            self.neg_assign();
            Ok(())
        }

        #[inline(always)]
        fn abs(&self) -> IntResult<Self> {
            Ok(self.clone().abs())
        }

        #[inline(always)]
        fn abs_mut(&mut self) -> IntResult<()> {
            ::rug::Integer::abs_mut(self);
            Ok(())
        }

        #[inline(always)]
        fn gcd_assign(&mut self, other: &Self) -> IntResult<()> {
            self.gcd_mut(other);
            Ok(())
        }

        #[inline(always)]
        fn lcm_assign(&mut self, other: &Self) -> IntResult<()> {
            self.lcm_mut(other);
            Ok(())
        }

        #[inline(always)]
        fn mul_assign(&mut self, rhs: &Self) -> IntResult<()> {
            *self *= rhs;
            Ok(())
        }

        #[inline(always)]
        fn div_assign_exact(&mut self, rhs: &Self) -> IntResult<()> {
            if rhs.is_zero() {
                return Err(IntError::DivisionByZero);
            }
            #[cfg(debug_assertions)]
            {
                let mut rem = self.clone();
                rem %= rhs;
                debug_assert!(rem.is_zero());
            }
            *self /= rhs;
            Ok(())
        }

        #[inline(always)]
        fn bareiss_update(
            aij: &Self,
            ars: &Self,
            ais: &Self,
            arj: &Self,
            det: &Self,
        ) -> IntResult<Self> {
            if det.is_zero() {
                return Err(IntError::DivisionByZero);
            }
            let mut numer = aij.clone();
            numer *= ars;
            let mut sub = ais.clone();
            sub *= arj;
            numer -= sub;
            #[cfg(debug_assertions)]
            {
                let mut rem = numer.clone();
                rem %= det;
                debug_assert!(rem.is_zero());
            }
            numer /= det;
            Ok(numer)
        }

        #[inline(always)]
        fn assign_from(dst: &mut Self, src: &Self) {
            dst.assign(src);
        }

        #[inline(always)]
        fn bareiss_update_in_place(
            aij: &mut Self,
            ars: &Self,
            ais: &Self,
            arj: &Self,
            det: &Self,
            scratch: &mut Self::PivotScratch,
        ) -> IntResult<()> {
            if det.is_zero() {
                return Err(IntError::DivisionByZero);
            }
            scratch.nt.assign(&*aij);
            scratch.nt *= ars;
            scratch.ns.assign(ais);
            scratch.ns *= arj;
            scratch.nt -= &scratch.ns;
            #[cfg(debug_assertions)]
            {
                let mut rem = scratch.nt.clone();
                rem %= det;
                debug_assert!(rem.is_zero());
            }
            scratch.nt /= det;
            aij.assign(&scratch.nt);
            Ok(())
        }

        #[inline(always)]
        fn div_exact(numer: &Self, denom: &Self) -> IntResult<Self> {
            if denom.is_zero() {
                return Err(IntError::DivisionByZero);
            }
            let mut q = numer.clone();
            #[cfg(debug_assertions)]
            {
                let mut rem = q.clone();
                rem %= denom;
                debug_assert!(rem.is_zero());
            }
            q /= denom;
            Ok(q)
        }

        #[inline(always)]
        fn cmp_product(
            a: &Self,
            b: &Self,
            c: &Self,
            d: &Self,
            scratch: &mut Self::CmpScratch,
        ) -> Ordering {
            // Fast path: compare signs and bit-length bounds before multiplying.
            //
            // `significant_bits()` is the bit-length of the magnitude (0 => 0).
            let left_zero = a.is_zero() || b.is_zero();
            let right_zero = c.is_zero() || d.is_zero();
            if left_zero && right_zero {
                return Ordering::Equal;
            }

            let left_neg = !left_zero && (a.is_negative() ^ b.is_negative());
            let right_neg = !right_zero && (c.is_negative() ^ d.is_negative());
            if left_neg != right_neg {
                return if left_neg {
                    Ordering::Less
                } else {
                    Ordering::Greater
                };
            }
            // Same sign: handle zero-products explicitly (bit-length heuristics are invalid when a
            // factor is zero).
            if left_zero {
                // left = 0, right != 0 and is not negative => left < right
                return Ordering::Less;
            }
            if right_zero {
                // right = 0, left != 0 and is not negative => left > right
                return Ordering::Greater;
            }

            // Same sign: compare magnitudes with cheap bounds.
            let left_bits: u64 = (a.significant_bits() as u64) + (b.significant_bits() as u64);
            let right_bits: u64 = (c.significant_bits() as u64) + (d.significant_bits() as u64);
            if left_bits + 2 <= right_bits {
                // |a*b| < |c*d|
                return if left_neg {
                    Ordering::Greater
                } else {
                    Ordering::Less
                };
            }
            if right_bits + 2 <= left_bits {
                // |a*b| > |c*d|
                return if left_neg {
                    Ordering::Less
                } else {
                    Ordering::Greater
                };
            }

            // Ambiguous: fall back to the exact signed comparison.
            scratch.left.assign(a);
            scratch.left *= b;
            scratch.right.assign(c);
            scratch.right *= d;
            scratch.left.cmp(&scratch.right)
        }
    }
}

#[cfg(feature = "dashu")]
mod dashu {
    use super::{Int, IntError, IntResult};
    use std::cmp::Ordering;

    use dashu_base::BitTest;
    use dashu_int::IBig;
    use dashu_int::Sign as DashuSign;

    #[derive(Debug)]
    pub struct DashuPivotScratch {
        nt: IBig,
        ns: IBig,
    }

    impl Default for DashuPivotScratch {
        fn default() -> Self {
            Self {
                nt: IBig::ZERO,
                ns: IBig::ZERO,
            }
        }
    }

    #[derive(Debug)]
    pub struct DashuCmpScratch {
        left: IBig,
        right: IBig,
    }

    impl Default for DashuCmpScratch {
        fn default() -> Self {
            Self {
                left: IBig::ZERO,
                right: IBig::ZERO,
            }
        }
    }

    impl Int for IBig {
        type PivotScratch = DashuPivotScratch;
        type CmpScratch = DashuCmpScratch;

        #[inline(always)]
        fn zero() -> Self {
            IBig::ZERO
        }

        #[inline(always)]
        fn one() -> Self {
            IBig::ONE
        }

        #[inline(always)]
        fn from_i64(value: i64) -> Self {
            IBig::from(value)
        }

        #[inline(always)]
        fn from_u64(value: u64) -> Self {
            IBig::from(value)
        }

        #[inline(always)]
        fn is_zero(&self) -> bool {
            self.is_zero()
        }

        #[inline(always)]
        fn is_positive(&self) -> bool {
            !self.is_zero() && self.sign() == DashuSign::Positive
        }

        #[inline(always)]
        fn is_negative(&self) -> bool {
            self.sign() == DashuSign::Negative
        }

        #[inline(always)]
        fn neg_mut(&mut self) -> IntResult<()> {
            let tmp = std::mem::replace(self, IBig::ZERO);
            *self = -tmp;
            Ok(())
        }

        #[inline(always)]
        fn abs(&self) -> IntResult<Self> {
            let mut out = self.clone();
            out.abs_mut()?;
            Ok(out)
        }

        #[inline(always)]
        fn abs_mut(&mut self) -> IntResult<()> {
            if self.sign() != DashuSign::Negative {
                return Ok(());
            }
            let (_sign, magnitude) = std::mem::replace(self, IBig::ZERO).into_parts();
            *self = IBig::from_parts(DashuSign::Positive, magnitude);
            Ok(())
        }

        #[inline(always)]
        fn gcd_assign(&mut self, other: &Self) -> IntResult<()> {
            if self.is_zero() {
                if other.is_zero() {
                    *self = IBig::ZERO;
                    return Ok(());
                }
                self.clone_from(other);
                self.abs_mut()?;
                return Ok(());
            }
            if other.is_zero() {
                self.abs_mut()?;
                return Ok(());
            }

            let g = dashu_base::Gcd::gcd(&*self, other);
            *self = IBig::from(g);
            Ok(())
        }

        #[inline(always)]
        fn lcm_assign(&mut self, other: &Self) -> IntResult<()> {
            if self.is_zero() || other.is_zero() {
                *self = IBig::ZERO;
                return Ok(());
            }
            // lcm(a,b) = |a / gcd(a,b)| * |b|
            self.abs_mut()?;
            let mut b = other.clone();
            b.abs_mut()?;

            let g = dashu_base::Gcd::gcd(&*self, &b);
            *self /= IBig::from(g);
            *self *= &b;
            Ok(())
        }

        #[inline(always)]
        fn mul_assign(&mut self, rhs: &Self) -> IntResult<()> {
            *self *= rhs;
            Ok(())
        }

        #[inline(always)]
        fn div_assign_exact(&mut self, rhs: &Self) -> IntResult<()> {
            if rhs.is_zero() {
                return Err(IntError::DivisionByZero);
            }
            #[cfg(debug_assertions)]
            {
                let rem = &*self % rhs;
                debug_assert!(rem.is_zero());
            }
            *self /= rhs;
            Ok(())
        }

        #[inline(always)]
        fn bareiss_update(
            aij: &Self,
            ars: &Self,
            ais: &Self,
            arj: &Self,
            det: &Self,
        ) -> IntResult<Self> {
            if det.is_zero() {
                return Err(IntError::DivisionByZero);
            }
            let mut numer = aij.clone();
            numer *= ars;
            let mut sub = ais.clone();
            sub *= arj;
            numer -= sub;
            #[cfg(debug_assertions)]
            {
                let rem = &numer % det;
                debug_assert!(rem.is_zero());
            }
            numer /= det;
            Ok(numer)
        }

        #[inline(always)]
        fn div_exact(numer: &Self, denom: &Self) -> IntResult<Self> {
            if denom.is_zero() {
                return Err(IntError::DivisionByZero);
            }
            let mut q = numer.clone();
            #[cfg(debug_assertions)]
            {
                let rem = &q % denom;
                debug_assert!(rem.is_zero());
            }
            q /= denom;
            Ok(q)
        }

        #[inline(always)]
        fn cmp_product(
            a: &Self,
            b: &Self,
            c: &Self,
            d: &Self,
            scratch: &mut Self::CmpScratch,
        ) -> Ordering {
            // Fast path: compare signs and bit-length bounds before multiplying.
            //
            // `bit_len()` is the bit-length of the magnitude (0 => 0).
            let left_zero = a.is_zero() || b.is_zero();
            let right_zero = c.is_zero() || d.is_zero();
            if left_zero && right_zero {
                return Ordering::Equal;
            }

            let left_neg = !left_zero
                && ((a.sign() == DashuSign::Negative) ^ (b.sign() == DashuSign::Negative));
            let right_neg = !right_zero
                && ((c.sign() == DashuSign::Negative) ^ (d.sign() == DashuSign::Negative));
            if left_neg != right_neg {
                return if left_neg {
                    Ordering::Less
                } else {
                    Ordering::Greater
                };
            }
            // Same sign: handle zero-products explicitly (bit-length heuristics are invalid when a
            // factor is zero).
            if left_zero {
                // left = 0, right != 0 and is not negative => left < right
                return Ordering::Less;
            }
            if right_zero {
                // right = 0, left != 0 and is not negative => left > right
                return Ordering::Greater;
            }

            let left_bits: u64 = (a.bit_len() as u64) + (b.bit_len() as u64);
            let right_bits: u64 = (c.bit_len() as u64) + (d.bit_len() as u64);
            if left_bits + 2 <= right_bits {
                // |a*b| < |c*d|
                return if left_neg {
                    Ordering::Greater
                } else {
                    Ordering::Less
                };
            }
            if right_bits + 2 <= left_bits {
                // |a*b| > |c*d|
                return if left_neg {
                    Ordering::Less
                } else {
                    Ordering::Greater
                };
            }

            // Ambiguous: fall back to the exact signed comparison.
            scratch.left.clone_from(a);
            scratch.left *= b;
            scratch.right.clone_from(c);
            scratch.right *= d;
            scratch.left.cmp(&scratch.right)
        }

        #[inline(always)]
        fn assign_from(dst: &mut Self, src: &Self) {
            dst.clone_from(src);
        }

        #[inline(always)]
        fn bareiss_update_in_place(
            aij: &mut Self,
            ars: &Self,
            ais: &Self,
            arj: &Self,
            det: &Self,
            scratch: &mut Self::PivotScratch,
        ) -> IntResult<()> {
            if det.is_zero() {
                return Err(IntError::DivisionByZero);
            }
            scratch.nt.clone_from(aij);
            scratch.nt *= ars;
            scratch.ns.clone_from(ais);
            scratch.ns *= arj;
            scratch.nt -= &scratch.ns;
            #[cfg(debug_assertions)]
            {
                let rem = &scratch.nt % det;
                debug_assert!(rem.is_zero());
            }
            scratch.nt /= det;
            aij.clone_from(&scratch.nt);
            Ok(())
        }
    }
}
