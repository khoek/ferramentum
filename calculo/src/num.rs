use std::cmp::Ordering;
use std::ops::{Add, Div, Mul, Neg, Sub};

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum Sign {
    Negative,
    Zero,
    Positive,
}

impl Sign {
    #[inline(always)]
    pub fn from_ordering(ord: Ordering) -> Self {
        match ord {
            Ordering::Less => Self::Negative,
            Ordering::Equal => Self::Zero,
            Ordering::Greater => Self::Positive,
        }
    }
}

pub trait Num:
    Clone
    + std::fmt::Debug
    + PartialEq
    + PartialOrd
    + Add<Self, Output = Self>
    + Sub<Self, Output = Self>
    + Mul<Self, Output = Self>
    + Div<Self, Output = Self>
    + Neg<Output = Self>
{
    type LinAlg: crate::linalg::LinAlgOps<Self>;

    fn zero() -> Self;

    fn one() -> Self;

    fn from_u64(value: u64) -> Self;

    fn ref_add(&self, other: &Self) -> Self;
    fn ref_sub(&self, other: &Self) -> Self;
    fn ref_mul(&self, other: &Self) -> Self;
    fn ref_div(&self, other: &Self) -> Self;
    fn ref_neg(&self) -> Self;

    #[inline(always)]
    fn abs(&self) -> Self {
        if self < &Self::zero() {
            -self.clone()
        } else {
            self.clone()
        }
    }

    #[inline(always)]
    fn default_eps() -> impl Epsilon<Self> + Clone
    where
        Self: Sized,
    {
        DynamicEpsilon::<Self>::new(Self::zero())
    }

    fn try_from_f64(value: f64) -> Option<Self>
    where
        Self: Sized;

    fn to_f64(&self) -> f64;

    fn hash_component(&self) -> u64;
}

#[derive(Clone, Debug)]
pub struct ConversionError;

impl std::error::Error for ConversionError {}

impl std::fmt::Display for ConversionError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "numeric conversion failed")
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum IntError {
    Overflow,
    DivisionByZero,
    NonExactDivision,
}

impl std::error::Error for IntError {}

impl std::fmt::Display for IntError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Overflow => write!(f, "integer overflow"),
            Self::DivisionByZero => write!(f, "division by zero"),
            Self::NonExactDivision => write!(f, "non-exact division"),
        }
    }
}

pub type IntResult<T> = std::result::Result<T, IntError>;

/// Exact integer operations required by fraction-free pivoting and lexicographic comparisons.
///
/// Notes:
/// - The small backend uses i64 storage with i128 intermediates for `bareiss_update` /
///   `cmp_product`.
/// - Exact divisions are required by the Bareiss-style update (they must divide).
pub trait Int:
    Clone
    + std::fmt::Debug
    + Ord
    + for<'a> std::ops::AddAssign<&'a Self>
    + for<'a> std::ops::SubAssign<&'a Self>
{
    /// Scratch space used by the in-place Bareiss update.
    type PivotScratch: Default;
    /// Scratch space used by product comparisons.
    type CmpScratch: Default;

    fn zero() -> Self;
    fn one() -> Self;
    fn from_i64(value: i64) -> Self;
    fn from_u64(value: u64) -> Self;

    fn is_zero(&self) -> bool;
    fn is_positive(&self) -> bool;
    fn is_negative(&self) -> bool;

    fn neg_mut(&mut self) -> IntResult<()>;
    fn abs(&self) -> IntResult<Self>;
    fn abs_mut(&mut self) -> IntResult<()>;

    fn gcd_assign(&mut self, other: &Self) -> IntResult<()>;
    fn lcm_assign(&mut self, other: &Self) -> IntResult<()>;

    fn mul_assign(&mut self, rhs: &Self) -> IntResult<()>;
    fn div_assign_exact(&mut self, rhs: &Self) -> IntResult<()>;

    #[inline(always)]
    fn negated(&self) -> IntResult<Self> {
        let mut v = self.clone();
        v.neg_mut()?;
        Ok(v)
    }

    /// Compute the Bareiss-style pivot update:
    ///
    /// \( (a_{ij}·a_{rs} - a_{is}·a_{rj}) / det \)
    ///
    /// The division must be exact.
    fn bareiss_update(
        aij: &Self,
        ars: &Self,
        ais: &Self,
        arj: &Self,
        det: &Self,
    ) -> IntResult<Self>;

    fn div_exact(numer: &Self, denom: &Self) -> IntResult<Self>;

    /// Compare \(a·b\) with \(c·d\) exactly.
    fn cmp_product(
        a: &Self,
        b: &Self,
        c: &Self,
        d: &Self,
        scratch: &mut Self::CmpScratch,
    ) -> Ordering;

    /// Assign `src` into `dst`, reusing allocations when possible.
    #[inline(always)]
    fn assign_from(dst: &mut Self, src: &Self) {
        *dst = src.clone();
    }

    /// In-place Bareiss pivot update:
    ///
    /// `aij ← (aij·ars − ais·arj) / det`
    ///
    /// Implementations should be allocation-free in the hot path.
    #[inline(always)]
    fn bareiss_update_in_place(
        aij: &mut Self,
        ars: &Self,
        ais: &Self,
        arj: &Self,
        det: &Self,
        _scratch: &mut Self::PivotScratch,
    ) -> IntResult<()> {
        let new = Self::bareiss_update(aij, ars, ais, arj, det)?;
        *aij = new;
        Ok(())
    }
}

pub trait CoerceFrom<Src: Num>: Num {
    fn coerce_from(value: &Src) -> Result<Self, ConversionError>
    where
        Self: Sized;
}

/// Exact rational numbers that can be decomposed into integer numerator/denominator parts.
///
/// Contract:
/// - `into_parts` must return a nonzero denominator.
/// - `from_frac` expects a nonzero denominator and should canonicalize the result.
pub trait Rat: Num {
    type Int: Int;

    fn into_parts(self) -> (Self::Int, Self::Int);
    fn from_frac(numer: Self::Int, denom: Self::Int) -> Self;
}

/// Marker trait for inexact (floating / approximate) numeric types.
///
/// This is primarily used by frontends to pick defaults; core algorithms remain generic over `Num`.
pub trait InexactNum: Num {}

impl InexactNum for f64 {}

/// Exact decomposition of a finite `f64` into an integer numerator and denominator.
///
/// The denominator is positive. For exact IEEE-754 binary floats, this is always a power of two.
pub trait F64Parts: Int {
    fn f64_to_num_den(value: f64) -> (Self, Self);
}

/// Convert a finite `f64` slice into scaled integer numerators over a shared denominator.
///
/// Returns `(denom, scaled)` such that, for each input `x_i`,
/// `x_i == scaled[i] / denom` exactly (as a rational).
#[inline(always)]
pub fn f64_row_to_scaled_ints<I: Int + F64Parts>(row: &[f64]) -> (I, Vec<I>) {
    if row.is_empty() {
        return (I::one(), Vec::new());
    }

    let mut parts = Vec::with_capacity(row.len());
    for &x in row {
        parts.push(I::f64_to_num_den(x));
    }

    let denom = parts
        .iter()
        .map(|(_n, d)| d)
        .max()
        .cloned()
        .unwrap_or_else(I::one);

    let mut scaled = Vec::with_capacity(row.len());
    for (mut numer, den) in parts {
        if numer.is_zero() || den == denom {
            scaled.push(numer);
            continue;
        }

        let mut scale = denom.clone();
        scale
            .div_assign_exact(&den)
            .expect("f64 row scaling requires exact power-of-two division");
        numer
            .mul_assign(&scale)
            .expect("f64 row scaling requires multiplication");
        scaled.push(numer);
    }

    (denom, scaled)
}

impl<T: Num> CoerceFrom<T> for T {
    fn coerce_from(value: &T) -> Result<Self, ConversionError> {
        Ok(value.clone())
    }
}

mod coerce_from {
    #[cfg(feature = "rug")]
    mod rug {
        use crate::num::{CoerceFrom, ConversionError, Num, RugFloat, RugRat};

        impl CoerceFrom<f64> for RugRat {
            fn coerce_from(value: &f64) -> Result<Self, ConversionError> {
                Self::try_from_f64(*value).ok_or(ConversionError)
            }
        }

        impl<const P: u32> CoerceFrom<RugFloat<P>> for RugRat {
            fn coerce_from(value: &RugFloat<P>) -> Result<Self, ConversionError> {
                value.0.to_rational().map(RugRat).ok_or(ConversionError)
            }
        }

        impl<const P: u32> CoerceFrom<f64> for RugFloat<P> {
            fn coerce_from(value: &f64) -> Result<Self, ConversionError> {
                Self::try_from_f64(*value).ok_or(ConversionError)
            }
        }

        impl CoerceFrom<RugRat> for f64 {
            fn coerce_from(value: &RugRat) -> Result<Self, ConversionError> {
                Ok(value.to_f64())
            }
        }

        impl<const P: u32> CoerceFrom<RugFloat<P>> for f64 {
            fn coerce_from(value: &RugFloat<P>) -> Result<Self, ConversionError> {
                Ok(value.to_f64())
            }
        }
    }

    #[cfg(feature = "dashu")]
    mod dashu {
        use crate::num::{CoerceFrom, ConversionError, DashuFloat, DashuRat, Num};
        use dashu_int::IBig;

        impl CoerceFrom<f64> for DashuRat {
            fn coerce_from(value: &f64) -> Result<Self, ConversionError> {
                Self::try_from_f64(*value).ok_or(ConversionError)
            }
        }

        impl<const P: usize> CoerceFrom<DashuFloat<P>> for DashuRat {
            fn coerce_from(value: &DashuFloat<P>) -> Result<Self, ConversionError> {
                let repr = value.0.repr();
                if repr.is_infinite() {
                    return Err(ConversionError);
                }

                let significand = repr.significand().clone();
                let exponent = repr.exponent();
                if exponent >= 0 {
                    let shift: usize = exponent.try_into().map_err(|_| ConversionError)?;
                    let numer = significand << shift;
                    return Ok(DashuRat(dashu_ratio::RBig::from(numer)));
                }

                let shift: usize = exponent
                    .checked_abs()
                    .and_then(|abs| abs.try_into().ok())
                    .ok_or(ConversionError)?;
                let denom = IBig::ONE << shift;
                Ok(DashuRat(dashu_ratio::RBig::from_parts_signed(
                    significand,
                    denom,
                )))
            }
        }

        impl<const P: usize> CoerceFrom<f64> for DashuFloat<P> {
            fn coerce_from(value: &f64) -> Result<Self, ConversionError> {
                Self::try_from_f64(*value).ok_or(ConversionError)
            }
        }

        impl CoerceFrom<DashuRat> for f64 {
            fn coerce_from(value: &DashuRat) -> Result<Self, ConversionError> {
                Ok(value.to_f64())
            }
        }

        impl<const P: usize> CoerceFrom<DashuFloat<P>> for f64 {
            fn coerce_from(value: &DashuFloat<P>) -> Result<Self, ConversionError> {
                Ok(value.to_f64())
            }
        }
    }
}

pub trait Epsilon<N: Num> {
    fn eps(&self) -> &N;
    fn neg_eps(&self) -> &N;

    #[inline(always)]
    fn sign(&self, value: &N) -> Sign {
        Sign::from_ordering(self.cmp_zero(value))
    }

    #[inline(always)]
    fn is_nonnegative(&self, value: &N) -> bool {
        value >= self.neg_eps()
    }

    #[inline(always)]
    fn is_nonpositive(&self, value: &N) -> bool {
        value <= self.eps()
    }

    #[inline(always)]
    fn is_positive(&self, value: &N) -> bool {
        value > self.eps()
    }

    #[inline(always)]
    fn is_negative(&self, value: &N) -> bool {
        value < self.neg_eps()
    }

    #[inline(always)]
    fn is_zero(&self, value: &N) -> bool {
        let neg_eps = self.neg_eps();
        if value < neg_eps {
            return false;
        }
        value <= self.eps()
    }

    #[inline(always)]
    fn cmp_zero(&self, value: &N) -> Ordering {
        if value > self.eps() {
            return Ordering::Greater;
        }
        if value < self.neg_eps() {
            return Ordering::Less;
        }
        Ordering::Equal
    }

    #[inline(always)]
    fn cmp(&self, left: &N, right: &N) -> Ordering {
        let diff = left.ref_sub(right);
        self.cmp_zero(&diff)
    }
}

pub trait Normalizer<N: Num>: Clone + std::fmt::Debug {
    fn normalize<E: Epsilon<N> + ?Sized>(&mut self, eps: &E, vector: &mut [N]) -> bool;

    fn normalize_pair<E: Epsilon<N> + ?Sized, M: Num + CoerceFrom<N>>(
        &mut self,
        eps: &E,
        vector: &mut [N],
        shadow: &mut [M],
    ) -> bool;
}

#[derive(Clone, Copy, Debug, Default)]
pub struct MaxNormalizer;

#[derive(Clone, Copy, Debug, Default)]
pub struct MinNormalizer;

#[derive(Clone, Copy, Debug, Default)]
pub struct NoNormalizer;

impl<N: Num> Normalizer<N> for MaxNormalizer {
    #[inline(always)]
    fn normalize<E: Epsilon<N> + ?Sized>(&mut self, eps: &E, vector: &mut [N]) -> bool {
        let mut max = None;
        for v in vector.iter() {
            let abs = v.abs();
            if eps.is_zero(&abs) {
                continue;
            }
            if max
                .as_ref()
                .is_none_or(|m| abs.partial_cmp(m).is_some_and(|o| o.is_gt()))
            {
                max = Some(abs);
            }
        }

        let Some(scale) = max else {
            for v in vector.iter_mut() {
                *v = N::zero();
            }
            return false;
        };

        crate::linalg::div_assign(vector, &scale);
        for v in vector.iter_mut() {
            if eps.is_zero(v) {
                *v = N::zero();
            }
        }
        true
    }

    #[inline(always)]
    fn normalize_pair<E: Epsilon<N> + ?Sized, M: Num + CoerceFrom<N>>(
        &mut self,
        eps: &E,
        vector: &mut [N],
        shadow: &mut [M],
    ) -> bool {
        assert_eq!(vector.len(), shadow.len(), "shadow length mismatch");

        let mut max = None;
        for v in vector.iter() {
            let abs = v.abs();
            if eps.is_zero(&abs) {
                continue;
            }
            if max
                .as_ref()
                .is_none_or(|m| abs.partial_cmp(m).is_some_and(|o| o.is_gt()))
            {
                max = Some(abs);
            }
        }

        let Some(scale) = max else {
            for v in vector.iter_mut() {
                *v = N::zero();
            }
            for v in shadow.iter_mut() {
                *v = M::zero();
            }
            return false;
        };

        crate::linalg::div_assign(vector, &scale);
        let scale_shadow = M::coerce_from(&scale).expect("normalization scale must be convertible");
        crate::linalg::div_assign(shadow, &scale_shadow);
        for (v, s) in vector.iter_mut().zip(shadow.iter_mut()) {
            if eps.is_zero(v) {
                *v = N::zero();
                *s = M::zero();
            }
        }
        true
    }
}

impl<N: Num> Normalizer<N> for MinNormalizer {
    #[inline(always)]
    fn normalize<E: Epsilon<N> + ?Sized>(&mut self, eps: &E, vector: &mut [N]) -> bool {
        let mut min = None;
        for v in vector.iter() {
            let abs = v.abs();
            if eps.is_zero(&abs) {
                continue;
            }
            if min
                .as_ref()
                .is_none_or(|m| abs.partial_cmp(m).is_some_and(|o| o.is_lt()))
            {
                min = Some(abs);
            }
        }

        let Some(scale) = min else {
            for v in vector.iter_mut() {
                *v = N::zero();
            }
            return false;
        };

        crate::linalg::div_assign(vector, &scale);
        for v in vector.iter_mut() {
            if eps.is_zero(v) {
                *v = N::zero();
            }
        }
        true
    }

    #[inline(always)]
    fn normalize_pair<E: Epsilon<N> + ?Sized, M: Num + CoerceFrom<N>>(
        &mut self,
        eps: &E,
        vector: &mut [N],
        shadow: &mut [M],
    ) -> bool {
        assert_eq!(vector.len(), shadow.len(), "shadow length mismatch");

        let mut min = None;
        for v in vector.iter() {
            let abs = v.abs();
            if eps.is_zero(&abs) {
                continue;
            }
            if min
                .as_ref()
                .is_none_or(|m| abs.partial_cmp(m).is_some_and(|o| o.is_lt()))
            {
                min = Some(abs);
            }
        }

        let Some(scale) = min else {
            for v in vector.iter_mut() {
                *v = N::zero();
            }
            for v in shadow.iter_mut() {
                *v = M::zero();
            }
            return false;
        };

        crate::linalg::div_assign(vector, &scale);
        let scale_shadow = M::coerce_from(&scale).expect("normalization scale must be convertible");
        crate::linalg::div_assign(shadow, &scale_shadow);
        for (v, s) in vector.iter_mut().zip(shadow.iter_mut()) {
            if eps.is_zero(v) {
                *v = N::zero();
                *s = M::zero();
            }
        }
        true
    }
}

impl<N: Num> Normalizer<N> for NoNormalizer {
    #[inline(always)]
    fn normalize<E: Epsilon<N> + ?Sized>(&mut self, eps: &E, vector: &mut [N]) -> bool {
        if vector.iter().any(|v| !eps.is_zero(v)) {
            return true;
        }
        for v in vector.iter_mut() {
            *v = N::zero();
        }
        false
    }

    #[inline(always)]
    fn normalize_pair<E: Epsilon<N> + ?Sized, M: Num + CoerceFrom<N>>(
        &mut self,
        eps: &E,
        vector: &mut [N],
        shadow: &mut [M],
    ) -> bool {
        assert_eq!(vector.len(), shadow.len(), "shadow length mismatch");

        if vector.iter().any(|v| !eps.is_zero(v)) {
            return true;
        }
        for v in vector.iter_mut() {
            *v = N::zero();
        }
        for v in shadow.iter_mut() {
            *v = M::zero();
        }
        false
    }
}

#[derive(Clone, Debug)]
pub struct DynamicEpsilon<N: Num> {
    eps: N,
    neg_eps: N,
}

impl<N: Num> DynamicEpsilon<N> {
    #[inline(always)]
    pub fn new(eps: N) -> Self {
        let neg_eps = eps.ref_neg();
        Self { eps, neg_eps }
    }
}

impl<N: Num> Epsilon<N> for DynamicEpsilon<N> {
    #[inline(always)]
    fn eps(&self) -> &N {
        &self.eps
    }

    #[inline(always)]
    fn neg_eps(&self) -> &N {
        &self.neg_eps
    }
}

#[derive(Debug)]
pub struct GcdNormalizer<N: Rat> {
    parts: Vec<(N::Int, N::Int)>,
    zero: N::Int,
    one: N::Int,
    lcm: N::Int,
    gcd: N::Int,
    tmp: N::Int,
    tmp_abs: N::Int,
}

impl<N: Rat> Default for GcdNormalizer<N> {
    fn default() -> Self {
        let zero = N::Int::zero();
        let one = N::Int::one();
        let mut lcm = N::Int::zero();
        N::Int::assign_from(&mut lcm, &one);
        Self {
            parts: Vec::new(),
            zero,
            one,
            lcm,
            gcd: N::Int::zero(),
            tmp: N::Int::zero(),
            tmp_abs: N::Int::zero(),
        }
    }
}

impl<N: Rat> Clone for GcdNormalizer<N> {
    fn clone(&self) -> Self {
        Self::default()
    }
}

impl<N: Rat> Normalizer<N> for GcdNormalizer<N> {
    fn normalize<E: Epsilon<N> + ?Sized>(&mut self, _eps: &E, vector: &mut [N]) -> bool {
        self.parts.clear();
        self.parts.reserve(vector.len());
        N::Int::assign_from(&mut self.lcm, &self.one);

        let mut any_nonzero = false;
        for v in vector.iter_mut() {
            let (mut numer, mut denom) = std::mem::replace(v, N::zero()).into_parts();
            if denom.is_negative() {
                denom
                    .neg_mut()
                    .expect("gcd normalization requires negation");
                numer
                    .neg_mut()
                    .expect("gcd normalization requires negation");
            }
            if !numer.is_zero() {
                any_nonzero = true;
                self.lcm
                    .lcm_assign(&denom)
                    .expect("gcd normalization requires LCM");
            }
            self.parts.push((numer, denom));
        }

        if !any_nonzero {
            for v in vector.iter_mut() {
                *v = N::zero();
            }
            return false;
        }

        N::Int::assign_from(&mut self.gcd, &self.zero);
        for (numer, denom) in self.parts.iter_mut() {
            if numer.is_zero() {
                continue;
            }

            N::Int::assign_from(&mut self.tmp, &self.lcm);
            self.tmp
                .div_assign_exact(denom)
                .expect("gcd normalization requires exact division");
            numer
                .mul_assign(&self.tmp)
                .expect("gcd normalization requires multiplication");

            N::Int::assign_from(&mut self.tmp_abs, numer);
            self.tmp_abs
                .abs_mut()
                .expect("gcd normalization requires abs");
            if self.gcd.is_zero() {
                N::Int::assign_from(&mut self.gcd, &self.tmp_abs);
            } else {
                self.gcd
                    .gcd_assign(&self.tmp_abs)
                    .expect("gcd normalization requires gcd");
            }
        }

        let gcd_is_one = self.gcd == self.one;
        for (dest, (numer, _denom)) in vector.iter_mut().zip(self.parts.iter_mut()) {
            if numer.is_zero() {
                *dest = N::zero();
                continue;
            }

            if !gcd_is_one {
                numer
                    .div_assign_exact(&self.gcd)
                    .expect("gcd normalization requires exact division");
            }

            let numer = std::mem::replace(numer, N::Int::zero());
            *dest = N::from_frac(numer, N::Int::one());
        }

        true
    }

    fn normalize_pair<E: Epsilon<N> + ?Sized, M: Num + CoerceFrom<N>>(
        &mut self,
        eps: &E,
        vector: &mut [N],
        shadow: &mut [M],
    ) -> bool {
        assert_eq!(vector.len(), shadow.len(), "shadow length mismatch");
        if !self.normalize(eps, vector) {
            for v in shadow.iter_mut() {
                *v = M::zero();
            }
            return false;
        }
        for (s, v) in shadow.iter_mut().zip(vector.iter()) {
            *s = M::coerce_from(v).expect("normalized vectors must be convertible");
        }
        true
    }
}

#[derive(Clone, Copy, Debug, Default)]
pub struct F64Em7Epsilon;

impl Epsilon<f64> for F64Em7Epsilon {
    #[inline(always)]
    fn eps(&self) -> &f64 {
        static EPS: f64 = 1.0e-7;
        &EPS
    }

    #[inline(always)]
    fn neg_eps(&self) -> &f64 {
        static NEG_EPS: f64 = -1.0e-7;
        &NEG_EPS
    }
}

#[derive(Clone, Copy, Debug, Default)]
pub struct F64Em9Epsilon;

impl Epsilon<f64> for F64Em9Epsilon {
    #[inline(always)]
    fn eps(&self) -> &f64 {
        static EPS: f64 = 1.0e-9;
        &EPS
    }

    #[inline(always)]
    fn neg_eps(&self) -> &f64 {
        static NEG_EPS: f64 = -1.0e-9;
        &NEG_EPS
    }
}

#[derive(Clone, Copy, Debug, Default)]
pub struct F64Em12Epsilon;

impl Epsilon<f64> for F64Em12Epsilon {
    #[inline(always)]
    fn eps(&self) -> &f64 {
        static EPS: f64 = 1.0e-12;
        &EPS
    }

    #[inline(always)]
    fn neg_eps(&self) -> &f64 {
        static NEG_EPS: f64 = -1.0e-12;
        &NEG_EPS
    }
}

mod native {
    use crate::num::{Epsilon, F64Em9Epsilon, Num};

    impl Num for f64 {
        type LinAlg = crate::linalg::F64Ops;

        #[inline(always)]
        fn zero() -> Self {
            0.0
        }

        #[inline(always)]
        fn one() -> Self {
            1.0
        }

        #[inline(always)]
        fn from_u64(value: u64) -> Self {
            value as f64
        }

        #[inline(always)]
        fn abs(&self) -> Self {
            f64::abs(*self)
        }

        #[inline(always)]
        fn ref_add(&self, other: &Self) -> Self {
            *self + *other
        }

        #[inline(always)]
        fn ref_sub(&self, other: &Self) -> Self {
            *self - *other
        }

        #[inline(always)]
        fn ref_mul(&self, other: &Self) -> Self {
            *self * *other
        }

        #[inline(always)]
        fn ref_div(&self, other: &Self) -> Self {
            *self / *other
        }

        #[inline(always)]
        fn ref_neg(&self) -> Self {
            -*self
        }

        #[inline(always)]
        fn default_eps() -> impl Epsilon<Self> + Clone {
            F64Em9Epsilon
        }

        #[inline(always)]
        fn try_from_f64(value: f64) -> Option<Self> {
            value.is_finite().then_some(value)
        }

        #[inline(always)]
        fn to_f64(&self) -> f64 {
            *self
        }

        #[inline(always)]
        fn hash_component(&self) -> u64 {
            const MANTISSA_MASK: u64 = (1u64 << 10) - 1;
            let mut bits = self.to_bits();
            if bits == (-0.0f64).to_bits() {
                bits = 0.0f64.to_bits();
            }
            bits & !MANTISSA_MASK
        }
    }
}

#[cfg(feature = "rug")]
#[inline(always)]
pub fn f64_to_num_den(value: f64) -> (::rug::Integer, ::rug::Integer) {
    assert!(
        value.is_finite(),
        "f64_to_num_den requires a finite f64, got {value}"
    );

    if value == 0.0 {
        return (::rug::Integer::from(0), ::rug::Integer::from(1));
    }

    let bits = value.to_bits();
    let sign = (bits >> 63) != 0;
    let exp_bits = ((bits >> 52) & 0x7ff) as i32;
    let frac = bits & ((1u64 << 52) - 1);

    let (mut num, mut den) = if exp_bits == 0 {
        // subnormal: frac * 2^-1074
        let mut n = ::rug::Integer::from(frac);
        let mut d = ::rug::Integer::from(1);
        d <<= 1074usize;
        if sign {
            n = -n;
        }
        (n, d)
    } else {
        // normal: (1<<52 | frac) * 2^(exp-1023-52)
        let exp = exp_bits - 1023;
        let mant = (1u64 << 52) | frac;
        let mut n = ::rug::Integer::from(mant);
        let mut d = ::rug::Integer::from(1);

        let shift = exp - 52;
        if shift >= 0 {
            n <<= shift as usize;
        } else {
            d <<= (-shift) as usize;
        }

        if sign {
            n = -n;
        }
        (n, d)
    };

    while num.is_even() && den.is_even() {
        num >>= 1usize;
        den >>= 1usize;
    }

    (num, den)
}

#[cfg(feature = "rug")]
pub use rug::{RugFloat, RugRat};

#[cfg(feature = "rug")]
impl F64Parts for ::rug::Integer {
    #[inline(always)]
    fn f64_to_num_den(value: f64) -> (Self, Self) {
        crate::num::f64_to_num_den(value)
    }
}

#[cfg(feature = "dashu")]
impl F64Parts for dashu_int::IBig {
    #[inline(always)]
    fn f64_to_num_den(value: f64) -> (Self, Self) {
        use dashu_base::BitTest;
        use dashu_int::IBig;

        assert!(
            value.is_finite(),
            "f64_to_num_den requires a finite f64, got {value}"
        );
        if value == 0.0 {
            return (IBig::ZERO, IBig::ONE);
        }

        let bits = value.to_bits();
        let sign = (bits >> 63) != 0;
        let exp_bits = ((bits >> 52) & 0x7ff) as i32;
        let frac = bits & ((1u64 << 52) - 1);

        let (mut num, mut den) = if exp_bits == 0 {
            // subnormal: frac * 2^-1074
            let mut n = IBig::from(frac);
            let mut d = IBig::ONE;
            d <<= 1074usize;
            if sign {
                n = -n;
            }
            (n, d)
        } else {
            // normal: (1<<52 | frac) * 2^(exp-1023-52)
            let exp = exp_bits - 1023;
            let mant = (1u64 << 52) | frac;
            let mut n = IBig::from(mant);
            let mut d = IBig::ONE;

            let shift = exp - 52;
            if shift >= 0 {
                n <<= shift as usize;
            } else {
                d <<= (-shift) as usize;
            }

            if sign {
                n = -n;
            }
            (n, d)
        };

        while !num.bit(0) && !den.bit(0) {
            num >>= 1usize;
            den >>= 1usize;
        }

        (num, den)
    }
}

#[cfg(feature = "rug")]
mod rug {
    use ahash::RandomState;
    use derive_more::{Add, Display, Div, Mul, Neg, Sub};
    use std::hash::{BuildHasher, Hash, Hasher};

    use crate::num::{DynamicEpsilon, Epsilon, Num};

    #[inline]
    fn new_fast_hasher() -> impl Hasher {
        RandomState::with_seeds(0, 0, 0, 0).build_hasher()
    }

    #[repr(transparent)]
    #[derive(Clone, Debug, Display, PartialEq, PartialOrd, Add, Sub, Mul, Div, Neg)]
    #[mul(forward)]
    #[div(forward)]
    #[display("{}", _0)]
    pub struct RugRat(pub rug::Rational);

    impl<'a> std::ops::Add<&'a RugRat> for RugRat {
        type Output = RugRat;
        #[inline(always)]
        fn add(mut self, rhs: &'a RugRat) -> RugRat {
            self.0 += &rhs.0;
            self
        }
    }

    impl<'a> std::ops::Sub<&'a RugRat> for RugRat {
        type Output = RugRat;
        #[inline(always)]
        fn sub(mut self, rhs: &'a RugRat) -> RugRat {
            self.0 -= &rhs.0;
            self
        }
    }

    impl<'a> std::ops::Mul<&'a RugRat> for RugRat {
        type Output = RugRat;
        #[inline(always)]
        fn mul(mut self, rhs: &'a RugRat) -> RugRat {
            self.0 *= &rhs.0;
            self
        }
    }

    impl<'a> std::ops::Div<&'a RugRat> for RugRat {
        type Output = RugRat;
        #[inline(always)]
        fn div(mut self, rhs: &'a RugRat) -> RugRat {
            self.0 /= &rhs.0;
            self
        }
    }

    impl Num for RugRat {
        type LinAlg = crate::linalg::RugRatOps;

        #[inline(always)]
        fn zero() -> Self {
            Self(rug::Rational::from(0))
        }

        #[inline(always)]
        fn one() -> Self {
            Self(rug::Rational::from(1))
        }

        #[inline(always)]
        fn from_u64(value: u64) -> Self {
            Self(rug::Rational::from(value))
        }

        #[inline(always)]
        fn abs(&self) -> Self {
            Self(self.0.clone().abs())
        }

        #[inline(always)]
        fn ref_add(&self, other: &Self) -> Self {
            Self((&self.0 + &other.0).into())
        }

        #[inline(always)]
        fn ref_sub(&self, other: &Self) -> Self {
            Self((&self.0 - &other.0).into())
        }

        #[inline(always)]
        fn ref_mul(&self, other: &Self) -> Self {
            Self((&self.0 * &other.0).into())
        }

        #[inline(always)]
        fn ref_div(&self, other: &Self) -> Self {
            Self((&self.0 / &other.0).into())
        }

        #[inline(always)]
        fn ref_neg(&self) -> Self {
            Self((-&self.0).into())
        }

        #[inline(always)]
        fn try_from_f64(value: f64) -> Option<Self> {
            rug::Rational::from_f64(value).map(Self)
        }

        #[inline(always)]
        fn to_f64(&self) -> f64 {
            self.0.to_f64()
        }

        #[inline(always)]
        fn hash_component(&self) -> u64 {
            let mut hasher = new_fast_hasher();
            self.0.numer().hash(&mut hasher);
            self.0.denom().hash(&mut hasher);
            hasher.finish()
        }
    }

    #[repr(transparent)]
    #[derive(Clone, Debug, PartialEq, PartialOrd, Add, Sub, Mul, Div, Neg)]
    #[mul(forward)]
    #[div(forward)]
    pub struct RugFloat<const P: u32>(pub rug::Float);

    impl<const P: u32> super::InexactNum for RugFloat<P> {}

    impl<const P: u32> std::fmt::Display for RugFloat<P> {
        #[inline(always)]
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            std::fmt::Display::fmt(&self.0, f)
        }
    }

    impl<'a, const P: u32> std::ops::Add<&'a RugFloat<P>> for RugFloat<P> {
        type Output = RugFloat<P>;
        #[inline(always)]
        fn add(mut self, rhs: &'a RugFloat<P>) -> RugFloat<P> {
            self.0 += &rhs.0;
            self
        }
    }

    impl<'a, const P: u32> std::ops::Sub<&'a RugFloat<P>> for RugFloat<P> {
        type Output = RugFloat<P>;
        #[inline(always)]
        fn sub(mut self, rhs: &'a RugFloat<P>) -> RugFloat<P> {
            self.0 -= &rhs.0;
            self
        }
    }

    impl<'a, const P: u32> std::ops::Mul<&'a RugFloat<P>> for RugFloat<P> {
        type Output = RugFloat<P>;
        #[inline(always)]
        fn mul(mut self, rhs: &'a RugFloat<P>) -> RugFloat<P> {
            self.0 *= &rhs.0;
            self
        }
    }

    impl<'a, const P: u32> std::ops::Div<&'a RugFloat<P>> for RugFloat<P> {
        type Output = RugFloat<P>;
        #[inline(always)]
        fn div(mut self, rhs: &'a RugFloat<P>) -> RugFloat<P> {
            self.0 /= &rhs.0;
            self
        }
    }

    impl<const P: u32> Num for RugFloat<P> {
        type LinAlg = crate::linalg::GenericOps;

        #[inline(always)]
        fn zero() -> Self {
            Self(rug::Float::with_val(P, 0))
        }

        #[inline(always)]
        fn one() -> Self {
            Self(rug::Float::with_val(P, 1))
        }

        #[inline(always)]
        fn from_u64(value: u64) -> Self {
            Self(rug::Float::with_val(P, value))
        }

        #[inline(always)]
        fn abs(&self) -> Self {
            Self(self.0.clone().abs())
        }

        #[inline(always)]
        fn ref_add(&self, other: &Self) -> Self {
            Self(rug::Float::with_val(P, &self.0 + &other.0))
        }

        #[inline(always)]
        fn ref_sub(&self, other: &Self) -> Self {
            Self(rug::Float::with_val(P, &self.0 - &other.0))
        }

        #[inline(always)]
        fn ref_mul(&self, other: &Self) -> Self {
            Self(rug::Float::with_val(P, &self.0 * &other.0))
        }

        #[inline(always)]
        fn ref_div(&self, other: &Self) -> Self {
            Self(rug::Float::with_val(P, &self.0 / &other.0))
        }

        #[inline(always)]
        fn ref_neg(&self) -> Self {
            Self(rug::Float::with_val(P, -&self.0))
        }

        #[inline(always)]
        fn default_eps() -> impl Epsilon<Self> + Clone {
            DynamicEpsilon::<Self>::new(Self(rug::Float::with_val(P, 1.0e-7)))
        }

        #[inline(always)]
        fn try_from_f64(value: f64) -> Option<Self> {
            value
                .is_finite()
                .then_some(Self(rug::Float::with_val(P, value)))
        }

        #[inline(always)]
        fn to_f64(&self) -> f64 {
            self.0.to_f64()
        }

        #[inline(always)]
        fn hash_component(&self) -> u64 {
            const MANTISSA_MASK: u64 = (1u64 << 10) - 1;
            let mut bits = self.to_f64().to_bits();
            if bits == (-0.0f64).to_bits() {
                bits = 0.0f64.to_bits();
            }
            bits & !MANTISSA_MASK
        }
    }

    impl super::Rat for RugRat {
        type Int = rug::Integer;

        #[inline(always)]
        fn into_parts(self) -> (Self::Int, Self::Int) {
            self.0.into_numer_denom()
        }

        #[inline(always)]
        fn from_frac(numer: Self::Int, denom: Self::Int) -> Self {
            Self(rug::Rational::from((numer, denom)))
        }
    }
}

#[cfg(feature = "dashu")]
pub use dashu::{DashuFloat, DashuRat};

#[cfg(feature = "dashu")]
mod dashu {
    use ahash::RandomState;
    use derive_more::{Add, Div, Mul, Neg, Sub};
    use std::hash::{BuildHasher, Hash, Hasher};

    use crate::num::{DynamicEpsilon, Epsilon, Num};

    #[inline]
    fn new_fast_hasher() -> impl Hasher {
        RandomState::with_seeds(0, 0, 0, 0).build_hasher()
    }

    #[repr(transparent)]
    #[derive(Clone, Debug, PartialEq, PartialOrd, Add, Sub, Mul, Div, Neg)]
    #[mul(forward)]
    #[div(forward)]
    pub struct DashuRat(pub dashu_ratio::RBig);

    impl std::fmt::Display for DashuRat {
        #[inline(always)]
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            std::fmt::Display::fmt(&self.0, f)
        }
    }

    impl<'a> std::ops::Add<&'a DashuRat> for DashuRat {
        type Output = DashuRat;
        #[inline(always)]
        fn add(mut self, rhs: &'a DashuRat) -> DashuRat {
            self.0 += &rhs.0;
            self
        }
    }

    impl<'a> std::ops::Sub<&'a DashuRat> for DashuRat {
        type Output = DashuRat;
        #[inline(always)]
        fn sub(mut self, rhs: &'a DashuRat) -> DashuRat {
            self.0 -= &rhs.0;
            self
        }
    }

    impl<'a> std::ops::Mul<&'a DashuRat> for DashuRat {
        type Output = DashuRat;
        #[inline(always)]
        fn mul(mut self, rhs: &'a DashuRat) -> DashuRat {
            self.0 *= &rhs.0;
            self
        }
    }

    impl<'a> std::ops::Div<&'a DashuRat> for DashuRat {
        type Output = DashuRat;
        #[inline(always)]
        fn div(mut self, rhs: &'a DashuRat) -> DashuRat {
            self.0 /= &rhs.0;
            self
        }
    }

    impl Num for DashuRat {
        type LinAlg = crate::linalg::DashuRatOps;

        #[inline(always)]
        fn zero() -> Self {
            Self(dashu_ratio::RBig::from(0))
        }

        #[inline(always)]
        fn one() -> Self {
            Self(dashu_ratio::RBig::from(1))
        }

        #[inline(always)]
        fn from_u64(value: u64) -> Self {
            Self(dashu_ratio::RBig::from(value))
        }

        #[inline(always)]
        fn ref_add(&self, other: &Self) -> Self {
            Self(&self.0 + &other.0)
        }

        #[inline(always)]
        fn ref_sub(&self, other: &Self) -> Self {
            Self(&self.0 - &other.0)
        }

        #[inline(always)]
        fn ref_mul(&self, other: &Self) -> Self {
            Self(&self.0 * &other.0)
        }

        #[inline(always)]
        fn ref_div(&self, other: &Self) -> Self {
            Self(&self.0 / &other.0)
        }

        #[inline(always)]
        fn ref_neg(&self) -> Self {
            Self(-&self.0)
        }

        #[inline(always)]
        fn try_from_f64(value: f64) -> Option<Self> {
            // Use the exact rational representation of the float (binary rational).
            //
            // NOTE: `simplest_from_f64` is a *different* operation: it searches the rounding
            // interval for a "simple" fraction. That often produces non power-of-two denominators,
            // which can explode LCM scaling in exact integer algorithms.
            dashu_ratio::RBig::try_from(value).ok().map(Self)
        }

        #[inline(always)]
        fn to_f64(&self) -> f64 {
            self.0.to_f64().value()
        }
        #[inline(always)]
        fn hash_component(&self) -> u64 {
            let mut hasher = new_fast_hasher();
            self.0.hash(&mut hasher);
            hasher.finish()
        }
    }

    impl super::Rat for DashuRat {
        type Int = dashu_int::IBig;

        #[inline(always)]
        fn into_parts(self) -> (Self::Int, Self::Int) {
            let (numer, denom) = self.0.into_parts();
            (numer, dashu_int::IBig::from(denom))
        }

        #[inline(always)]
        fn from_frac(numer: Self::Int, denom: Self::Int) -> Self {
            Self(dashu_ratio::RBig::from_parts_signed(numer, denom))
        }
    }

    #[repr(transparent)]
    #[derive(Clone, Debug, PartialEq, PartialOrd, Add, Sub, Mul, Div, Neg)]
    #[mul(forward)]
    #[div(forward)]
    pub struct DashuFloat<const P: usize>(
        pub dashu_float::FBig<dashu_float::round::mode::HalfEven, 2>,
    );

    impl<const P: usize> super::InexactNum for DashuFloat<P> {}

    impl<const P: usize> std::fmt::Display for DashuFloat<P> {
        #[inline(always)]
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            std::fmt::Display::fmt(&self.0, f)
        }
    }

    impl<'a, const P: usize> std::ops::Add<&'a DashuFloat<P>> for DashuFloat<P> {
        type Output = DashuFloat<P>;
        #[inline(always)]
        fn add(mut self, rhs: &'a DashuFloat<P>) -> DashuFloat<P> {
            self.0 += &rhs.0;
            self
        }
    }

    impl<'a, const P: usize> std::ops::Sub<&'a DashuFloat<P>> for DashuFloat<P> {
        type Output = DashuFloat<P>;
        #[inline(always)]
        fn sub(mut self, rhs: &'a DashuFloat<P>) -> DashuFloat<P> {
            self.0 -= &rhs.0;
            self
        }
    }

    impl<'a, const P: usize> std::ops::Mul<&'a DashuFloat<P>> for DashuFloat<P> {
        type Output = DashuFloat<P>;
        #[inline(always)]
        fn mul(mut self, rhs: &'a DashuFloat<P>) -> DashuFloat<P> {
            self.0 *= &rhs.0;
            self
        }
    }

    impl<'a, const P: usize> std::ops::Div<&'a DashuFloat<P>> for DashuFloat<P> {
        type Output = DashuFloat<P>;
        #[inline(always)]
        fn div(mut self, rhs: &'a DashuFloat<P>) -> DashuFloat<P> {
            self.0 /= &rhs.0;
            self
        }
    }

    impl<const P: usize> Num for DashuFloat<P> {
        type LinAlg = crate::linalg::GenericOps;

        #[inline(always)]
        fn zero() -> Self {
            Self(dashu_float::FBig::from(0).with_precision(P).value())
        }

        #[inline(always)]
        fn one() -> Self {
            Self(dashu_float::FBig::from(1).with_precision(P).value())
        }

        #[inline(always)]
        fn from_u64(value: u64) -> Self {
            Self(dashu_float::FBig::from(value).with_precision(P).value())
        }

        #[inline(always)]
        fn ref_add(&self, other: &Self) -> Self {
            Self(&self.0 + &other.0)
        }

        #[inline(always)]
        fn ref_sub(&self, other: &Self) -> Self {
            Self(&self.0 - &other.0)
        }

        #[inline(always)]
        fn ref_mul(&self, other: &Self) -> Self {
            Self(&self.0 * &other.0)
        }

        #[inline(always)]
        fn ref_div(&self, other: &Self) -> Self {
            Self(&self.0 / &other.0)
        }

        #[inline(always)]
        fn ref_neg(&self) -> Self {
            Self(-&self.0)
        }

        #[inline(always)]
        fn default_eps() -> impl Epsilon<Self> + Clone {
            DynamicEpsilon::<Self>::new(Self::try_from_f64(1.0e-7).expect("default eps is finite"))
        }

        #[inline(always)]
        fn try_from_f64(value: f64) -> Option<Self> {
            dashu_float::FBig::<dashu_float::round::mode::HalfEven, 2>::try_from(value)
                .ok()
                .map(|v| Self(v.with_precision(P).value()))
        }

        #[inline(always)]
        fn to_f64(&self) -> f64 {
            self.0.to_f64().value()
        }

        #[inline(always)]
        fn hash_component(&self) -> u64 {
            const MANTISSA_MASK: u64 = (1u64 << 10) - 1;
            let mut bits = self.to_f64().to_bits();
            if bits == (-0.0f64).to_bits() {
                bits = 0.0f64.to_bits();
            }
            bits & !MANTISSA_MASK
        }
    }
}
