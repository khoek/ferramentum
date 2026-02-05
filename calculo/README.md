# calculo

Pluggable numeric traits for exact and approximate arithmetic. Built for
algorithms that need to swap between fast floats and arbitrary-precision
rationals without changing application code.

## Features

- **`Num` trait**: Unified interface for `f64`, arbitrary-precision rationals, and floats.
- **`Int` trait**: Exact integer ops for fraction-free (Bareiss) pivoting and product comparisons.
- **`Rat` trait**: Those `Num`s which conveniently decompose into numerator/denominator pairs.
- **Backend-agnostic**: Write once, run with `f64`, `rug::Rational`, or `dashu::RBig`.

## Backends

| Feature | Types | Notes |
|---------|-------|-------|
| - / `simd` | `f64` | Native floats / optional SIMD-acceleration via `pulp` |
| `rug` | `RugRat`, `RugFloat<P: usize>` | GMP-backed arbitrary precision |
| `dashu` | `DashuRat`, `DashuFloat<P: usize>` | Pure-Rust arbitrary precision |

## Example

```rust
use calculo::linalg;
use calculo::num::Num;

fn normalize<N: Num>(v: &mut [N]) {
    let scale = v.iter().map(|x| x.abs()).fold(N::zero(), |a, b| if b > a { b } else { a });
    linalg::div_assign(v, &scale);
}
```

Switch precision by changing the type parameter---no algorithm changes required.

## License

Licensed under AGPL-3.0-only. See `LICENSE` for details.
