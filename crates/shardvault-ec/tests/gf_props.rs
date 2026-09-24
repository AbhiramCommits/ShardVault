use proptest::prelude::*;
use shardvault_ec::gf;

proptest! {
    #[test]
    fn mul_is_commutative(a in any::<u8>(), b in any::<u8>()) {
        prop_assert_eq!(gf::mul(a, b), gf::mul(b, a));
    }

    #[test]
    fn mul_is_associative(a in any::<u8>(), b in any::<u8>(), c in any::<u8>()) {
        prop_assert_eq!(gf::mul(gf::mul(a, b), c), gf::mul(a, gf::mul(b, c)));
    }

    #[test]
    fn mul_by_inverse_is_one(a in any::<u8>()) {
        prop_assume!(a != 0);
        prop_assert_eq!(gf::mul(a, gf::inv(a)), 1);
    }

    #[test]
    fn mul_distributes_over_add(a in any::<u8>(), b in any::<u8>(), c in any::<u8>()) {
        prop_assert_eq!(
            gf::mul(a, gf::add(b, c)),
            gf::add(gf::mul(a, b), gf::mul(a, c))
        );
    }

    #[test]
    fn add_is_xor(a in any::<u8>(), b in any::<u8>()) {
        prop_assert_eq!(gf::add(a, b), a ^ b);
    }

    #[test]
    fn div_undoes_mul(a in any::<u8>(), b in any::<u8>()) {
        prop_assume!(b != 0);
        prop_assert_eq!(gf::mul(gf::div(a, b), b), a);
    }

    #[test]
    fn double_inverse_is_identity(a in any::<u8>()) {
        prop_assume!(a != 0);
        prop_assert_eq!(gf::inv(gf::inv(a)), a);
    }
}
