use std::fmt;

use hkdf::Hkdf;
use sha2::Sha256;
use zeroize::Zeroizing;

use crate::configuration::NetworkKey;

const NETWORK_KEY_SALT: &[u8] = b"oll-sync-network-key\0v1";
const NOISE_PSK_INFO: &[u8] = b"oll-sync-noise-psk\0v1\0Noise_NNpsk0_25519_ChaChaPoly_BLAKE2s";

pub(crate) struct NoisePsk(Zeroizing<[u8; 32]>);

impl NoisePsk {
    pub(crate) fn expose(&self) -> &[u8; 32] {
        &self.0
    }
}

impl fmt::Debug for NoisePsk {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("NoisePsk(REDACTED)")
    }
}

pub(crate) fn derive_noise_psk(network_key: &NetworkKey) -> NoisePsk {
    let mut output = Zeroizing::new([0_u8; 32]);
    if network_key.expose().len() == output.len() {
        output.copy_from_slice(network_key.expose());
    } else {
        Hkdf::<Sha256>::new(Some(NETWORK_KEY_SALT), network_key.expose())
            .expand(NOISE_PSK_INFO, output.as_mut())
            .expect("SHA-256 HKDF can always produce a 32-byte Noise PSK");
    }
    NoisePsk(output)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn exactly_thirty_two_bytes_are_direct_and_other_lengths_use_the_fixed_hkdf_domain() {
        let direct = NetworkKey::new_for_test([0x5a; 32].to_vec());
        assert_eq!(derive_noise_psk(&direct).expose(), &[0x5a; 32]);

        let short = NetworkKey::new_for_test(b"short-key".to_vec());
        assert_eq!(
            derive_noise_psk(&short).expose(),
            &[
                0xaa, 0x85, 0xee, 0x4e, 0xf9, 0x04, 0xce, 0x75, 0xeb, 0xb0, 0x86, 0x85, 0xc2, 0x4b,
                0x2a, 0x97, 0xe6, 0x7e, 0x98, 0x03, 0x73, 0xde, 0x9e, 0xd7, 0xb2, 0x22, 0xf9, 0xe4,
                0x52, 0x47, 0xef, 0x7d,
            ]
        );
        assert_eq!(
            format!("{:?}", derive_noise_psk(&short)),
            "NoisePsk(REDACTED)"
        );
    }
}
