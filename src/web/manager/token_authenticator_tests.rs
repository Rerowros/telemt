use super::*;

fn authenticator(key: u8) -> TokenAuthenticator {
    TokenAuthenticator::from_key(&[key; 32])
}

#[test]
fn process_tokens_are_domain_separated_and_fail_closed_after_mutation() {
    let issuer = authenticator(0x11);
    let other_process = authenticator(0x22);
    let nonce = [0x33; TOKEN_NONCE_BYTES];
    let bootstrap = issuer.issue(TokenKind::Bootstrap, nonce);
    let session = issuer.issue(TokenKind::Session, nonce);

    assert_ne!(bootstrap, session);
    assert!(issuer.authentic(&bootstrap));
    assert!(issuer.authentic(&session));
    assert!(!other_process.authentic(&bootstrap));
    assert!(!other_process.authentic(&session));

    for token in [bootstrap, session] {
        for index in 0..TOKEN_BYTES {
            let mut mutated = token;
            mutated[index] ^= 1;
            assert!(!issuer.authentic(&mutated), "mutation at byte {index}");
        }
    }
}

#[test]
fn one_keyed_hash_per_nonce_carries_both_token_tags() {
    let issuer = authenticator(0x44);
    let nonce = [0x55; TOKEN_NONCE_BYTES];
    let mut mac = Hmac::<Sha256>::new_from_slice(&[0x44; 32]).unwrap();
    mac.update(TOKEN_CONTEXT);
    mac.update(&nonce);
    let tags: [u8; 32] = mac.finalize().into_bytes().into();

    let bootstrap = issuer.issue(TokenKind::Bootstrap, nonce);
    let session = issuer.issue(TokenKind::Session, nonce);
    assert_eq!(bootstrap[..TOKEN_NONCE_BYTES], nonce);
    assert_eq!(bootstrap[TOKEN_NONCE_BYTES..], tags[..TOKEN_NONCE_BYTES]);
    assert_eq!(session[TOKEN_NONCE_BYTES..], tags[TOKEN_NONCE_BYTES..]);
    // The prepared state is cloned, never consumed: repeated checks agree.
    for _ in 0..3 {
        assert!(issuer.authentic(&bootstrap));
        assert!(issuer.authentic(&session));
    }
}
