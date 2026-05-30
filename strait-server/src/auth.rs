use std::sync::Arc;

use argon2::{
    Argon2, PasswordHash, PasswordHasher, PasswordVerifier,
    password_hash::{SaltString, rand_core::OsRng},
};
use axum::{
    extract::{FromRef, FromRequestParts},
    http::{StatusCode, request::Parts},
    response::{IntoResponse, Redirect, Response},
};
use axum_extra::extract::cookie::{Cookie, CookieJar, SameSite};
use hmac::{Hmac, Mac};
use sha2::Sha256;

use crate::{AppState, models::User};

const SESSION_COOKIE: &str = "strait_session";
type HmacSha256 = Hmac<Sha256>;

pub fn hash_password(password: &str) -> Result<String, String> {
    let salt = SaltString::generate(&mut OsRng);
    Argon2::default()
        .hash_password(password.as_bytes(), &salt)
        .map(|value| value.to_string())
        .map_err(|error| error.to_string())
}

pub fn verify_password(password: &str, hash: &str) -> bool {
    let Ok(parsed) = PasswordHash::new(hash) else {
        return false;
    };
    Argon2::default()
        .verify_password(password.as_bytes(), &parsed)
        .is_ok()
}

pub fn session_cookie(secret: &str, value: &str) -> Cookie<'static> {
    let signed_value = format!("{value}.{}", sign(secret, value));
    let mut cookie = Cookie::new(SESSION_COOKIE, signed_value);
    cookie.set_http_only(true);
    cookie.set_path("/");
    cookie.set_same_site(SameSite::Lax);
    cookie
}

pub fn clear_session_cookie() -> Cookie<'static> {
    let mut cookie = Cookie::new(SESSION_COOKIE, "");
    cookie.set_http_only(true);
    cookie.set_path("/");
    cookie.make_removal();
    cookie
}

#[derive(Clone)]
pub struct CurrentUser(pub User);

impl<S> FromRequestParts<S> for CurrentUser
where
    Arc<AppState>: FromRef<S>,
    S: Send + Sync,
{
    type Rejection = AuthRedirect;

    async fn from_request_parts(parts: &mut Parts, state: &S) -> Result<Self, Self::Rejection> {
        let state = Arc::<AppState>::from_ref(state);
        let jar = CookieJar::from_headers(&parts.headers);
        let session = jar
            .get(SESSION_COOKIE)
            .ok_or(AuthRedirect)?
            .value()
            .to_string();
        let session = verify_signed_cookie(&state.config.auth.session_secret, &session)
            .ok_or(AuthRedirect)?;
        let user = state
            .db
            .user_for_session(&session)
            .map_err(|_| AuthRedirect)?
            .ok_or(AuthRedirect)?;
        Ok(Self(user))
    }
}

pub struct AdminUser;

impl<S> FromRequestParts<S> for AdminUser
where
    Arc<AppState>: FromRef<S>,
    S: Send + Sync,
{
    type Rejection = AuthRedirect;

    async fn from_request_parts(parts: &mut Parts, state: &S) -> Result<Self, Self::Rejection> {
        let user = CurrentUser::from_request_parts(parts, state).await?.0;
        if user.role != "admin" {
            return Err(AuthRedirect);
        }
        Ok(Self)
    }
}

pub struct AuthRedirect;

impl IntoResponse for AuthRedirect {
    fn into_response(self) -> Response {
        Redirect::to("/login").into_response()
    }
}

pub fn unauthorized() -> Response {
    (StatusCode::UNAUTHORIZED, "invalid credentials").into_response()
}

fn sign(secret: &str, value: &str) -> String {
    let mut mac = HmacSha256::new_from_slice(secret.as_bytes()).expect("valid hmac key");
    mac.update(value.as_bytes());
    hex::encode(mac.finalize().into_bytes())
}

fn verify_signed_cookie(secret: &str, value: &str) -> Option<String> {
    let (session_id, signature) = value.rsplit_once('.')?;
    if sign(secret, session_id) == signature {
        Some(session_id.to_string())
    } else {
        None
    }
}
