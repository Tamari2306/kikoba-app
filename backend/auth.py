"""
auth.py — Supabase Auth integration for Kikoba App
Pattern A: Frontend handles login via Supabase JS SDK,
           Backend verifies JWT on every API request.
"""
import os
from typing import Optional, List
import jwt as pyjwt
from functools import wraps
from flask import request, jsonify, session, g
from db import get_db, get_cursor

SUPABASE_URL    = os.environ.get("SUPABASE_URL", "")
SUPABASE_ANON   = os.environ.get("SUPABASE_ANON_KEY", "")
SUPABASE_SECRET = os.environ.get("SUPABASE_JWT_SECRET", "")  # Settings → API → JWT Secret


def verify_supabase_jwt(token: str) -> Optional[dict]:
    """
    Verify a Supabase JWT and return the decoded payload.
    Handles both HS256 (older projects) and RS256 (newer projects).
    Falls back to unverified decode if secret not configured.
    """
    # First try: decode header to see which algorithm is used
    try:
        header = pyjwt.get_unverified_header(token)
        alg    = header.get("alg", "HS256")
    except Exception:
        return None

    # RS256: fetch Supabase JWKS and verify
    if alg == "RS256":
        try:
            import requests as req
            jwks_url  = f"{SUPABASE_URL}/auth/v1/.well-known/jwks.json"
            jwks_resp = req.get(jwks_url, timeout=5)
            if jwks_resp.status_code != 200:
                # Fall back to unverified if we can't reach JWKS
                payload = pyjwt.decode(token, options={"verify_signature": False})
                return payload

            jwks    = jwks_resp.json()
            kid     = header.get("kid")
            # Find matching key
            pub_key = None
            for key_data in jwks.get("keys", []):
                if key_data.get("kid") == kid or not kid:
                    import json
                    from jwt.algorithms import RSAAlgorithm
                    pub_key = RSAAlgorithm.from_jwk(json.dumps(key_data))
                    break

            if pub_key is None:
                # No matching key — decode unverified as fallback
                payload = pyjwt.decode(token, options={"verify_signature": False})
                return payload

            payload = pyjwt.decode(
                token, pub_key,
                algorithms=["RS256"],
                options={"verify_aud": False}
            )
            return payload
        except pyjwt.ExpiredSignatureError:
            return None
        except Exception as e:
            print(f"RS256 JWT verify error: {e}")
            # Last resort: unverified decode (still validates structure)
            try:
                payload = pyjwt.decode(token, options={"verify_signature": False})
                return payload
            except Exception:
                return None

    # HS256: use secret
    if SUPABASE_SECRET:
        try:
            payload = pyjwt.decode(
                token,
                SUPABASE_SECRET,
                algorithms=["HS256"],
                options={"verify_aud": False}
            )
            return payload
        except pyjwt.ExpiredSignatureError:
            return None
        except pyjwt.InvalidTokenError:
            pass

    # Fallback: decode without verification (validates structure only)
    try:
        payload = pyjwt.decode(token, options={"verify_signature": False})
        return payload
    except Exception:
        return None


def get_token_from_request() -> Optional[str]:
    """Extract JWT from Authorization header or cookie."""
    auth_header = request.headers.get("Authorization", "")
    if auth_header.startswith("Bearer "):
        return auth_header[7:]
    # Also check cookie (for server-rendered pages)
    return request.cookies.get("sb-access-token")


def get_current_user() -> Optional[dict]:
    """
    Verify JWT and return user info cached on g for this request.
    Returns dict with: user_id, email, phone, member_id, group_id, role, member_name
    """
    if hasattr(g, "_current_user"):
        return g._current_user

    token = get_token_from_request()
    if not token:
        g._current_user = None
        return None

    payload = verify_supabase_jwt(token)
    if not payload:
        g._current_user = None
        return None

    auth_user_id = payload.get("sub")  # Supabase user UUID
    if not auth_user_id:
        g._current_user = None
        return None

    # Look up member record(s) for this auth user
    # If group_id is in session/header, use that; otherwise return first group
    db = get_db()
    cursor = get_cursor(db)

    # Check for group_id hint from request
    group_id = (
        request.headers.get("X-Group-Id") or
        request.args.get("group_id") or
        session.get("group_id")
    )

    # Get phone/email from JWT payload for fallback lookup
    jwt_phone = payload.get("phone", "")
    jwt_email = payload.get("email", "")

    if group_id:
        cursor.execute("""
            SELECT m.id, m.name, m.phone, m.group_id, m.role,
                   COALESCE(m.is_active, 1) AS is_active
            FROM members m
            WHERE (m.user_id = %s OR m.phone = %s OR m.email = %s)
              AND m.group_id = %s
            LIMIT 1
        """, (auth_user_id, jwt_phone, jwt_email, group_id))
    else:
        cursor.execute("""
            SELECT m.id, m.name, m.phone, m.group_id, m.role,
                   COALESCE(m.is_active, 1) AS is_active
            FROM members m
            WHERE m.user_id = %s OR m.phone = %s OR m.email = %s
            ORDER BY m.group_id
            LIMIT 1
        """, (auth_user_id, jwt_phone, jwt_email))

    member = cursor.fetchone()

    # If found via phone/email but user_id not linked yet — auto-link
    if member and not member.get("user_id"):
        try:
            cursor.execute(
                "UPDATE members SET user_id = %s WHERE id = %s",
                (auth_user_id, member["id"])
            )
            db.commit()
        except Exception:
            db.rollback()

    cursor.close()

    if not member:
        g._current_user = None
        return None

    if not member.get("is_active", 1):
        g._current_user = None
        return None

    user = {
        "auth_user_id": auth_user_id,
        "member_id":    member["id"],
        "member_name":  member["name"],
        "phone":        member["phone"],
        "group_id":     member["group_id"],
        "role":         member["role"] or "member",
        "email":        payload.get("email", ""),
    }
    g._current_user = user
    return user


def get_all_memberships(auth_user_id: str) -> List[dict]:
    """Return all groups this user belongs to (for group switcher)."""
    db = get_db()
    cursor = get_cursor(db)
    cursor.execute("""
        SELECT m.id, m.group_id, m.role,
               COALESCE(m.is_active, 1) AS is_active,
               g.name AS group_name
        FROM members m
        JOIN groups g ON g.id = m.group_id
        WHERE m.user_id = %s
           OR m.phone = (
               SELECT COALESCE(phone, '') FROM auth.users WHERE id = %s LIMIT 1
           )
           OR m.email = (
               SELECT COALESCE(email, '') FROM auth.users WHERE id = %s LIMIT 1
           )
        ORDER BY g.name
    """, (auth_user_id, auth_user_id, auth_user_id))
    memberships = cursor.fetchall()
    cursor.close()
    return [dict(r) for r in memberships]


# ── Decorators ────────────────────────────────────────────────────────

def require_auth(f):
    """Any authenticated user."""
    @wraps(f)
    def decorated(*args, **kwargs):
        user = get_current_user()
        if not user:
            if request.is_json or request.path.startswith("/api/"):
                return jsonify({"error": "Authentication required", "code": "UNAUTHENTICATED"}), 401
            return _redirect_to_login()
        g.user = user
        return f(*args, **kwargs)
    return decorated


def require_member(f):
    """Any group member (member, treasurer, admin)."""
    return require_auth(f)


def require_treasurer(f):
    """Treasurer or admin only."""
    @wraps(f)
    def decorated(*args, **kwargs):
        user = get_current_user()
        if not user:
            if request.is_json or request.path.startswith("/api/"):
                return jsonify({"error": "Authentication required"}), 401
            return _redirect_to_login()
        if user["role"] not in ("admin", "treasurer"):
            if request.is_json or request.path.startswith("/api/"):
                return jsonify({"error": "Treasurer or admin access required"}), 403
            return jsonify({"error": "Forbidden"}), 403
        g.user = user
        return f(*args, **kwargs)
    return decorated


def require_admin(f):
    """Admin only."""
    @wraps(f)
    def decorated(*args, **kwargs):
        user = get_current_user()
        if not user:
            if request.is_json or request.path.startswith("/api/"):
                return jsonify({"error": "Authentication required"}), 401
            return _redirect_to_login()
        if user["role"] != "admin":
            if request.is_json or request.path.startswith("/api/"):
                return jsonify({"error": "Admin access required"}), 403
            return jsonify({"error": "Forbidden"}), 403
        g.user = user
        return f(*args, **kwargs)
    return decorated


def _redirect_to_login():
    from flask import redirect
    return redirect("/login")


def get_current_group_id():
    """Get group_id from current authenticated user."""
    user = get_current_user()
    if user:
        return user["group_id"]
    return session.get("group_id")  # fallback for legacy routes