"""
auth.py — Supabase Auth integration for Kikoba App
Pattern A: Frontend handles login via Supabase JS SDK,
           Backend verifies JWT on every API request.
"""
import os
from typing import Optional, List

# PyJWT installs as the `jwt` module, not `PyJWT`.
# Using the actual import name avoids editor resolution warnings while
# keeping compatibility with the same runtime API.
try:
    import jwt as pyjwt
except ImportError:  # pragma: no cover
    pyjwt = None

from functools import wraps
from flask import request, jsonify, session, g
from db import get_db, get_cursor

SUPABASE_URL    = os.environ.get("SUPABASE_URL", "")
SUPABASE_ANON   = os.environ.get("SUPABASE_ANON_KEY", "")
SUPABASE_SECRET = os.environ.get("SUPABASE_JWT_SECRET", "")  # Settings → API → JWT Secret


def verify_supabase_jwt(token: str) -> Optional[dict]:
    if not SUPABASE_SECRET:
        print("❌ SUPABASE_JWT_SECRET is missing")
        return None

    try:
        payload = pyjwt.decode(
            token,
            SUPABASE_SECRET,
            algorithms=["HS256"],
            options={"verify_aud": False}
        )

        print("✅ Supabase JWT verified")
        print("JWT user ID:", payload.get("sub"))
        print("JWT email:", payload.get("email"))

        return payload

    except pyjwt.ExpiredSignatureError:
        print("❌ Supabase JWT expired")
        return None

    except pyjwt.InvalidTokenError as e:
        print(f"❌ Supabase JWT invalid: {e}")
        return None

    except Exception as e:
        print(f"❌ JWT verification error: {e}")
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

    if group_id:
        cursor.execute("""
            SELECT m.id, m.name, m.phone, m.group_id, m.role, m.is_active
            FROM members m
            WHERE m.user_id = %s AND m.group_id = %s
            LIMIT 1
        """, (auth_user_id, group_id))
    else:
        cursor.execute("""
            SELECT m.id, m.name, m.phone, m.group_id, m.role, m.is_active
            FROM members m
            WHERE m.user_id = %s
            ORDER BY m.group_id
            LIMIT 1
        """, (auth_user_id,))

    member = cursor.fetchone()
    cursor.close()

    if not member:
        g._current_user = None
        return None

    if not member["is_active"]:
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
        SELECT m.id, m.group_id, m.role, m.is_active, g.name AS group_name
        FROM members m
        JOIN groups g ON g.id = m.group_id
        WHERE m.user_id = %s AND m.is_active = 1
        ORDER BY g.name
    """, (auth_user_id,))
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