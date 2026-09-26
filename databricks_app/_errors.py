"""Consistent user-facing errors for App operations backed by the SDK."""

from __future__ import annotations

from flask import jsonify


def friendly_message(exc: Exception, operation: str) -> str:
    """Give the user a next step without putting an SDK traceback in the headline."""
    detail = str(exc).lower()
    kind = type(exc).__name__.lower()
    if any(word in detail or word in kind for word in ('permission_denied', 'permission denied', 'forbidden', 'unauthorized', '403')):
        return (
            f'Could not {operation}: access was denied. Ask a workspace admin to grant the '
            'App service principal the required permissions, then try again.'
        )
    if 'warehouse' in detail and ('not found' in detail or 'does not exist' in detail):
        return f'Could not {operation}: the SQL warehouse is unavailable. Select an accessible warehouse and try again.'
    return f'Could not {operation}. Open Details for the technical error, then try again or contact an administrator.'


def exception_response(exc: Exception, operation: str, status: int = 500):
    """Preserve the exception for diagnostics while keeping it out of the headline."""
    return jsonify({
        'error': friendly_message(exc, operation),
        'details': f'{type(exc).__name__}: {exc}',
    }), status
