"""Abstract authentication and authorization layer
"""

from functools import wraps
from typing import Any, Callable, Dict, List, Optional, Union

from flask import current_app, g, request
from typing_extensions import Protocol
from werkzeug.exceptions import Unauthorized as BaseUnauthorized

from giftless.util import get_callable

from . import allow_anon  # noqa: F401
from .identity import Identity

# We'll use the Werkzeug exception for Unauthorized, but encapsulate it
Unauthorized = BaseUnauthorized


# Type for "Authenticator"
# This can probably be made more specific once our protocol is more clear
class Authenticator(Protocol):
    """Authenticators are callables (an object or function) that can authenticate
    a request and provide an identity object
    """
    def __call__(self, request: Any) -> Optional[Identity]:
        raise NotImplementedError('This is a protocol definition and should not be called directly')


class PreAuthorizedActionAuthenticator(Authenticator):
    """Pre-authorized action authenticators are special authenticators
    that can also pre-authorize a follow-up action to the Git LFS server

    They serve to both pre-authorize Git LFS actions and check these actions
    are authorized as they come in.
    """
    def authorize_action(self, action: Dict[str, Any]) -> Dict[str, Any]:
        """Authorize an action
        """
        raise NotImplementedError('Implement this method in inheriting classes')


class Authentication:

    def __init__(self, app=None, default_identity: Identity = None):
        self._default_identity = default_identity
        self._authenticators: List[Authenticator] = []
        self._unauthorized_handler: Optional[Callable] = None
        self.preauth_handler: Optional[PreAuthorizedActionAuthenticator] = None

        if app is not None:
            self.init_app(app)

    def init_app(self, app):
        """Initialize the Flask app
        """
        app.config.setdefault('AUTH_PROVIDERS', [])

    def get_identity(self) -> Identity:
        if hasattr(g, 'user') and isinstance(g.user, Identity):
            return g.user

        g.user = self._authenticate()
        if g.user is None:
            # Fall back to returning an anon user with no permissions
            return allow_anon.AnonymousUser()
        else:
            return g.user

    def login_required(self, f):
        """A typical Flask "login_required" view decorator
        """
        @wraps(f)
        def decorated_function(*args, **kwargs):
            user = self.get_identity()
            if not user:
                return self.auth_failure()
            return f(*args, **kwargs)
        return decorated_function

    def no_identity_handler(self, f):
        """Marker decorator for "unauthorized handler" function

        This function will be called automatically if no authenticated identity was found
        but is required.
        """
        self._unauthorized_handler = f
        @wraps(f)
        def decorated_func(*args, **kwargs):
            return f(*args, **kwargs)
        return decorated_func

    def auth_failure(self):
        """Trigger an authentication failure
        """
        if self._unauthorized_handler:
            return self._unauthorized_handler()
        else:
            raise Unauthorized("User identity is required")

    def init_authenticators(self, reload=False):
        """Register an authenticator function
        """
        if reload:
            self._authenticators = None

        if self._authenticators:
            return

        self._authenticators = [_create_authenticator(a) for a in current_app.config['AUTH_PROVIDERS']]

        if current_app.config['PRE_AUTHORIZED_ACTION_PROVIDER']:
            self._preauth_handler = _create_authenticator(current_app.config['PRE_AUTHORIZED_ACTION_PROVIDER'])
            self.push_authenticator(self._preauth_handler)

    def push_authenticator(self, authenticator):
        """Push an authenticator at the top of the stack
        """
        self._authenticators.insert(0, authenticator)

    def _authenticate(self) -> Optional[Identity]:
        """Call all registered authenticators until we find an identity
        """
        self.init_authenticators()
        for authn in self._authenticators:
            try:
                identity = authn(request)
                if identity is not None:
                    return identity
            except Unauthorized:
                # An authenticator is telling us the provided identity is invalid
                # We should stop looking and return "no identity"
                return None

        return self._default_identity


def _create_authenticator(spec: Union[str, Dict[str, Any]]) -> Authenticator:
    """Instantiate an authenticator from configuration spec

    Configuration spec can be a string referencing a callable (e.g. mypackage.mymodule:callable)
    in which case the callable will be returned as is; Or, a dict with 'factory' and 'options'
    keys, in which case the factory callable is called with 'options' passed in as argument, and
    the resulting callable is returned.
    """
    if isinstance(spec, str):
        return get_callable(spec, __name__)

    factory = get_callable(spec['factory'], __name__)  # type: Callable[..., Authenticator]
    options = spec.get('options', {})
    return factory(**options)


authentication = Authentication()