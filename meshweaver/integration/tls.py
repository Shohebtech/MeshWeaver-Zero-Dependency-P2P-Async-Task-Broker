import functools
import os
import ssl
from typing import Callable, Optional

from meshweaver.integration.task_channel import send_task

CERT_VARIABLE = "MESHWEAVER_TLS_CERT"
KEY_VARIABLE = "MESHWEAVER_TLS_KEY"
CA_VARIABLE = "MESHWEAVER_TLS_CA"


class TLSConfigError(Exception):
    """The certificate files are missing or unusable."""


def _load(context: ssl.SSLContext, cert: str, key: str, ca: str) -> ssl.SSLContext:
    context.minimum_version = ssl.TLSVersion.TLSv1_2
    context.verify_mode = ssl.CERT_REQUIRED           # the other side must show a certificate
    try:
        context.load_cert_chain(cert, key)            # our own identity
        context.load_verify_locations(ca)             # the only CA we trust
    except (OSError, ssl.SSLError) as exc:
        raise TLSConfigError(f"cannot load TLS files: {exc}") from exc
    return context


def make_server_context(cert: str, key: str, ca: str) -> ssl.SSLContext:
    """For TaskServer(ssl_context=...). Clients must present a certificate from the CA."""
    return _load(ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER), cert, key, ca)


def make_client_context(cert: str, key: str, ca: str) -> ssl.SSLContext:
    """For send_task(ssl_context=...). The server's certificate must come from the CA."""
    context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
    # Mesh nodes are reached by IP address, so a name check would always fail.
    # Trust comes from the shared CA, not from the host name.
    context.check_hostname = False
    return _load(context, cert, key, ca)


def contexts_from_env() -> Optional[tuple]:
    """
    (server_context, client_context) from the three environment variables, or None
    if none of them is set. Setting only some of them is an error, so a typo can
    never switch encryption off silently.
    """
    values = [os.environ.get(name) for name in (CERT_VARIABLE, KEY_VARIABLE, CA_VARIABLE)]
    if not any(values):
        return None
    if not all(values):
        raise TLSConfigError(
            f"set all of {CERT_VARIABLE}, {KEY_VARIABLE} and {CA_VARIABLE}, or none of them")
    cert, key, ca = values
    return make_server_context(cert, key, ca), make_client_context(cert, key, ca) #type: ignore


def tls_sender(client_context: ssl.SSLContext) -> Callable:
    """A send_task that always uses TLS. Pass it as TaskManager(send=...)."""
    return functools.partial(send_task, ssl_context=client_context)