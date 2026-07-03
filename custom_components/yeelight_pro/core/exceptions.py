class YeelightProError(Exception):
    """Base error for Yeelight Pro client failures."""


class ProtocolError(YeelightProError):
    """Gateway data did not match the expected protocol."""


class ProtocolFrameTooLarge(ProtocolError):
    """Gateway sent a single framed JSON message above the configured limit."""


class GatewayErrorResponse(ProtocolError):
    """Gateway returned an explicit error response for a request."""


class ConnectionClosed(YeelightProError):
    """The TCP connection is not available."""


class RequestTimeout(YeelightProError):
    """A request did not receive a matching response id before timeout."""
