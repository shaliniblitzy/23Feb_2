"""Payment Service source package.

This package houses the FastAPI/ASGI application for the Payment Service:
the dual-provider (Stripe + Razorpay) payment processing hub described in
AAP Section 0.1.1 (Component #7) and AAP Section 0.5.2.2 bullet 7.

Public API
----------
This module intentionally exposes ONLY the package version constant.
Consumers import specific submodules directly, for example::

    from src.main import app
    from src.container import build_container
    from src.controllers.health import router as health_router
    from src.providers.stripe.stripe_provider import StripeProvider
    from src.providers.razorpay.razorpay_provider import RazorpayProvider
    from src.providers.routing import ProviderRouter

Do NOT add submodule imports or side-effect code here. The Payment
Service's heavy SDK dependencies (stripe, razorpay, confluent-kafka,
cryptography, boto3) make eager submodule imports expensive and risk
import-time side effects (e.g., SDK telemetry hooks) that would break
testability and slow process startup.
"""

from __future__ import annotations

__version__: str = "1.0.0"

__all__: list[str] = ["__version__"]
