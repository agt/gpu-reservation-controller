"""``K8S_TLS_STRICT_VERIFY`` — relaxing OpenSSL's strict X.509 checks.

Python 3.13's ``ssl.create_default_context()`` enables ``VERIFY_X509_STRICT``,
and urllib3 2.x mirrors that flag in the context it builds for every HTTPS pool.
Under it OpenSSL rejects a chain whose certificates carry no Authority Key
Identifier — which a cluster PKI built by older tooling may not — and *every*
call to the API server fails, with a mounted, correct service-account
``ca.crt``.  The flag is the escape hatch until the apiserver certificate is
reissued.

These tests use the **real** ``kubernetes`` API client and the **real** urllib3
pool manager rather than stand-ins, because the risky part of the change is not
the arithmetic — it is the attribute path
``api_client.rest_client.pool_manager.connection_pool_kw`` and urllib3's
treatment of an injected ``ssl_context``.  A fake would keep passing after a
client upgrade moved either one.
"""

from __future__ import annotations

import logging
import ssl

import pytest
from kubernetes import client as k8s_client
from urllib3.util.ssl_ import create_urllib3_context

from app import k8s_client as k8s
from tests.conftest import kv_fields


def _api_client(*, verify_ssl: bool = True):
    """A real ``ApiClient``, hence a real ``RESTClientObject`` and pool manager."""
    configuration = k8s_client.Configuration()
    configuration.host = "https://10.0.0.1:443"
    configuration.verify_ssl = verify_ssl
    return k8s_client.ApiClient(configuration)


def _pool_kw(api_client):
    return api_client.rest_client.pool_manager.connection_pool_kw


class TestRelaxStrictTlsVerify:
    def test_injects_urllib3s_own_context_minus_the_strict_flag(self):
        """Exactly one bit differs from what urllib3 would have built itself.

        Stated as a difference against a reference context rather than as
        ``not flags & STRICT``, which is vacuously true on Python < 3.13 where
        urllib3 never sets the flag — this repo's image is 3.13, but the suite
        also runs on whatever the CI runner provides.
        """
        api_client = _api_client()

        k8s._relax_strict_tls_verify(api_client)

        context = _pool_kw(api_client)["ssl_context"]
        reference = create_urllib3_context(cert_reqs=ssl.CERT_REQUIRED)
        assert context.verify_flags == reference.verify_flags & ~ssl.VERIFY_X509_STRICT

    def test_the_flag_is_cleared_when_the_default_context_carries_it(
        self, monkeypatch
    ):
        """The 3.13 case, forced on whatever interpreter is running the suite.

        This is the behaviour the whole flag exists for, and on an older
        interpreter nothing else here would exercise it: urllib3 only sets
        ``VERIFY_X509_STRICT`` on 3.13+, so the assertions above would pass on
        3.11 even if the clearing line were deleted.
        """
        def strict_context(**kwargs):
            context = create_urllib3_context(**kwargs)
            context.verify_flags |= ssl.VERIFY_X509_STRICT
            return context

        monkeypatch.setattr(k8s, "create_urllib3_context", strict_context)
        api_client = _api_client()

        k8s._relax_strict_tls_verify(api_client)

        context = _pool_kw(api_client)["ssl_context"]
        assert not context.verify_flags & ssl.VERIFY_X509_STRICT

    def test_verification_itself_is_untouched(self):
        """The whole point: this is not ``insecure-skip-tls-verify``.

        Chain verification and hostname matching must both survive, or the flag
        would be a far larger concession than the one it advertises.
        """
        api_client = _api_client()

        k8s._relax_strict_tls_verify(api_client)

        context = _pool_kw(api_client)["ssl_context"]
        assert context.verify_mode == ssl.CERT_REQUIRED
        assert context.check_hostname is True

    def test_the_ca_file_is_still_the_pools_trust_anchor(self):
        """``ca_certs`` stays in place; urllib3 loads it into our context.

        Injecting ``ssl_context`` does not displace the other pool kwargs —
        ``ssl_wrap_socket`` calls ``load_verify_locations`` on whatever context
        it is handed — so the service-account CA is still what the chain is
        verified against.
        """
        api_client = _api_client()
        before = _pool_kw(api_client).get("ca_certs")

        k8s._relax_strict_tls_verify(api_client)

        assert _pool_kw(api_client).get("ca_certs") == before

    def test_an_insecure_kubeconfig_does_not_blow_up(self):
        """``verify_ssl: false`` yields ``CERT_NONE``, which forbids check_hostname.

        The context is built from the pool's *existing* ``cert_reqs`` for exactly
        this reason: hardcoding ``CERT_REQUIRED`` would leave ``check_hostname``
        on, and urllib3's later ``verify_mode = CERT_NONE`` would raise.
        """
        api_client = _api_client(verify_ssl=False)

        k8s._relax_strict_tls_verify(api_client)

        context = _pool_kw(api_client)["ssl_context"]
        assert context.check_hostname is False
        context.verify_mode = ssl.CERT_NONE  # what urllib3 does at connect time

    def test_each_client_gets_its_own_context(self):
        """One context per pool manager — they are configured independently."""
        core, coordination = _api_client(), _api_client()

        k8s._relax_strict_tls_verify(core, coordination)

        assert _pool_kw(core)["ssl_context"] is not _pool_kw(coordination)["ssl_context"]


class TestInitK8sWiring:
    @pytest.fixture
    def in_cluster(self, monkeypatch):
        """In-cluster auth, without a cluster: only the credential load is faked."""
        monkeypatch.setattr(k8s.k8s_config, "load_incluster_config", lambda: None)

    def test_strict_by_default_leaves_the_pools_alone(self, in_cluster):
        """No context is injected, so urllib3 builds its own — current behaviour."""
        k8s.init_k8s(None)

        assert "ssl_context" not in _pool_kw(k8s._core_v1.api_client)
        assert "ssl_context" not in _pool_kw(k8s._coordination_v1.api_client)

    def test_relaxing_covers_both_api_clients(self, in_cluster):
        """The coordination client is a separate pool manager from the core one.

        It is also the one that surfaced the failure — the singleton Lease is the
        first call startup makes — so missing it would leave the symptom in place
        while looking fixed.
        """
        k8s.init_k8s(None, strict_tls_verify=False)

        for api in (k8s._core_v1.api_client, k8s._coordination_v1.api_client):
            context = _pool_kw(api)["ssl_context"]
            assert not context.verify_flags & ssl.VERIFY_X509_STRICT

    def test_relaxing_says_so_at_warning(self, in_cluster, caplog):
        """A deployment on the escape hatch must not look normal in its logs."""
        with caplog.at_level(logging.WARNING, logger=k8s.log.name):
            k8s.init_k8s(None, strict_tls_verify=False)

        records = [r for r in caplog.records if "k8s.tls_relaxed" in r.getMessage()]
        assert len(records) == 1
        assert records[0].levelno == logging.WARNING
        assert kv_fields(records[0].getMessage())["name"] == "K8S_TLS_STRICT_VERIFY"

    def test_strict_is_silent(self, in_cluster, caplog):
        with caplog.at_level(logging.WARNING, logger=k8s.log.name):
            k8s.init_k8s(None)

        assert not [r for r in caplog.records if "k8s.tls_relaxed" in r.getMessage()]
