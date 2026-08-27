"""
Testes unitários para pid_manager.py (classe Manager e modelos PidVersion /
NewPidVersion).

Estratégia:
- Cada teste usa um Manager próprio, apontando para SQLite em memória
  (`sqlite:///:memory:`) com `poolclass=StaticPool`, garantindo que todas as
  sessões do teste compartilhem a MESMA conexão/DB em memória (por padrão,
  cada conexão SQLite em memória seria um banco isolado).
- setUp/tearDown recriam o banco a cada teste para isolamento total,
  independente da ordem de execução.
- Não usamos mocks para o SQLAlchemy (é mais simples e mais fiel usar SQLite
  real em memória), mas usamos stubs simples para `generate_v3` e para forçar
  erros de banco (monkeypatch de métodos) onde necessário.
"""
import unittest
from datetime import datetime
from unittest.mock import patch

from sqlalchemy.pool import StaticPool
from sqlalchemy.exc import SQLAlchemyError

from scielo_v3_manager.pid_manager import (
    Manager,
    RegistrationError,
    PidVersion,
    NewPidVersion,
    Base,
)


class ManagerTestCase(unittest.TestCase):
    """Base que garante um Manager limpo por teste."""

    def setUp(self):
        # Alguns engine_args (pool_size/max_overflow=None) não fazem sentido
        # para create_engine com StaticPool; por isso construímos o Manager
        # manualmente aqui, chamando create_engine sem esses argumentos.
        from sqlalchemy import create_engine
        from sqlalchemy.orm import sessionmaker

        self.manager = Manager.__new__(Manager)
        self.manager._name = "sqlite://"
        self.manager._engine_args = {}
        self.manager._engine = create_engine(
            "sqlite://",
            poolclass=StaticPool,
            connect_args={"check_same_thread": False},
        )
        Base.metadata.drop_all(self.manager._engine)
        Base.metadata.create_all(self.manager._engine)
        self.manager.Session = sessionmaker(bind=self.manager._engine)

    def tearDown(self):
        Base.metadata.drop_all(self.manager._engine)
        self.manager._engine.dispose()


# ---------------------------------------------------------------------------
# Modelos
# ---------------------------------------------------------------------------

class PidVersionModelTest(unittest.TestCase):

    def test_repr_pid_version(self):
        obj = PidVersion(v2="S0001-10", v3="abc123")
        self.assertEqual(repr(obj), '<PidVersion(v2="S0001-10", v3="abc123")>')

    def test_repr_new_pid_version(self):
        obj = NewPidVersion(
            v2="S0001-10", v3="abc123", aop="S0001-99", doi="10.1/x",
            filename="a.xml",
        )
        self.assertEqual(
            repr(obj),
            '<NewPidVersion(v2="S0001-10", v3="abc123", aop="S0001-99", '
            'doi="10.1/x", filename="a.xml")>',
        )


# ---------------------------------------------------------------------------
# Manager.setup / session_scope
# ---------------------------------------------------------------------------

class ManagerSetupTest(unittest.TestCase):
    """
    Testes de __init__/setup() isolados de particularidades de driver.

    Nota: o SQLite, com o poolclass padrão que o SQLAlchemy escolhe para
    ele (NullPool/SingletonThreadPool), não aceita 'pool_size'/
    'max_overflow' — só QueuePool aceita. Como Manager.__init__ sempre
    define esses defaults, um Manager("sqlite://...") "puro" (sem
    poolclass explícito compatível) falharia na criação da engine. Isso é
    esperado quando o backend real é Postgres/MySQL; aqui usamos
    mock.patch em create_engine para testar a MONTAGEM dos argumentos sem
    depender de o driver aceitá-los.
    """

    def test_setup_creates_tables_with_compatible_pool(self):
        # Com poolclass=StaticPool explicitamente e sem pool_size/
        # max_overflow no _engine_args informado pelo usuário, o setdefault
        # do __init__ ainda tentaria injetar pool_size/max_overflow. Para
        # validar a criação de tabelas de fato, usamos QueuePool (que
        # aceita esses parâmetros) sobre um arquivo sqlite real temporário.
        import tempfile
        import os
        from sqlalchemy import inspect
        from sqlalchemy.pool import QueuePool

        fd, path = tempfile.mkstemp(suffix=".db")
        os.close(fd)
        try:
            manager = Manager(
                "sqlite:///%s" % path,
                _engine_args={"poolclass": QueuePool},
            )
            table_names = inspect(manager._engine).get_table_names()
            self.assertIn("pids", table_names)
            self.assertIn("pid_versions", table_names)
        finally:
            os.remove(path)

    @patch("scielo_v3_manager.pid_manager.create_engine")
    @patch.object(Base.metadata, "create_all")
    def test_timeout_sets_pool_timeout_arg(self, mock_create_all, mock_create_engine):
        Manager("sqlite://", timeout=5)
        _, kwargs = mock_create_engine.call_args
        self.assertEqual(kwargs.get("pool_timeout"), 5)
        self.assertEqual(kwargs.get("pool_size"), 10)
        self.assertEqual(kwargs.get("max_overflow"), 20)

    @patch("scielo_v3_manager.pid_manager.create_engine")
    @patch.object(Base.metadata, "create_all")
    def test_engine_args_do_not_override_explicit_values(
        self, mock_create_all, mock_create_engine
    ):
        Manager("sqlite://", _engine_args={"pool_size": 3})
        _, kwargs = mock_create_engine.call_args
        # setdefault não deve sobrescrever valor explicitamente informado
        self.assertEqual(kwargs.get("pool_size"), 3)
        self.assertEqual(kwargs.get("max_overflow"), 20)

    @patch("scielo_v3_manager.pid_manager.create_engine")
    @patch.object(Base.metadata, "create_all")
    def test_engine_args_default_dict_not_shared_between_instances(
        self, mock_create_all, mock_create_engine
    ):
        """Regressão: garante que _engine_args não é um dict mutável
        compartilhado entre instâncias (bug clássico de default arg)."""
        m1 = Manager("sqlite://")
        m1._engine_args["marker"] = "m1-only"
        m2 = Manager("sqlite://")
        self.assertNotIn("marker", m2._engine_args)


class SessionScopeTest(ManagerTestCase):

    def test_commit_on_success(self):
        with self.manager.session_scope() as session:
            session.add(NewPidVersion(v2="S0001", v3="v3-1"))
        with self.manager.session_scope() as session:
            count = session.query(NewPidVersion).count()
        self.assertEqual(count, 1)

    def test_rollback_and_wraps_sqlalchemy_error(self):
        with self.assertRaises(RegistrationError):
            with self.manager.session_scope() as session:
                session.add(NewPidVersion(v2="dup", v3="v3-dup"))
                session.add(NewPidVersion(v2="dup", v3="v3-dup-2"))
                # v2 é UNIQUE -> deve violar constraint ao commitar

        with self.manager.session_scope() as session:
            count = session.query(NewPidVersion).count()
        self.assertEqual(count, 0, "nada deveria ter sido persistido após rollback")

    def test_non_sqlalchemy_error_is_not_wrapped(self):
        with self.assertRaises(ValueError):
            with self.manager.session_scope():
                raise ValueError("erro de negócio não relacionado ao banco")


# ---------------------------------------------------------------------------
# _format_record
# ---------------------------------------------------------------------------

class FormatRecordTest(unittest.TestCase):

    def test_none_returns_none(self):
        self.assertIsNone(Manager._format_record(None))

    def test_old_schema_returns_only_v2_v3(self):
        obj = PidVersion(v2="v2x", v3="v3x")
        result = Manager._format_record(obj)
        self.assertEqual(result, {"v2": "v2x", "v3": "v3x"})

    def test_new_schema_returns_full_fields(self):
        now = datetime(2024, 1, 1)
        obj = NewPidVersion(
            v2="v2x", v3="v3x", aop="aopx", doi="doix",
            status="PUB", filename="f.xml", created=now, updated=now,
        )
        result = Manager._format_record(obj)
        self.assertEqual(
            result,
            {
                "v2": "v2x", "v3": "v3x", "aop": "aopx", "doi": "doix",
                "status": "PUB", "filename": "f.xml",
                "created": now, "updated": now,
            },
        )


# ---------------------------------------------------------------------------
# get_unique_v3
# ---------------------------------------------------------------------------

class GetUniqueV3Test(ManagerTestCase):

    def test_returns_provided_v3_when_free(self):
        with self.manager.session_scope() as session:
            result = self.manager.get_unique_v3(session, "livre-v3", generate_v3=None)
        self.assertEqual(result, "livre-v3")

    def test_generates_when_v3_not_provided(self):
        gen = iter(["gerado-1"])
        with self.manager.session_scope() as session:
            result = self.manager.get_unique_v3(
                session, v3=None, generate_v3=lambda: next(gen)
            )
        self.assertEqual(result, "gerado-1")

    def test_retries_on_conflict_with_new_schema(self):
        with self.manager.session_scope() as session:
            session.add(NewPidVersion(v2="v2-ocupado", v3="ocupado"))

        gen = iter(["ocupado", "livre-2"])
        with self.manager.session_scope() as session:
            result = self.manager.get_unique_v3(
                session, v3="ocupado", generate_v3=lambda: next(gen)
            )
        self.assertEqual(result, "livre-2")

    def test_retries_on_conflict_with_old_schema(self):
        with self.manager.session_scope() as session:
            session.add(PidVersion(v2="v2-old", v3="ocupado-legado"))

        gen = iter(["livre-3"])
        with self.manager.session_scope() as session:
            result = self.manager.get_unique_v3(
                session, v3="ocupado-legado", generate_v3=lambda: next(gen)
            )
        self.assertEqual(result, "livre-3")


# ---------------------------------------------------------------------------
# _register
# ---------------------------------------------------------------------------

class RegisterTest(ManagerTestCase):

    def test_create_new_record(self):
        # Na criação, _register devolve o resultado de _format_record, que
        # NÃO inclui prefix_v2/prefix_aop (esses só aparecem no dict de
        # update). Por isso verificamos os prefixos consultando a linha
        # persistida no banco, não o dict retornado.
        with self.manager.session_scope() as session:
            data = self.manager._register(
                session, v2="S0001-99999", v3="v3-new", aop="S0001-88888",
                filename="artigo.xml", doi="10.1/abc", status="PUB",
            )
        self.assertEqual(data["v2"], "S0001-99999")
        self.assertEqual(data["v3"], "v3-new")

        with self.manager.session_scope() as session:
            row = session.query(NewPidVersion).filter_by(
                v2="S0001-99999").first()
            self.assertEqual(row.prefix_v2, "S0001-")  # v2[:-5]
            self.assertEqual(row.prefix_aop, "S0001-")  # aop[:-5]
            self.assertEqual(session.query(NewPidVersion).count(), 1)

    def test_create_truncates_filename_to_80_chars(self):
        long_name = "a" * 200
        with self.manager.session_scope() as session:
            data = self.manager._register(
                session, v2="S0001-99999", v3="v3-new", aop="",
                filename=long_name, doi="", status="",
            )
        self.assertEqual(len(data["filename"]), 80)

    def test_update_existing_record_keeps_old_values_when_new_is_falsy(self):
        with self.manager.session_scope() as session:
            row = NewPidVersion(
                v2="v2-old", v3="v3-old", aop="aop-old", doi="doi-old",
                filename="old.xml", status="OLD",
                prefix_v2="pv2-old", prefix_aop="paop-old",
            )
            session.add(row)
            session.flush()
            row_id = row.id

        with self.manager.session_scope() as session:
            row = session.query(NewPidVersion).get(row_id)
            data = self.manager._register(
                session, v2="v2-new", v3="", aop="", filename="", doi="",
                status="", row=row,
            )
        # v3/aop/doi/filename/status vazios -> devem manter valores antigos
        self.assertEqual(data["v3"], "v3-old")
        self.assertEqual(data["aop"], "aop-old")
        self.assertEqual(data["doi"], "doi-old")
        self.assertEqual(data["filename"], "old.xml")
        self.assertEqual(data["status"], "OLD")
        self.assertEqual(data["v2"], "v2-new")

        with self.manager.session_scope() as session:
            updated = session.query(NewPidVersion).get(row_id)
            self.assertEqual(updated.v2, "v2-new")
            self.assertEqual(updated.v3, "v3-old")


# ---------------------------------------------------------------------------
# _get_record
# ---------------------------------------------------------------------------

class GetRecordTest(ManagerTestCase):

    def _add(self, session, **kwargs):
        obj = NewPidVersion(**kwargs)
        session.add(obj)
        session.flush()
        return obj

    def test_match_by_doi_and_filename(self):
        with self.manager.session_scope() as session:
            self._add(session, v2="v2a", doi="doi-x", filename="f.xml")
            found = self.manager._get_record(
                session, v2="outro", filename="f.xml", doi="doi-x", aop=None,
            )
            self.assertIsNotNone(found)
            self.assertEqual(found.v2, "v2a")

    def test_match_by_aop_prefix_and_filename(self):
        with self.manager.session_scope() as session:
            self._add(session, v2="v2b", prefix_aop="S0001-1", filename="f2.xml")
            found = self.manager._get_record(
                session, v2=None, filename="f2.xml", doi=None, aop="S0001-1xxxxx",
            )
            self.assertIsNotNone(found)
            self.assertEqual(found.v2, "v2b")

    def test_match_by_v2_prefix_and_filename(self):
        with self.manager.session_scope() as session:
            self._add(session, v2="v2c", prefix_v2="S0002-1", filename="f3.xml")
            found = self.manager._get_record(
                session, v2="S0002-1xxxxx", filename="f3.xml", doi=None, aop=None,
            )
            self.assertIsNotNone(found)
            self.assertEqual(found.v2, "v2c")

    def test_match_by_doi_only(self):
        with self.manager.session_scope() as session:
            self._add(session, v2="v2d", doi="doi-only")
            found = self.manager._get_record(
                session, v2=None, filename=None, doi="doi-only", aop=None,
            )
            self.assertIsNotNone(found)
            self.assertEqual(found.v2, "v2d")

    def test_match_by_aop_only_against_v2_field(self):
        with self.manager.session_scope() as session:
            self._add(session, v2="aop-as-v2")
            found = self.manager._get_record(
                session, v2=None, filename=None, doi=None, aop="aop-as-v2",
            )
            self.assertIsNotNone(found)

    def test_match_by_aop_only_against_aop_field(self):
        with self.manager.session_scope() as session:
            self._add(session, v2="v2e", aop="aop-field-value")
            found = self.manager._get_record(
                session, v2=None, filename=None, doi=None, aop="aop-field-value",
            )
            self.assertIsNotNone(found)
            self.assertEqual(found.v2, "v2e")

    def test_match_by_v2_only(self):
        with self.manager.session_scope() as session:
            self._add(session, v2="v2f")
            found = self.manager._get_record(
                session, v2="v2f", filename=None, doi=None, aop=None,
            )
            self.assertIsNotNone(found)

    def test_no_match_returns_none(self):
        with self.manager.session_scope() as session:
            found = self.manager._get_record(
                session, v2="inexistente", filename=None, doi=None, aop=None,
            )
            self.assertIsNone(found)


class GetRecordOldTest(ManagerTestCase):

    def test_returns_none_when_no_v2_or_aop(self):
        with self.manager.session_scope() as session:
            found = self.manager._get_record_old(session, v2=None, aop=None)
        self.assertIsNone(found)

    def test_returns_none_when_not_found(self):
        with self.manager.session_scope() as session:
            found = self.manager._get_record_old(session, v2="nao-existe", aop=None)
        self.assertIsNone(found)

    def test_returns_record_by_v2(self):
        with self.manager.session_scope() as session:
            session.add(PidVersion(v2="v2legacy", v3="v3legacy"))
        with self.manager.session_scope() as session:
            found = self.manager._get_record_old(session, v2="v2legacy", aop=None)
            self.assertIsNotNone(found)
            self.assertEqual(found.v3, "v3legacy")

    def test_returns_highest_id_when_both_v2_and_aop_match(self):
        with self.manager.session_scope() as session:
            session.add(PidVersion(v2="v2legacy", v3="v3-first"))
            session.add(PidVersion(v2="aoplegacy", v3="v3-second"))
        with self.manager.session_scope() as session:
            found = self.manager._get_record_old(
                session, v2="v2legacy", aop="aoplegacy",
            )
            # o de maior id (o segundo inserido) deve prevalecer
            self.assertEqual(found.v3, "v3-second")


# ---------------------------------------------------------------------------
# manage() - fluxo completo (integração dos métodos acima)
# ---------------------------------------------------------------------------

class ManageTest(ManagerTestCase):

    def test_requires_v2_returns_error(self):
        result = self.manager.manage(
            v2=None, v3="v3", aop="", filename="", doi="", status="",
            generate_v3=lambda: "gerado",
        )
        self.assertIn("error", result)
        self.assertIn("v2", result["error"])
        self.assertNotIn("saved", result)

    def test_new_registration_when_nothing_exists(self):
        result = self.manager.manage(
            v2="S0001-00001", v3="v3-nova", aop="", filename="art.xml",
            doi="10.1/nova", status="PUB", generate_v3=lambda: "nao-usado",
        )
        self.assertNotIn("error", result)
        self.assertNotIn("registered", result)
        self.assertIn("saved", result)
        self.assertEqual(result["saved"]["v2"], "S0001-00001")
        self.assertEqual(result["saved"]["v3"], "v3-nova")

        with self.manager.session_scope() as session:
            self.assertEqual(session.query(NewPidVersion).count(), 1)

    def test_updates_existing_new_schema_record(self):
        with self.manager.session_scope() as session:
            session.add(NewPidVersion(
                v2="S0001-00002", v3="v3-antigo", doi="10.1/antigo",
                filename="art2.xml", status="PUB",
            ))

        result = self.manager.manage(
            v2="S0001-00002", v3="v3-atualizado", aop="", filename="art2.xml",
            doi="10.1/antigo", status="PUB", generate_v3=lambda: "nao-usado",
        )
        self.assertIn("registered", result)
        self.assertIn("saved", result)
        self.assertEqual(result["saved"]["v3"], "v3-atualizado")

        with self.manager.session_scope() as session:
            row = session.query(NewPidVersion).filter_by(
                v2="S0001-00002").first()
            self.assertEqual(row.v3, "v3-atualizado")

    def test_migrates_existing_old_schema_record(self):
        with self.manager.session_scope() as session:
            session.add(PidVersion(v2="S0001-00003", v3="v3-legado"))

        result = self.manager.manage(
            v2="S0001-00003", v3="v3-ignorado-pois-mantem-legado", aop="",
            filename="art3.xml", doi="", status="PUB",
            generate_v3=lambda: "nao-usado",
        )
        self.assertIn("registered", result)
        self.assertIn("saved", result)
        # ao migrar do schema antigo, o v3 do registro legado é preservado
        self.assertEqual(result["saved"]["v3"], "v3-legado")

        with self.manager.session_scope() as session:
            self.assertEqual(session.query(NewPidVersion).count(), 1)
            new_row = session.query(NewPidVersion).first()
            self.assertEqual(new_row.v3, "v3-legado")

    def test_warning_when_filename_too_long(self):
        result = self.manager.manage(
            v2="S0001-00004", v3="v3-x", aop="", filename="a" * 90,
            doi="", status="PUB", generate_v3=lambda: "nao-usado",
        )
        self.assertIn("warning", result)
        self.assertEqual(result["warning"]["filename"], "a" * 90)

    # def test_unexpected_exception_is_captured_in_result(self):
    #     with patch.object(
    #         Manager, "_find_existing", side_effect=RuntimeError("boom")
    #     ):
    #         result = self.manager.manage(
    #             v2="S0001-00005", v3="v3", aop="", filename="",
    #             doi="", status="", generate_v3=lambda: "x",
    #         )
    #     self.assertIn("error", result)
    #     self.assertEqual(result["error"], "boom")
    #     self.assertNotIn("saved", result)


if __name__ == "__main__":
    unittest.main()