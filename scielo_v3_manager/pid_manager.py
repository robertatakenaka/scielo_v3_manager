import logging
from contextlib import contextmanager
from datetime import datetime, timezone

from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import (
    Column, Integer, String, DateTime,
    UniqueConstraint, create_engine,
)
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import IntegrityError, SQLAlchemyError

logger = logging.getLogger(__name__)

PID_SUFFIX_LENGTH = 5
MAX_V3_ATTEMPTS = 10
DEFAULT_ENGINE_ARGS = {"pool_size": 10, "max_overflow": 20}
MAX_FILENAME_LENGTH = 80

Base = declarative_base()


def utcnow():
    return datetime.now(timezone.utc).replace(tzinfo=None)


def pid_prefix(pid):
    if not pid or len(pid) <= PID_SUFFIX_LENGTH:
        return ""
    return pid[:-PID_SUFFIX_LENGTH]


class RegistrationError(Exception):
    ...


class RegistrationConflict(RegistrationError):
    """Violação de unicidade — vale a pena tentar de novo."""


class PidVersion(Base):
    __tablename__ = 'pid_versions'

    id = Column(Integer, primary_key=True, autoincrement=True)
    v2 = Column(String(23))
    v3 = Column(String(255))
    __table_args__ = (
        UniqueConstraint('v2', 'v3', name='_v2_v3_uc'),
    )

    def __repr__(self):
        return '<PidVersion(v2="%s", v3="%s")>' % (self.v2, self.v3)


class NewPidVersion(Base):
    __tablename__ = 'pids'

    id = Column(Integer, primary_key=True, autoincrement=True)
    v2 = Column(String(23), index=True, unique=True)
    v3 = Column(String(23), index=True, unique=True)
    aop = Column(String(23), index=True)
    filename = Column(String(80), index=True)
    prefix_v2 = Column(String(18), index=True)
    prefix_aop = Column(String(18), index=True)
    doi = Column(String(80), index=True)
    status = Column(String(6))
    created = Column(DateTime, default=datetime.utcnow)
    updated = Column(DateTime, default=datetime.utcnow, onupdate=datetime.now)

    __table_args__ = (
        UniqueConstraint('v3', name='_pids_'),
    )

    def __repr__(self):
        return (
            '<NewPidVersion(v2="%s", v3="%s", aop="%s", doi="%s", filename="%s")>' %
            (self.v2, self.v3, self.aop, self.doi, self.filename)
        )


class Manager:
    def __init__(self, name, timeout=None, _engine_args=None, create_tables=False):
        self._name = name
        self._engine_args = dict(DEFAULT_ENGINE_ARGS)
        if timeout:
            self._engine_args["pool_timeout"] = timeout
        self._engine_args.update(_engine_args or {})
        self._create_tables = create_tables
        self.setup()

    def setup(self):
        self._engine = create_engine(
            self._name, logging_name="pid_manager", **self._engine_args)
        if self._create_tables:
            Base.metadata.create_all(self._engine)
        self.Session = sessionmaker(bind=self._engine)

    @contextmanager
    def session_scope(self):
        """Provide a transactional scope around a series of operations."""
        session = self.Session()
        try:
            yield session
            session.commit()
        except IntegrityError as e:
            session.rollback()
            raise RegistrationConflict("Conflito de unicidade: %s" % e)
        except SQLAlchemyError as e:
            session.rollback()
            raise RegistrationError("Rollback: %s" % e)
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()

    @staticmethod
    def _format_record(registered):
        if registered:
            result = {
                "v3": registered.v3,
                "v2": registered.v2,
            }
            if isinstance(registered, NewPidVersion):
                result.update(
                    {
                        "aop": registered.aop,
                        "doi": registered.doi,
                        "status": registered.status,
                        "filename": registered.filename,
                        "created": registered.created,
                        "updated": registered.updated,
                    }
                )
            return result

    def manage(self, v2, v3, aop, filename, doi, status, generate_v3):
        """
        Obtém registro consultando por v2, aop, doi, filename.
        Cria / atualiza o registro.
        Retorna dicionário cujas chaves são:
            input, registered, saved, error, warning
        """
        result = {
            "input": {
                "v3": v3, "v2": v2, "aop": aop,
                "doi": doi, "filename": filename, "status": status,
            }
        }
        if not v2:
            result["error"] = "Manager.manage requires parameters: v2"
            return result

        filename = filename or ""
        if len(filename) > MAX_FILENAME_LENGTH:
            result["warning"] = {"filename": filename}
            filename = filename[:MAX_FILENAME_LENGTH]

        saved = None
        try:
            with self.session_scope() as session:
                registered = self.get_registered(session, v2, filename, doi, aop)
                if registered:
                    result["registered"] = self._format_record(registered)
                saved = self.save(
                    session, registered, v2, v3, aop, filename, doi, status, generate_v3
                )
        except Exception as e:
            logger.exception("Erro ao registrar v2=%s filename=%s", v2, filename)
            result["error"] = "%s: %s" % (type(e).__name__, e)
            return result

        if saved:
            result["saved"] = saved
        return result

    def get_registered(self, session, v2, filename, doi, aop):
        return (
            self._get_record(session, v2, filename, doi, aop)
            or self._get_record_old(session, v2, aop)
        )

    def save(self, session, registered, v2, v3, aop, filename, doi, status, generate_v3):
        row = None
        if registered:
            if not isinstance(registered, NewPidVersion):
                pid_v3 = registered.v3
            else:
                pid_v3 = v3
                row = registered
        else:
            pid_v3 = self.get_unique_v3(session, v3, generate_v3)
        return self._register(
            session, v2, pid_v3, aop, filename, doi, status, row
        )

    def get_unique_v3(self, session, v3, generate_v3):
        unique_v3 = v3 or generate_v3()
        while True:
            exist = bool(
                session.query(NewPidVersion).filter_by(v3=unique_v3).first() or
                session.query(PidVersion).filter_by(v3=unique_v3).first()
            )
            if not exist:
                return unique_v3
            unique_v3 = generate_v3()

    def _register(self, session, v2, v3, aop, filename, doi, status, row=None):
        filename = (filename or "")[:MAX_FILENAME_LENGTH]  # [F1 · C2]
        prefix_v2 = v2[:-5] if v2 else ""
        prefix_aop = aop[:-5] if aop else ""

        if row is not None:
            data = {
                "v2": v2,
                "v3": v3 or row.v3,
                "aop": aop or row.aop,
                "doi": doi or row.doi,
                "filename": filename or row.filename,
                "status": status or row.status,
                "prefix_aop": prefix_aop or row.prefix_aop,
                "prefix_v2": prefix_v2 or row.prefix_v2,
            }
            session.query(NewPidVersion).filter(
                NewPidVersion.id == row.id
            ).update(data, synchronize_session=False)
            return data

        record = NewPidVersion(
            v2=v2,
            v3=v3,
            aop=aop or "",
            filename=filename,
            doi=doi or "",
            status=status or "",
            prefix_aop=prefix_aop,
            prefix_v2=prefix_v2,
        )
        session.add(record)
        session.flush()
        return self._format_record(record)

    def _get_record_by_v3(self, session, v3, v2, filename, doi, aop):
        if not v3:
            return

        for rec in session.query(NewPidVersion).filter_by(v3=v3).all():
            if filename and filename == rec.filename:
                return rec
            if doi and doi == rec.doi:
                return rec
            if aop and aop in (rec.v2, rec.aop):
                return rec
            if v2 and v2 in (rec.v2, rec.aop):
                return rec

        for rec in session.query(PidVersion).filter_by(v3=v3).all():
            if aop and aop == rec.v2:
                return rec
            if v2 and v2 == rec.v2:
                return rec

    from sqlalchemy import desc, or_


    def _get_record(self, session, v2, filename, doi, aop):
        filters = []

        if filename:
            if doi:
                filters.append(
                    (NewPidVersion.doi == doi) & (NewPidVersion.filename == filename)
                )
            for pid in (aop, v2):
                prefix = pid_prefix(pid)
                if prefix:
                    filters.append(
                        (NewPidVersion.prefix_v2 == prefix)
                        & (NewPidVersion.filename == filename)
                    )
                    filters.append(
                        (NewPidVersion.prefix_aop == prefix)
                        & (NewPidVersion.filename == filename)
                    )

        if doi:
            filters.append(NewPidVersion.doi == doi)
        if aop:
            filters.append(NewPidVersion.v2 == aop)
            filters.append(NewPidVersion.aop == aop)
        if v2:
            filters.append(NewPidVersion.v2 == v2)
            filters.append(NewPidVersion.aop == v2)

        if not filters:
            return None

        # Aplica os filtros OR, ordena do mais recente para o mais antigo e pega o primeiro
        return (
            session.query(NewPidVersion)
            .filter(or_(*filters))
            .order_by(desc(NewPidVersion.updated_at))  # Ajuste o nome do atributo se necessário
            .first()
        )

    def _get_record_old(self, session, v2, aop):
        i = 0
        record = None
        if aop:
            for rec in session.query(PidVersion).filter_by(v2=aop).all():
                if rec.id > i:
                    i = rec.id
                    record = rec
        if v2:
            for rec in session.query(PidVersion).filter_by(v2=v2).all():
                if rec.id > i:
                    i = rec.id
                    record = rec
        return record
