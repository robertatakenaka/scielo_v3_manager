import logging
from datetime import datetime
from contextlib import contextmanager

from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import (
    Column, Integer, String, DateTime,
    UniqueConstraint, create_engine,
)
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import SQLAlchemyError

Base = declarative_base()

logging.basicConfig()
logging.getLogger("sqlalchemy.engine").setLevel(logging.INFO)
logging.getLogger("sqlalchemy.pool").setLevel(logging.DEBUG)


class RegistrationError(Exception):
    ...


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
    def __init__(self, name, timeout=None, _engine_args={}):
        self._name = name
        self._engine_args = {"pool_timeout": timeout} if timeout else {}
        self._engine_args.update({"pool_size": 10, "max_overflow": 20})
        self._engine_args.update(_engine_args)
        self.setup()

    def setup(self):
        self._engine = create_engine(
            self._name, logging_name='pid_manager', **self._engine_args)
        Base.metadata.create_all(self._engine)
        self.Session = sessionmaker(bind=self._engine)

    @contextmanager
    def session_scope(self):
        """Provide a transactional scope around a series of operations."""
        session = self.Session()
        try:
            yield session
            session.commit()
        except SQLAlchemyError as e:
            session.rollback()
            raise RegistrationError("Rollback: %s" % str(e))
        finally:
            session.close()

    @staticmethod
    def _format_record(registered):
        if registered:
            result = {
                "v3": registered.v3,
                "v2": registered.v2,
            }
            if hasattr(registered, 'created'):
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
        Obtém registro consultando por v2, aop, doi, filename
        Cria / atualiza o registro
        Retorna dicionário cujas chaves são:
            input, found, saved, error, warning
        """
        result = {
            "input": {
                "v3": v3,
                "v2": v2,
                "aop": aop,
                "doi": doi,
                "filename": filename,
                "status": status,
            }
        }
        saved = None
        try:
            if not v2:
                raise ValueError("Manager.manage requires parameters: v2")
            with self.session_scope() as session:
                # obtém o registro
                registered = None
                if not registered:
                    registered = self._get_record(
                        session, v2, filename, doi, aop)
                if not registered:
                    registered = self._get_record_old(session, v2, aop)
                if registered:
                    result['registered'] = self._format_record(registered)

                # guarda o registro
                if len(filename) > 80:
                    result['warning'] = {"filename": filename}
                if registered:
                    if not hasattr(registered, 'created'):
                        # versão anterior do schema (v2, v3),
                        # então registrar no novo schema
                        saved = self._register(
                            session, v2, registered.v3, aop,
                            filename, doi, status)
                    else:
                        # já está registraddo no schema novo,
                        # então fazer atualização
                        saved = self._register(
                            session, v2, v3, aop,
                            filename, doi, status, registered)
                else:
                    saved = self._register(
                        session, v2,
                        self.get_unique_v3(session, v3, generate_v3),
                        aop, filename, doi, status)
                if saved:
                    result['saved'] = saved
        except RegistrationError as e:
            result['error'] = str(e)
        except Exception as e:
            result['error'] = str(e)
        finally:
            return result

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
        filename = filename[:80]
        prefix_v2 = v2 and v2[:-5] or ""
        prefix_aop = aop and aop[:-5] or ""
        if row:
            # update
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
                NewPidVersion.id == row.id).update(data)
            return data
        else:
            # create
            data = NewPidVersion(
                v2=v2,
                v3=v3,
                aop=aop or "",
                filename=filename or "",
                doi=doi or "",
                status=status or "",
                prefix_aop=prefix_aop,
                prefix_v2=prefix_v2,
            )
            session.add(data)
            return self._format_record(data)

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

    def _get_record(self, session, v2, filename, doi, aop):
        record = None
        if filename:
            if not record and doi:
                record = session.query(NewPidVersion).filter_by(
                    doi=doi, filename=filename
                ).first()
            if not record and aop:
                prefix = aop[:-5]
                record = (
                    session.query(NewPidVersion).filter_by(
                        prefix_v2=prefix, filename=filename
                    ).first() or
                    session.query(NewPidVersion).filter_by(
                        prefix_aop=prefix, filename=filename
                    ).first()
                )
            if not record and v2:
                prefix = v2[:-5]
                record = (
                    session.query(NewPidVersion).filter_by(
                        prefix_v2=prefix, filename=filename
                    ).first() or
                    session.query(NewPidVersion).filter_by(
                        prefix_aop=prefix, filename=filename
                    ).first()
                )
        if not record and doi:
            record = session.query(NewPidVersion).filter_by(doi=doi).first()
        if not record and aop:
            record = (
                session.query(NewPidVersion).filter_by(v2=aop).first() or
                session.query(NewPidVersion).filter_by(aop=aop).first()
            )
        if not record and v2:
            record = (
                session.query(NewPidVersion).filter_by(v2=v2).first() or
                session.query(NewPidVersion).filter_by(aop=v2).first()
            )
        return record

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
