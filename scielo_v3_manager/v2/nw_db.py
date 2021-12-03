from datetime import datetime
from mongoengine import Q
from mongoengine.document import NotUniqueError

from scielo_v3_manager import exceptions
from scielo_v3_manager.models import (
    db_connect_by_uri,
    get_attributes,
    PidManagerDocument,
    MULTI_VALUE_ATTRIBS,
    COMPOSE_VALUE_ATTRIBS,
)


def mk_connection(host):
    try:
        db_connect_by_uri(host=host)
    except Exception as e:
        raise exceptions.DBConnectError(
            {"exception": type(e), "msg": str(e)}
        )


def create_document():
    try:
        return PidManagerDocument()
    except Exception as e:
        raise exceptions.DBCreateDocumentError(
            {"exception": type(e), "msg": str(e)}
        )


def save_document(document):
    if not hasattr(document, 'created'):
        document.created = None
    try:
        document.updated = datetime.utcnow()
        if not document.created:
            document.created = document.updated
        document.save()
    except NotUniqueError as e:
        raise exceptions.DBSaveNotUniqueError(e)
    else:
        return document


def update_document_with_data(doc, data):
    # doc._id
    for attr_name, values in data.items():
        _values = values or ''
        if attr_name in COMPOSE_VALUE_ATTRIBS.keys():
            _values = []
            for attr, val in values.items():
                obj = COMPOSE_VALUE_ATTRIBS[attr_name]()
                setattr(obj, attr, val)
                _values.append(obj)
        setattr(doc, attr_name, _values)
    return doc


def _select_args(arguments, selection):
    args = []
    for k in selection:
        try:
            args.extend(arguments[k])
        except KeyError:
            continue
    return args


def _kwargs_to_args(**kwargs):
    args = {}
    for k, v in kwargs.items():
        if k in MULTI_VALUE_ATTRIBS:
            args[k] = [
                {k: item} for item in v
            ]
        else:
            args[k] = [{k: v}]
    return args


def _get_Qs_with_PIPE_operator(arguments):
    """
    Obtém QuerySet
    """
    Qs = None
    for _kwargs in arguments:
        if Qs:
            Qs |= Q(**_kwargs)
        else:
            Qs = Q(**_kwargs)
    return Qs


def _get_Qs_with_AND_operator(arguments):
    """
    Obtém QuerySet
    """
    Qs = None
    for _kwargs in arguments:
        if Qs:
            Qs &= Q(**_kwargs)
        else:
            Qs = Q(**_kwargs)
    return Qs


def fetch_documents(**kwargs):

    order_by = kwargs.get("order_by") or '-updated'
    items_per_page = kwargs.get("items_per_page") or 50
    page = kwargs.get("page") or 1

    skip = ((page - 1) * items_per_page)
    limit = items_per_page

    if kwargs.get("qs"):
        return PidManagerDocument.objects(
            kwargs.get("qs")).order_by(order_by).skip(skip).limit(limit)
    return PidManagerDocument.objects(
        **kwargs).order_by(order_by).skip(skip).limit(limit)


def fetch_document_by_v3(v3):
    objects = fetch_documents(v3=v3)
    if not objects:
        raise exceptions.DocumentDoesNotExistError(f"{v3} does not exist")
    return objects


def fetch_document_by_any_id(v2, v3, aop_pid, other_pids, doi_with_lang):
    if not any([v2, v3, aop_pid, other_pids, doi_with_lang]):
        raise excpetions.InsuficientArgumentsToSearchDocumentError(
            "scielo_v3_manager.db.fetch_document_by_any_id requires "
            "at least one argument: v2, v3, aop_pid, other_pids, doi_with_lang"
        )
    args = [
        dict(v3=v3),
        dict(v2=v2),
        dict(v2=aop_pid),
        dict(aop=aop_pid),
        dict(aop=v2),
        dict(other_pids=v2),
        dict(other_pids=v3),
        dict(other_pids=aop_pid),
        dict(doi_with_lang=doi_with_lang),
    ]
    qs = None
    for kwargs in args:
        if not kwargs.values():
            continue
        if qs:
            qs |= Q(**kwargs)
        else:
            qs = Q(**kwargs)

    return PidManagerDocument.objects(qs)


def fetch_documents_by_issue(issns, pub_year, volume, number, suppl):
    return fetch_documents(
        issns=issns,
        pub_year=pub_year,
        volume=volume or '',
        number=number or '',
        suppl=suppl or '',
    )


def fetch_documents_by_person_citation(issns, pub_year, surname, given_names, orcid):
    found = None
    if not found and surname and given_names and orcid:
        found = fetch_documents(
            issns=issns,
            pub_year=pub_year,
            authors__surname=surname,
            authors__given_names=given_names,
            authors__orcid=orcid,
        )
    if not found and surname and given_names:
        found = fetch_documents(
            issns=issns,
            pub_year=pub_year,
            authors__surname=surname,
            authors__given_names=given_names,
        )
    if not found and surname:
        found = fetch_documents(
            issns=issns,
            pub_year=pub_year,
            authors__surname=surname,
        )
    if found:
        return found


def fetch_documents_by_institution_citation(issns, pub_year, collab):
    return fetch_documents(
        issns=issns,
        pub_year=pub_year,
        collab=collab,
    )


def fetch_documents_by_filename(issns, pub_year, filename):
    return fetch_documents(
        issns=issns,
        pub_year=pub_year,
        filename=filename,
    )
