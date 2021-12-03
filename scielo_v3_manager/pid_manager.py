from http import HTTPStatus

from scielo_v3_manager.v1 import local_models
from scielo_v3_manager.v2 import nw_pid_manager


class Manager:

    def __init__(self, **kwargs):
        pid_manager = kwargs.get("pid_manager")
        if pid_manager:
            self._pid_manager = pid_manager
            return

        name = kwargs.get("name")
        if name:
            timeout = kwargs.get("timeout")
            _engine_args = kwargs.get("_engine_args")
            self._pid_manager = local_models.Manager(
                name, timeout, _engine_args)
            return

        raise ValueError(
            "scielo_v3_manager.pid_manager.Manager requires `pid_manager`")

    def manage(self, v2, v3, aop, filename, doi, status, generate_v3):
        """
        Usa pid_manager_local
        """
        return self._pid_manager.manage(
            v2, v3, aop, filename, doi, status, generate_v3)

    def get_pid_v3_provider(
            self, v3, id_provided_by_journal, v2, aop, other_pids,
            issns, pub_year,
            doi_with_lang,
            authors, collab, article_titles,
            volume, number, suppl, elocation, fpage, fpage_seq, lpage,
            epub_date, filenames,
            related_articles,
            ):
        return nw_pid_manager.PidV3Provider(
            v3, id_provided_by_journal, v2, aop, other_pids,
            issns, pub_year,
            doi_with_lang,
            authors, collab, article_titles,
            volume, number, suppl, elocation, fpage, fpage_seq, lpage,
            epub_date, filenames,
            related_articles,
        )

    def check_document(self, pid_v3_provider):
        try:
            response = pid_v3_provider.fetch_document()
        except nw_pid_manager.nw_db.MissingArgumentsError as e:
            response = {}
            return nw_pid_manager._add_status_code(
                response, HTTPStatus.BAD_REQUEST, e)

        # avalia resposta
        if not response.get("results"):
            # documento não registrado
            return nw_pid_manager._add_status_code(
                response, HTTPStatus.NOT_FOUND, "Document is not registered")

        if response.get("best result") and len(response["results"]) == 1:
            # encontrou um documento e é bem similar
            response["document"] = response["best result"]
            return nw_pid_manager._add_status_code(
                response, HTTPStatus.OK, "Document is registered")

        # encontrou mais de um resultado
        msg = (
            "Document is similar to more than one document"
        )
        # consulte outros
        return nw_pid_manager._add_status_code(
                response, HTTPStatus.SEE_OTHER, msg)

    def register_new_document(self, pid_v3_provider):
        return pid_v3_provider.register_new_document()

    def update_existing_document(self, pid_v3_provider, _id):
        return pid_v3_provider.update_existing_document(_id)


"""
    def _check_permission_to_register_new_document(self):
        """
        Verifica permissão para registrar novo documento

        - Verifica se foi fornecido um valor de v3
        - Verifica se contém dados suficientes para buscar documento para
        garantir que é inédito

"""
response = {}

try:
    # verifica se o documento contém um v3 válido
    self._validate_document_v3()

    # verifica se o documento já está registrado ou não
    response = self.search_document()
except (
        exceptions.InsuficientArgumentsToSearchDocumentError,
        exceptions.MissingRequiredV3Error,
        exceptions.V3IsAlreadyRegisteredError,
        ) as e:
    return _add_status_code(
        response, HTTPStatus.BAD_REQUEST,
        {"exception": type(e), "msg": str(e)}
    )

# verifica se é permitido registrar documento como novo
if response["results"]:
    msg = (
        "Not allowed to register document as new document. "
        "Document is already registered"
    )
    return _add_status_code(
        response, HTTPStatus.METHOD_NOT_ALLOWED, msg)
"""