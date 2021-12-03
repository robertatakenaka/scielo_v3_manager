from http import HTTPStatus

from scielo_v3_manager import (
    nw_db,
)
from scielo_v3_manager.utils.v3_gen import generates

from difflib import SequenceMatcher
from scielo_v3_manager import exceptions


MIN_EXPECTED_SIMILARITY_FOR_THE_FIRST = 0.9
MAX_EXPECTED_SIMILARITY_FOR_THE_SECOND = 0.6


def _evaluate_results_similarity(document_attributes, results):
    # IMPROVEME
    """
    Avalia os resultados de uma consulta na base de dados ao buscar documentos,
    comparando o documento buscado com os resultados da consulta e
    atribuindo uma taxa de similaridade a cada resultado.
    Retorna uma lista dos pares: taxa de similaridade e índice de `results`

    Returns
    -------
    list
    """
    _document_attributes = str(document_attributes)
    ranking = []
    for index, result in enumerate(results):
        ratio = SequenceMatcher(
            None, _document_attributes, str(result.to_compare)).ratio()
        ranking.append((ratio, index))
    return ranking


def _get_ranking_and_best_result(document_attributes, found_docs):
    # IMPROVEME
    """
    Avalia os rankings dos resultados da consulta à base de dados
    Verifica se o primeiro lugar tem
    pontuação > MIN_EXPECTED_SIMILARITY_FOR_THE_FIRST
    Verifica se o segundo lugar tem
    pontuação < MAX_EXPECTED_SIMILARITY_FOR_THE_SECOND

    Returns
    -------
    dict with keys: results, ranking, best result
    """
    response = {}
    if not found_docs:
        return response

    ranking = _evaluate_results_similarity(document_attributes, found_docs)
    ranking = sorted(ranking, reverse=True)

    ratio, index = ranking[0]
    if ratio > MIN_EXPECTED_SIMILARITY_FOR_THE_FIRST:
        second = 0 if len(ranking) == 1 else ranking[1][0]

        if second < MAX_EXPECTED_SIMILARITY_FOR_THE_SECOND:
            # obtém o melhor resultado
            response["best result"] = found_docs[index]

    response["ranking"] = ranking
    response["results"] = found_docs
    return response


def _add_status_code(response, code, msg):
    response["code"] = code
    response["msg"] = str(msg)
    return response


class PidV3Provider:

    def __init__(
            self,
            v3, id_provided_by_journal, v2, aop, other_pids,
            issns, pub_year,
            doi_with_lang,
            authors, collab, article_titles,
            volume, number, suppl, elocation, fpage, fpage_seq, lpage,
            epub_date, filenames,
            related_articles,
            ):
        self._document_attributes = nw_db.get_attributes(
            v3, id_provided_by_journal, v2, aop, other_pids,
            issns, pub_year,
            doi_with_lang,
            authors, collab, article_titles,
            volume, number, suppl, elocation, fpage, fpage_seq, lpage,
            epub_date, filenames,
            related_articles,
        )

    @property
    def document_attributes(self):
        return self._document_attributes

    @property
    def attributes_which_have_content(self):
        return {k: v for k, v in self.document_attributes.items() if v}

    def _execute_relax_match(self, method, query_params, qs_func):
        result = None
        arguments = nw_db._kwargs_to_args(self.attributes_which_have_content)
        args = nw_db._select_args(arguments, query_params)
        issns = nw_db._select_args(arguments, ['issns'])
        if args:
            qs = (qs_func(args)) & qs_func(issns)
            result = nw_db.fetch_documents(**{'qs': qs})
        return result

    @property
    def _search_by_ids(self):
        query_params = ['v2', 'v3', 'aop', 'other_pids', 'doi_with_lang', ]
        return nw_db.fetch_document_by_any_id(
            self.document_attributes.get("v2"),
            self.document_attributes.get("v3"),
            self.document_attributes.get("aop"),
            self.document_attributes.get("other_pids"),
            self.document_attributes.get("doi_with_lang"),
        )

    @property
    def _search_by_authors_and_titles_and_related_articles(self):
        query_params = [
            'authors', 'collab', 'article_titles', 'related_articles',
        ]
        return self._execute_relax_match(
            method="_search_by_authors_and_titles_and_related_articles",
            query_params=query_params,
            qs_func=nw_db._get_Qs_with_PIPE_operator,
        )

    def _evaluate_query_results(self, response, found_docs):
        """
        Avalia os rankings dos resultados da consulta à base de dados

        Returns
        -------
        dict with keys: results, ranking, best result, explain
        """
        if not found_docs:
            return response
        response["explain"] = found_docs.explain()
        response.update(
            _get_ranking_and_best_result(self.document_attributes, found_docs)
        )
        return response

    def registered_pid_v3(self, v3):
        # verifica se está registrado
        return nw_db.fetch_documents(v3=v3)

    @property
    def unregistered_pid_v3(self):
        # gera v3 enquanto v3 for um pid registrado
        while True:
            # gera v3
            v3 = generates()
            # verifica se está registrado
            doc = nw_db.fetch_documents(v3=v3)
            if not doc:
                # retorna v3
                return v3

    def search_document(self):
        """
        Obtém documento, se está registrado (tem o pid v3 registrado)

        Returns
        -------
        dict
            keys: results, explain, ranking, best result

        Raises
        ------
            exceptions.InsuficientArgumentsToSearchDocumentError
        """
        response = {}
        try:
            results = self._search_by_ids
        except exceptions.InsuficientArgumentsToSearchDocumentError:
            try:
                results = self._search_by_authors_and_titles_and_related_articles
            except exceptions.InsuficientArgumentsToSearchDocumentError:
                raise exceptions.InsuficientArgumentsToSearchDocumentError(
                    "Unable to search document because it was provided "
                    "insuficient data: no ID, no authors, "
                    "no article titles and no related articles"
                )
        return self._evaluate_query_results(response, results)

    def register_new_document(self):
        """
        Create a new record, if the document is considered new

        Returns
        -------
        dict
            keys: results, code, msg, explain, ranking, best result
            - code, msg
            code:
                - BAD_REQUEST
                - METHOD_NOT_ALLOWED
                - INTERNAL_SERVER_ERROR
                - CREATED
        """
        response = self._check_permission_to_register_new_document()
        if response.get("code"):
            return response

        # prepara os dados para gravar
        doc = nw_db.create_document()
        return self._save(doc, HTTPStatus.CREATED, "")

    def update_existing_document(self, _id):
        """
        Update record, if the document can be updated

        Returns
        -------
        dict
            keys: results, code, msg, explain, ranking, best result

            code:
                - BAD_REQUEST
                - METHOD_NOT_ALLOWED
                - INTERNAL_SERVER_ERROR
                - OK
        """
        response = self._check_permission_to_update_document(_id)
        if response.get("code"):
            return response

        # prepara os dados para gravar
        return self._save(response["best result"], HTTPStatus.OK, "updated")

    def _validate_document_v3(self):
        """
        Verifica se o documento contém v3 e se está registrado na base de dados
        """
        try:
            v3 = self._document_attributes["v3"]
            if not v3:
                raise ValueError
        except (KeyError, ValueError):
            raise exceptions.MissingRequiredV3Error(
                "To register a new document, it is required that document has "
                "pid v3. Provide a scielo pid v3. "
            )
        else:
            # garante que o pid v3 do documento não está registrado para outro
            if self.registered_pid_v3(v3):
                raise exceptions.V3IsAlreadyRegisteredError(
                    "To register a new document, "
                    "it is required a not registered pid v3. "
                    "Provide a new scielo pid v3. "
                )
            return True

    def _check_permission_to_register_new_document(self):
        """
        Verifica se documento tem permissão para ser registrado como novo

        - Verifica se documento contém v3
        - Verifica se documento contém dados suficientes
        para buscar documento na base e garantir que é inédito

        Returns
        -------
        dict
            keys: results, code, msg
            code values:
                - HTTPStatus.BAD_REQUEST
                - HTTPStatus.METHOD_NOT_ALLOWED
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
            return _add_status_code(
                response, HTTPStatus.METHOD_NOT_ALLOWED,
                "Not allowed to register document as new "
                "because it is already registered. "
            )
        return response

    def _check_permission_to_update_document(self, _id):
        """
        Verifica se documento tem permissão para ser atualizado

        - Verifica se documento contém v3
        - Verifica se documento contém dados suficientes
        para buscar documento na base e garantir que é inédito

        Returns
        -------
        dict
            keys: results, code, msg
            code values:
                - HTTPStatus.BAD_REQUEST
                - HTTPStatus.METHOD_NOT_ALLOWED
        """
        response = {}

        try:
            # verifica se o documento contém um v3 válido
            self._validate_document_v3()
        except exceptions.V3IsAlreadyRegisteredError:
            # ok, pois é uma atualização
            pass
        except exceptions.MissingRequiredV3Error as e:
            return _add_status_code(
                response, HTTPStatus.BAD_REQUEST,
                {"exception": type(e), "msg": str(e)}
            )

        # busca documento que será atualizado
        registered = nw_db.fetch_documents(_id=_id)

        if not registered:
            # documento inexistente, não é possível atualizar
            return _add_status_code(
                response, HTTPStatus.METHOD_NOT_ALLOWED,
                f"Not allowed to update document {_id} "
                "because it is not registered"
            )

        # atualiza response com o resultado e avaliação do resultado de busca
        self._evaluate_query_results(response, registered)

        if not response["best result"]:
            # não encontrou similaridade dos dados registrados vs daddos atuais
            return _add_status_code(
                response, HTTPStatus.FORBIDDEN,
                f"Not allowed to update document {_id} "
                "because it is not similar enough"
            )

    def _save(self, doc, http_status_for_success, msg):
        doc = nw_db.update_document_with_data(doc, self.document_attributes)
        try:
            # grava os dados
            doc = nw_db.save_document(doc)
        except exceptions.DBSaveNotUniqueError as e:
            # não gravou porque encontrou registros que cujos campos
            # devem ter valores únicos: v3 e v2
            response = _add_status_code(
                response, HTTPStatus.BAD_REQUEST,
                {"exception": type(e), "msg": str(e)}
            )
        except Exception as e:
            # não gravou porque encontrou uma exceção não esperada
            response = _add_status_code(
                response, HTTPStatus.INTERNAL_SERVER_ERROR,
                {"exception": type(e), "msg": str(e)}
            )
        else:
            # obteve sucesso ao gravar
            response = _add_status_code(response, http_status_for_success, msg)
            response["document"] = doc
        return response
