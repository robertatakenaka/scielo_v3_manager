from datetime import datetime

from tenacity import (
    retry,
    wait_exponential,
    stop_after_attempt,
)
from mongoengine import (
    EmbeddedDocument,
    EmbeddedDocumentField,
    EmbeddedDocumentListField,
    Document,
    StringField,
    DictField,
    ListField,
    DateField,
    DateTimeField,
    connect,
)


MULTI_VALUE_ATTRIBS = [
    'issns', 'authors', 'article_titles', 'filenames', 'other_pids',
    'doi_with_lang',
]
COMPOSE_VALUE_ATTRIBS = dict(
    issns=ISSN,
    doi_with_lang=DOI,
    authors=Author,
    article_titles=TextAndLang,
    related_articles=RelatedArticle,
)


def get_attributes(
        v3, id_provided_by_journal, v2, aop, other_pids,
        issns, pub_year,
        doi_with_lang,
        authors, collab, article_titles,
        volume, number, suppl, elocation, fpage, fpage_seq, lpage,
        epub_date, filenames,
        ):
    """
    Retorna um dicionário com os atributos do documento
    """
    return {
        "v3": v3 or '',
        "id_provided_by_journal": id_provided_by_journal or '',
        "v2": v2 or '',
        "aop": aop or '',
        "other_pids": other_pids or '',
        "issns": issns or '',
        "pub_year": pub_year or '',
        "doi_with_lang": doi_with_lang or '',
        "authors": authors or '',
        "collab": collab or '',
        "article_titles": article_titles or '',
        "volume": volume or '',
        "number": number or '',
        "suppl": suppl or '',
        "elocation": elocation or '',
        "fpage": fpage or '',
        "fpage_seq": fpage_seq or '',
        "lpage": lpage or '',
        "epub_date": epub_date or '',
        "filenames": filenames or '',
    }


@retry(wait=wait_exponential(), stop=stop_after_attempt(10))
def db_connect_by_uri(uri):
    """
    mongodb://{login}:{password}@{host}:{port}/{database}
    """
    conn = connect(host=uri)
    print("%s connected" % uri)
    return conn


@retry(wait=wait_exponential(), stop=stop_after_attempt(10))
def db_connect(host, port, schema, login, password, **extra_dejson):
    uri = "mongodb://{creds}{host}{port}/{database}".format(
        creds="{}:{}@".format(login, password) if login else "",
        host=host,
        port="" if port is None else ":{}".format(port),
        database=schema,
    )

    return connect(host=uri, **extra_dejson)


class TextAndLang(EmbeddedDocument):
    lang = StringField()
    text = StringField()

    def __unicode__(self):
        return {"lang": self.lang, "text": self.text}


DOI_CREATION_STATUS = ('auto_assigned', 'assigned_by_editor', 'UNK')
DOI_REGISTRATION_STATUS = ('registered', 'not_registered', 'UNK')


class DOI(EmbeddedDocument):
    lang = StringField()
    value = StringField()
    creation_status = StringField(choices=DOI_CREATION_STATUS)
    registration_status = StringField(choices=DOI_REGISTRATION_STATUS)

    def __unicode__(self):
        return {
            "lang": self.lang, "value": self.value,
            "creation_status": self.creation_status,
            "registration_status": self.registration_status,
        }


ISSN_TYPES = ('epub', 'ppub', 'l', 'id')


class ISSN(EmbeddedDocument):
    value = StringField()
    type = StringField(choices=ISSN_TYPES)

    def __unicode__(self):
        return {
            "value": self.value,
            "type": self.type,
        }


class Author(EmbeddedDocument):
    surname = StringField()
    given_names = StringField()
    orcid = StringField()

    def __unicode__(self):
        return (
            '%s' %
            {"surname": self.surname, "given_names": self.given_names,
             "orcid": self.orcid, })


class RelatedArticle(EmbeddedDocument):
    """
    Model responsible for relationship between articles.
    Attributes:
    ref_id: String content any reference Id to the article pid_v1, pid_v2 or pid_v3
    doi: String content the Crossref Id, if the article dont have DOI use the
    ``ref_id``
    related_type: String with a category of relation.
    Example of this model:
        "related_articles" : [
            {
                "ref_id": "9LzVjQrYQF7BvkYWnJw9sDy",
                "doi" : "10.1590/S0103-50532006000200015",
                "related_type" : "corrected-article"
            },
            {
                "ref_id": "3LzVjQrOIEJYUSvkYWnJwsDy",
                "doi" : "10.1590/S0103-5053200600020098983",
                "related_type" : "addendum"
            },
            {
                "ref_id": "6LzVjQrKOIJAKSJUIOAKKODy",
                "doi" : "10.1590/S0103-50532006000200015",
                "related_type" : "retraction"
            },
        ]
    """
    ref_id = StringField()
    doi = StringField()
    related_type = StringField()

    def __unicode__(self):
        return {
            "ref_id": self.ref_id,
            "doi": self.doi,
            "related_type": self.related_type,
        }


class PidManagerDocument(Document):
    _id = StringField(max_length=32, primary_key=True, required=True)
    v3 = StringField(max_length=23, unique=True, required=True)

    # outros tipos de ID
    id_provided_by_the_journal = StringField()
    v2 = StringField(max_length=23, unique=True, required=True)
    aop = StringField(max_length=23, required=False)
    other_pids = ListField(field=StringField())

    # dados que identificam o documento e que sempre estão presentes
    issns = EmbeddedDocumentListField(ISSN)
    pub_year = StringField()

    # dados que identificam o documento e não são obrigatórios
    doi_with_lang = EmbeddedDocumentListField(DOI)
    authors = EmbeddedDocumentListField(Author)
    collab = StringField()
    article_titles = EmbeddedDocumentListField(TextAndLang)

    # dados complementares que identificam o documento
    volume = StringField()
    number = StringField()
    suppl = StringField()
    elocation = StringField()
    fpage = StringField()
    fpage_seq = StringField()
    lpage = StringField()

    # related
    related_articles = EmbeddedDocumentListField(RelatedArticle)

    # dados de processamento / procedimentos
    epub_date = DateField()
    filenames = ListField(field=StringField())
    isis_updated = StringField()
    extra_info = ListField(field=DictField())

    # datas deste registro
    created = DateTimeField()
    updated = DateTimeField()

    meta = {
        'collection': 'pid_manager',
        'indexes': [
            'v3',
            'v2',
            'aop',
            'id_provided_by_the_journal',
            'other_pids',
            'issns',
            'doi_with_lang',
            'filenames',
            'pub_year',
            'authors',
            'volume',
            'number',
            'suppl',
            'elocation',
            'fpage',
            'fpage_seq',
            'lpage',
        ]
    }

    def __unicode__(self):
        return str(
            get_attributes(
                self.v3, self.id_provided_by_journal,
                self.v2, self.aop, self.other_pids,
                self.issns, self.pub_year,
                self.doi_with_lang,
                self.authors, self.collab, self.article_titles,
                self.volume, self.number, self.suppl,
                self.elocation, self.fpage, self.fpage_seq, self.lpage,
                self.epub_date, self.filenames,
                self.related_articles,
            )
        )

    @property
    def to_compare(self):
        return get_attributes(
            self.v3, self.id_provided_by_journal,
            self.v2, self.aop, self.other_pids,
            self.issns, self.pub_year,
            self.doi_with_lang,
            self.authors, self.collab, self.article_titles,
            self.volume, self.number, self.suppl,
            self.elocation, self.fpage, self.fpage_seq, self.lpage,
            self.epub_date, self.filenames,
            self.related_articles,
        )

    def save(self, *args, **kwargs):
        if not self.created:
            self.created = datetime.utcnow().isoformat().replace("T", " ")
        self.updated = datetime.utcnow().isoformat().replace("T", " ")

        return super(PidManagerDocument, self).save(*args, **kwargs)


class PreviousPidManagerDocument(Document):
    _id = StringField(max_length=32, primary_key=True, required=True)
    v3 = StringField(max_length=23, unique=False, required=True)

    # outros tipos de ID
    id_provided_by_the_journal = StringField()
    v2 = StringField(max_length=23, unique=False, required=True)
    aop = StringField(max_length=23, required=False)
    other_pids = ListField(field=StringField())

    # dados que identificam o documento e que sempre estão presentes
    issns = EmbeddedDocumentListField(ISSN)
    pub_year = StringField()

    # dados que identificam o documento e não são obrigatórios
    doi_with_lang = EmbeddedDocumentListField(DOI)
    authors = EmbeddedDocumentListField(Author)
    collab = StringField()
    article_titles = EmbeddedDocumentListField(TextAndLang)

    # dados complementares que identificam o documento
    volume = StringField()
    number = StringField()
    suppl = StringField()
    elocation = StringField()
    fpage = StringField()
    fpage_seq = StringField()
    lpage = StringField()

    # related
    related_articles = EmbeddedDocumentListField(RelatedArticle)

    # dados de processamento / procedimentos
    epub_date = DateField()
    filenames = ListField(field=StringField())
    isis_updated = StringField()
    extra_info = ListField(field=DictField())

    # datas deste registro
    created = DateTimeField()
    updated = DateTimeField()

    meta = {
        'collection': 'pid_manager',
        'indexes': [
            'v3',
            'v2',
            'aop',
            'id_provided_by_the_journal',
            'other_pids',
            'issns',
            'doi_with_lang',
            'filenames',
            'pub_year',
            'authors',
            'volume',
            'number',
            'suppl',
            'elocation',
            'fpage',
            'fpage_seq',
            'lpage',
        ]
    }

    def __unicode__(self):
        return str(
            get_attributes(
                self.v3, self.id_provided_by_journal,
                self.v2, self.aop, self.other_pids,
                self.issns, self.pub_year,
                self.doi_with_lang,
                self.authors, self.collab, self.article_titles,
                self.volume, self.number, self.suppl,
                self.elocation, self.fpage, self.fpage_seq, self.lpage,
                self.epub_date, self.filenames,
                self.related_articles,
            )
        )

    @property
    def to_compare(self):
        return get_attributes(
            self.v3, self.id_provided_by_journal,
            self.v2, self.aop, self.other_pids,
            self.issns, self.pub_year,
            self.doi_with_lang,
            self.authors, self.collab, self.article_titles,
            self.volume, self.number, self.suppl,
            self.elocation, self.fpage, self.fpage_seq, self.lpage,
            self.epub_date, self.filenames,
            self.related_articles,
        )

    def save(self, *args, **kwargs):
        if not self.created:
            self.created = datetime.utcnow().isoformat().replace("T", " ")
        self.updated = datetime.utcnow().isoformat().replace("T", " ")

        return super(PidManagerDocument, self).save(*args, **kwargs)

