import json

from rdflib import URIRef, Literal
from ckantoolkit import config, asbool, get_action

from ckanext.dcat.utils import (
    resource_uri,
)
from ..base import RDFProfile, URIRefOrLiteral, CleanedURIRef
from ..base import (
    RDF,
    RDFS,
    DCAT,
    DCT,
    FOAF,
    SCHEMA,
    CNT,
    DCATAP,
    DC,
    namespaces,
    MD_ES_FORMATS,
    DISTRIBUTION_LICENSE_FALLBACK_CONFIG
)

from ..euro_dcat_ap_2 import EuropeanDCATAP2Profile

# Default values
from ckanext.dcat.profiles.default_config import default_translated_fields_spain_dcat, default_translated_fields, spain_dcat_default_values

#TODO: SpanishDCATAP2Profile
class SpanishDCATAP2Profile(EuropeanDCATAP2Profile):
    '''
    An RDF profile based on the DCAT NTI-RISP profile for data portals in Spain

    Default values for some fields:
    
    ckanext-dcat/ckanext/dcat/.profiles.default_config.py

    More information and specification:

    https://datos.gob.es/es/documentacion/guia-de-aplicacion-de-la-norma-tecnica-de-interoperabilidad-de-reutilizacion-de
    
    https://datos.gob.es/es/documentacion/guias-de-datosgobes
    
    https://datos.gob.es/es/documentacion/norma-tecnica-de-interoperabilidad-de-reutilizacion-de-recursos-de-informacion

    '''