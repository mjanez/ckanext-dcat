"""
This file contains default values for the SpanishDCATProfile and EuropeanDCATAPProfile profiles.
Default values are used to fill in missing metadata in resources.
Default values can be overridden in resource-specific metadata.

metadata_field_names: dict
    A dictionary containing the metadata field names for each profile.

spain_dcat_default_values: dict
    A dictionary containing the default values for the SpanishDCATProfile.

euro_dcat_ap_default_values: dict
    A dictionary containing the default values for the EuropeanDCATAPProfile.
"""

# DCAT default elements by profile
metadata_field_names = {
    'euro_dcat_ap': {
        'theme': 'theme_eu',
    },
    'spain_dcat': {
        'theme': 'theme_es',
    }
}

# SpanishDCATProfile: Mandatory elements by NTI-RISP.
spain_dcat_default_values = {
    'access_rights': 'http://publications.europa.eu/resource/authority/access-right/PUBLIC',
    'conformance_es': 'http://www.boe.es/eli/es/res/2013/02/19/(4)',
    'description': 'Recurso sin descripción.',
    'description_es': 'Recurso sin descripción.',
    'format_es': 'HTML',
    'language_code': 'es',
    'license': 'http://www.opendefinition.org/licenses/cc-by',
    'license_url': 'http://www.opendefinition.org/licenses/cc-by',
    'mimetype_es': 'text/html',
    'notes': 'Conjunto de datos sin descripción.',
    'notes_es': 'Conjunto de datos sin descripción.',
    'notes_en': 'Dataset without description.',
    'publisher_identifier': 'http://datos.gob.es/recurso/sector-publico/org/Organismo/EA0007777',
    'theme_es': 'http://datos.gob.es/kos/sector-publico/sector/sector-publico',
    'theme_eu': 'http://publications.europa.eu/resource/authority/data-theme/GOVE',
    'theme_taxonomy': 'http://datos.gob.es/kos/sector-publico/sector/',
    'spatial_uri': 'http://datos.gob.es/recurso/sector-publico/territorio/Pais/España',
}

# EuropeanDCATAPProfile: Mandatory catalog elements by DCAT-AP.
euro_dcat_ap_default_values = {
    'conformance': 'http://data.europa.eu/930/',
    'description': 'Resource without description.',
    'description_en': 'Resource without description.',
    'description_es': 'Recurso sin descripción.',
    'license': 'http://www.opendefinition.org/licenses/cc-by',
    'license_url': 'http://www.opendefinition.org/licenses/cc-by',
    'notes': 'Dataset without description.',
    'notes_es': 'Conjunto de datos sin descripción.',
    'notes_en': 'Dataset without description.',
    'publisher_identifier': 'http://datos.gob.es/recurso/sector-publico/org/Organismo/EA0007777',
    'theme_es': 'http://datos.gob.es/kos/sector-publico/sector/sector-publico',
    'theme_eu': 'http://publications.europa.eu/resource/authority/data-theme/GOVE',
    'theme_taxonomy': 'http://publications.europa.eu/resource/authority/data-theme',
    'spatial_uri': 'http://publications.europa.eu/resource/authority/country/ESP',
}