import json

from rdflib import URIRef, Literal
from ckantoolkit import config, asbool, get_action

from ckanext.dcat.utils import (
    resource_uri,
)
from .base import RDFProfile, URIRefOrLiteral, CleanedURIRef
from .base import (
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

from .euro_dcat_ap import EuropeanDCATAPProfile

# Default values
from ckanext.dcat.profiles.default_config import default_translated_fields_spain_dcat, default_translated_fields, spain_dcat_default_values


class SpanishDCATProfile(EuropeanDCATAPProfile):
    '''
    An RDF profile based on the DCAT NTI-RISP profile for data portals in Spain

    Default values for some fields:
    
    ckanext-dcat/ckanext/dcat/.profiles.default_config.py

    More information and specification:

    https://datos.gob.es/es/documentacion/guia-de-aplicacion-de-la-norma-tecnica-de-interoperabilidad-de-reutilizacion-de
    
    https://datos.gob.es/es/documentacion/guias-de-datosgobes
    
    https://datos.gob.es/es/documentacion/norma-tecnica-de-interoperabilidad-de-reutilizacion-de-recursos-de-informacion

    '''
    def parse_dataset(self, dataset_dict, dataset_ref):
        """
        Parses a CKAN dataset dictionary and generates an RDF graph.

        Args:
            dataset_dict (dict): The dictionary containing the dataset metadata.
            dataset_ref (URIRef): The URI of the dataset in the RDF graph.

        Returns:
            dict: The updated dataset dictionary with the RDF metadata.
        """
        # call super method
        super(SpanishDCATProfile, self).parse_dataset(dataset_dict, dataset_ref)

        # Lists
        for key, predicate in (
            ('theme_es', DCAT.theme),
            ('temporal_resolution', DCAT.temporalResolution),
            ('is_referenced_by', DCT.isReferencedBy),
        ):
            values = self._object_value_list(dataset_ref, predicate)
            if values:
                dataset_dict['extras'].append({'key': key,
                                               'value': json.dumps(values)})
        # Temporal
        start, end = self._time_interval(dataset_ref, DCT.temporal, dcat_ap_version=2)
        if start:
            self._insert_or_update_temporal(dataset_dict, 'temporal_start', start)
        if end:
            self._insert_or_update_temporal(dataset_dict, 'temporal_end', end)

        # Spatial
        spatial = self._spatial(dataset_ref, DCT.spatial)
        for key in ('bbox', 'centroid'):
            self._add_spatial_to_dict(dataset_dict, key, spatial)

        # Resources
        for distribution in self._distributions(dataset_ref):
            distribution_ref = str(distribution)
            for resource_dict in dataset_dict.get('resources', []):
                # Match distribution in graph and distribution in resource dict
                if resource_dict and distribution_ref == resource_dict.get('distribution_ref'):
                    #  Simple values
                    for key, predicate in (
                            ('availability', DCATAP.availability),
                            ('compress_format', DCAT.compressFormat),
                            ('package_format', DCAT.packageFormat),
                            ):
                        value = self._object_value(distribution, predicate)
                        if value:
                            resource_dict[key] = value

                    # Access services
                        access_service_list = []

                        for access_service in self.g.objects(distribution, DCAT.accessService):
                            access_service_dict = {}

                            #  Simple values
                            for key, predicate in (
                                    ('availability', DCATAP.availability),
                                    ('title', DCT.title),
                                    ('endpoint_description', DCAT.endpointDescription),
                                    ('license', DCT.license),
                                    ('access_rights', DCT.accessRights),
                                    ('description', DCT.description),
                                    ):
                                value = self._object_value(access_service, predicate)
                                if value:
                                    access_service_dict[key] = value
                            #  List
                            for key, predicate in (
                                    ('endpoint_url', DCAT.endpointURL),
                                    ('serves_dataset', DCAT.servesDataset),
                                    ('resource_relation', DCT.relation),
                                    ):
                                values = self._object_value_list(access_service, predicate)
                                if values:
                                    access_service_dict[key] = values

                            # Access service URI (explicitly show the missing ones)
                            access_service_dict['uri'] = (str(access_service)
                                    if isinstance(access_service, URIRef)
                                    else '')

                            # Remember the (internal) access service reference for referencing in
                            # further profiles, e.g. for adding more properties
                            access_service_dict['access_service_ref'] = str(access_service)

                            access_service_list.append(access_service_dict)

                        if access_service_list:
                            resource_dict['access_services'] = json.dumps(access_service_list)

        return dataset_dict

    def graph_from_dataset(self, dataset_dict, dataset_ref):
        """
        Generates an RDF graph from a dataset dictionary.

        Args:
            dataset_dict (dict): The dictionary containing the dataset metadata.
            dataset_ref (URIRef): The URI of the dataset in the RDF graph.

        Returns:
            None
        """
        
        # Namespaces
        self._bind_namespaces()
        
        g = self.g

        for prefix, namespace in namespaces.items():
            g.bind(prefix, namespace)

        g.add((dataset_ref, RDF.type, DCAT.Dataset))

        # Translated fields
        items = [(
            default_translated_fields[key]['field_name'],
            default_translated_fields[key]['rdf_predicate'],
            default_translated_fields[key]['fallbacks'],
            default_translated_fields[key]['_type'],
            default_translated_fields[key]['required_lang']
            )
            for key in default_translated_fields_spain_dcat
        ]
        self._add_triples_from_dict(dataset_dict, dataset_ref, items, multilang=True)


        # Basic elements (Title, description, Dates)
        basic_elements = [
            ('url', DCAT.landingPage, None, URIRef),
        ]
        
        dates = [
            ('created', DCT.created, ['metadata_created'], Literal),
            ('issued', DCT.issued, ['metadata_created'], Literal),
            ('modified', DCT.modified, ['metadata_modified'], Literal),
            ('valid', DCT.valid, None, Literal),
        ]
        
        self._add_date_triples_from_dict(dataset_dict, dataset_ref, dates)
        self._add_list_triples_from_dict(dataset_dict, dataset_ref, basic_elements)
        
        # NTI-RISP Core elements
        items = {
            'notes': (DCT.description, spain_dcat_default_values['notes']),
            'conforms_to_es': (DCT.conformsTo, spain_dcat_default_values['conformance_es']),
            'identifier_uri': (DCT.identifier, dataset_ref),
            'license_url': (DCT.license, spain_dcat_default_values['license_url']),
            'language_code': (DC.language, spain_dcat_default_values['language_code'] or config.get('ckan.locale_default')),
            'spatial_uri': (DCT.spatial, spain_dcat_default_values['spatial_uri']),
            'publisher_identifier_es': (DCT.publisher,  spain_dcat_default_values['publisher_identifier'] or dataset_dict['publisher_identifier']),
        }

        self._add_dataset_triples_from_dict(dataset_dict, dataset_ref, items)      

        # Lists
        dataset_dict['tags'] = [tag['name'].replace(" ", "").lower() for tag in dataset_dict.get('tags', [])]

        # Lists NTI-RISP Core
        items_list = [
            ('reference', DCT.references, None, URIRefOrLiteral),
            ('tags', DCAT.keyword, None, Literal),
            ('theme_es', DCAT.theme, spain_dcat_default_values['theme_es'], URIRefOrLiteral),
        ]

        #Lists DCAT-AP Extension
        items_dcatap_list = [
            ('conforms_to', DCT.conformsTo, None, URIRef),
            ('metadata_profile', DCT.conformsTo, None, URIRef),
        ]

        items_list.extend(items_dcatap_list)
        self._add_list_triples_from_dict(dataset_dict, dataset_ref, items_list)
         
        #FIXME Frequency - Frecuencia ('frequency', DCT.accrualPeriodicity, None, URIRefOrLiteral): https://github.com/ctt-gob-es/datos.gob.es/blob/30c4a0d97356e0caf948aff2bb74790f4885c67f/ckan/ckanext-dge-harvest/ckanext/dge_harvest/profiles.py#L2508

        # Temporal - Cobertura temporal
        self._temporal_graph(dataset_dict, dataset_ref)
    
        # Use fallback license if set in config
        resource_license_fallback = None
        if asbool(config.get(DISTRIBUTION_LICENSE_FALLBACK_CONFIG, False)):
            if 'license_id' in dataset_dict and isinstance(URIRefOrLiteral(dataset_dict['license_id']), URIRef):
                resource_license_fallback = dataset_dict['license_id']
            elif 'license_url' in dataset_dict and isinstance(URIRefOrLiteral(dataset_dict['license_url']), URIRef):
                resource_license_fallback = dataset_dict['license_url']
        
        # Resources
        for resource_dict in dataset_dict.get('resources', []):

            distribution = CleanedURIRef(resource_uri(resource_dict))

            g.add((dataset_ref, DCAT.distribution, distribution))

            g.add((distribution, RDF.type, DCAT.Distribution))
            
            # NTI-RISP Core elements
            items = {
                'url': (DCAT.accessURL, None),
                'name': (DCT.title, None),
                'description': (DCT.description, spain_dcat_default_values['description']),
                'language_code': (DC.language, spain_dcat_default_values['language_code'] or config.get('ckan.locale_default')),
                'license': (DCT.license, spain_dcat_default_values['license']),
                'identifier_uri': (DCT.identifier, distribution),
                'size': (DCT.byteSize, None),
            }

            self._add_dataset_triples_from_dict(resource_dict, distribution, items)   

            # Lists
            items_lists = [
                ('resource_relation', DCT.relation, None, URIRef),
            ]

            self._add_list_triples_from_dict(resource_dict, distribution, items_lists)

            # Format/Mimetype - Formato de la distribución
            self._distribution_format(resource_dict, distribution)
    
    def graph_from_catalog(self, catalog_dict, catalog_ref):
        """
        Adds the metadata of a CKAN catalog to the RDF graph.

        Args:
            catalog_dict (dict): A dictionary containing the metadata of the catalog.
            catalog_ref (URIRef): The URI of the catalog in the RDF graph.

        Returns:
            None
        """
        g = self.g

        for prefix, namespace in namespaces.items():
            g.bind(prefix, namespace)

        g.add((catalog_ref, RDF.type, DCAT.Catalog))
        
        # Basic fields
        license, publisher_identifier, access_rights, spatial_uri, language_code = [
            self._get_catalog_field(field_name='license_url', fallback='license_id', default_values_dict=spain_dcat_default_values),
            spain_dcat_default_values['publisher_identifier'] or self._get_catalog_field(field_name='publisher_identifier', default_values_dict=spain_dcat_default_values),
            self._get_catalog_field(field_name='access_rights', default_values_dict=spain_dcat_default_values),
            self._get_catalog_field(field_name='spatial_uri', default_values_dict=spain_dcat_default_values),
            spain_dcat_default_values['language_code'] or config.get('ckan.locale_default')
            ]

        # Mandatory elements by NTI-RISP (datos.gob.es)
        items_core = [
            ('title', DCT.title, config.get('ckan.site_title'), Literal),
            ('description', DCT.description, config.get('ckan.site_description'), Literal),
            ('publisher_identifier', DCT.publisher, publisher_identifier, URIRef),
            ('identifier', DCT.identifier, f'{config.get("ckan_url")}/catalog.rdf', Literal),
            ('encoding', CNT.characterEncoding, 'UTF-8', Literal),
            ('language_code', DC.language, language_code, URIRefOrLiteral),
            ('spatial_uri', DCT.spatial, spatial_uri, URIRefOrLiteral),
            ('theme_taxonomy', DCAT.themeTaxonomy, spain_dcat_default_values['theme_taxonomy'], URIRef),
            ('homepage', FOAF.homepage, config.get('ckan_url'), URIRef),
        ]
        
        # DCAT-AP extension
        items_dcatap = [
            ('license', DCT.license, license, URIRef),
            ('conforms_to', DCT.conformsTo, spain_dcat_default_values['conformance_es'], URIRef),
            ('access_rights', DCT.accessRights, access_rights, URIRefOrLiteral),
        ]
         
        items_core.extend(items_dcatap)
         
        for item in items_core:
            key, predicate, fallback, _type = item
            value = catalog_dict.get(key, fallback) if catalog_dict else fallback
            if value:
                g.add((catalog_ref, predicate, _type(value)))

        #TODO: Tamaño del catálogo - dct:extent

        # Dates
        modified = self._get_catalog_field(field_name='metadata_modified', default_values_dict=spain_dcat_default_values)
        issued = self._get_catalog_field(field_name='metadata_created', default_values_dict=spain_dcat_default_values, order='asc')
        if modified or issued or license:
            if modified:
                self._add_date_triple(catalog_ref, DCT.modified, modified)
            if issued:
                self._add_date_triple(catalog_ref, DCT.issued, issued)

    def _add_dataset_triples_from_dict(self, dataset_dict, dataset_ref, items):
        """Adds triples to the RDF graph for the given dataset.

        Args:
            dataset_dict (dict): A dictionary containing the dataset metadata.
            dataset_ref (rdflib.URIRef): The URI reference for the dataset.
            items (dict): A dictionary containing the keys and values for the metadata items.

        Returns:
            None

        """
        
        for key, (predicate, default_value) in items.items():
            value = dataset_dict.get(key, default_value)
            if value is not None:
                self.g.add((dataset_ref, predicate, URIRefOrLiteral(value)))

    def _bind_namespaces(self):
        self.g.namespace_manager.bind('schema', namespaces['schema'], replace=True)

    def _temporal_graph(self, dataset_dict, dataset_ref):
        """Adds the dct:temporal triple to the RDF graph for the given dataset.

        Args:
            dataset_dict (dict): A dictionary containing the dataset metadata.
            dataset_ref (rdflib.URIRef): The URI reference for the dataset.

        Returns:
            None

        """
        # Temporal
        start = self._get_dataset_value(dataset_dict, 'temporal_start')
        end = self._get_dataset_value(dataset_dict, 'temporal_end')

        uid = 1

        temporal_extent = URIRef(
            "%s/%s-%s" % (dataset_ref, 'PeriodOfTime', uid))
        self.g.add((temporal_extent, RDF.type, DCT.PeriodOfTime))
        if start:
            self._add_date_triple(
                temporal_extent, SCHEMA.startDate, start)
        if end:
            self._add_date_triple(
                temporal_extent, SCHEMA.endDate, end)
        self.g.add((dataset_ref, DCT.temporal, temporal_extent))

    def _distribution_format(self, resource_dict, distribution):
        """
        Generates an RDF triple for the format of a resource.

        Args:
            mime_type (str): The MIME type of the format.
            label (str): The label of the format.

        Returns:
            str: The RDF triple for the format.
        """
        resource_format = resource_dict.get('format', spain_dcat_default_values['format_es'])

        format_es = self._search_value_codelist(MD_ES_FORMATS, resource_format, 'label','label', False) or spain_dcat_default_values['format_es']
        mime_type = self._search_value_codelist(MD_ES_FORMATS, format_es, 'label','id') or spain_dcat_default_values['mimetype_es']

        if format_es:
            imt = URIRef("%s/format" % distribution)
            self.g.add((imt, RDF.type, DCT.IMT))
            self.g.add((distribution, DCT['format'], imt))
            self.g.add((imt, RDFS.label, Literal(format_es)))

        if mime_type:
            self.g.add((imt, RDF.value, Literal(mime_type)))

    def _check_resource_url(self, resource_url, distribution):
        """
        Verifies if the URL of a resource is a download URL and adds the corresponding triple to the RDF graph.

        Args:
            resource_url (str): The URL of the resource.
            distribution (URIRef): The URI of the distribution in the RDF graph.

        Returns:
            None
        """
        
        download_keywords = ['descarga', 'download', 'get', 'file', 'data', 'archive', 'zip', 'rar', 'tar', 'gz', 'tgz', '7z', 'exe', 'msi', 'dmg', 'pkg', 'deb', 'rpm', 'iso', 'img', 'bin', 'cue', 'torrent', 'magnet', 'shp', 'gpkg', 'tiff']

        if any(keyword in resource_url.lower() for keyword in download_keywords):
            self.g.add((distribution, DCAT.downloadURL, URIRef(resource_url)))
        else:
            self.g.add((distribution, DCAT.accessURL, URIRef(resource_url)))
